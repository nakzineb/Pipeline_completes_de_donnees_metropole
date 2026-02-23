"""
DAG AIRFLOW - PIPELINE VELIB COMPLET
=====================================

Ce DAG orchestre l'ensemble du pipeline de données Vélib' :
1. Vérification de la santé de l'API
2. Production des données dans Kafka
3. Consommation et insertion dans ClickHouse
4. Vérifications de qualité des données
5. Rafraîchissement des agrégations

Schedule : Toutes les 5 minutes
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# Ajouter le chemin pour importer nos modules
sys.path.insert(0, '/opt/airflow/velib_kafka')

# =================================================================
# CONFIGURATION DU DAG
# =================================================================

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,                          # Nombre de tentatives en cas d'échec
    'retry_delay': timedelta(minutes=1),   # Délai entre les tentatives
    'execution_timeout': timedelta(minutes=10),  # Timeout max pour une task
}

dag = DAG(
    'velib_pipeline',
    default_args=default_args,
    description='Pipeline complet de données Vélib\' - Kafka → ClickHouse',
    schedule_interval='*/5 * * * *',  # Toutes les 5 minutes
    start_date=days_ago(1),
    catchup=False,  # Ne pas exécuter les runs passés
    tags=['velib', 'kafka', 'clickhouse', 'streaming'],
)

# =================================================================
# FONCTIONS DES TASKS
# =================================================================

def check_api_health():
    """
    Vérifie que l'API Vélib' est accessible.
    """
    import requests
    
    url = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            nb_stations = len(data.get('data', {}).get('stations', []))
            print(f" API accessible - {nb_stations} stations")
            return True
        else:
            print(f" API retourne le code {response.status_code}")
            raise Exception(f"API health check failed: {response.status_code}")
    except Exception as e:
        print(f" Erreur API: {e}")
        raise

def produce_velib_data():
    """
    Exécute le producteur Kafka pour récupérer les données de l'API.
    """
    from producer import VelibProducer
    
    print(" Démarrage du producteur Kafka...")
    
    # Créer le producteur avec l'adresse interne Docker
    producer = VelibProducer(
        bootstrap_servers=['kafka:29092']  # Adresse depuis le container Airflow
    )
    
    # Connexion
    if not producer.connect():
        raise Exception("Impossible de se connecter à Kafka")
    
    # Production des données
    try:
        status_count = producer.produce_station_status()
        print(f" {status_count} statuts produits")
        
        info_count = producer.produce_station_information()
        print(f" {info_count} infos produites")
        
        if status_count == 0 or info_count == 0:
            raise Exception("Aucune donnée produite")
            
    finally:
        producer.close()

def consume_velib_data():
    """
    Exécute le consommateur Kafka pour insérer les données dans ClickHouse.
    """
    from consumer import VelibConsumer
    
    print(" Démarrage du consommateur Kafka...")
    
    # Créer le consommateur avec les adresses internes Docker
    consumer = VelibConsumer(
        bootstrap_servers=['kafka:29092'],
        clickhouse_host='clickhouse',
        batch_size=100
    )
    
    # Connexions
    if not consumer.connect_kafka():
        raise Exception("Impossible de se connecter à Kafka")
    
    if not consumer.connect_clickhouse():
        raise Exception("Impossible de se connecter à ClickHouse")
    
    # Consommation pendant 2 minutes max
    try:
        consumer.consume(duration_seconds=120)
        
        print(f" Messages consommés: {consumer.stats['messages_consumed']}")
        print(f" Messages insérés: {consumer.stats['messages_inserted']}")
        
        if consumer.stats['errors'] > 0:
            print(f"  Erreurs: {consumer.stats['errors']}")
        
    finally:
        consumer.close()

def check_data_quality():
    """
    Vérifie la qualité des données dans ClickHouse.
    """
    import clickhouse_connect
    
    print(" Vérification de la qualité des données...")
    
    client = clickhouse_connect.get_client(
        host='clickhouse',
        port=8123,
        username='default',
        password='clickhouse123',
        database='velib_db'
    )
    
    try:
        # Check 1 : Y a-t-il des données récentes ?
        result = client.query(
            """
            SELECT count() 
            FROM raw_station_status 
            WHERE ingestion_timestamp > now() - INTERVAL 10 MINUTE
            """
        )
        recent_count = result.result_rows[0][0]
        print(f" Données récentes (10 min): {recent_count}")
        
        if recent_count == 0:
            raise Exception("Aucune donnée récente trouvée")
        
        # Check 2 : Nombre de stations distinctes
        result = client.query("SELECT count(DISTINCT station_id) FROM raw_station_status")
        nb_stations = result.result_rows[0][0]
        print(f" Stations distinctes: {nb_stations}")
        
        if nb_stations < 1000:  # On attend au moins 1000 stations
            raise Exception(f"Trop peu de stations: {nb_stations}")
        
        # Check 3 : Materialized View fonctionne ?
        result = client.query("SELECT count() FROM fact_station_availability")
        fact_count = result.result_rows[0][0]
        print(f" Lignes dans fact_station_availability: {fact_count}")
        
        if fact_count == 0:
            raise Exception("Materialized View ne fonctionne pas")
        
        # Check 4 : Pas trop de valeurs NULL ?
        result = client.query(
            """
            SELECT 
                countIf(num_bikes_available IS NULL) as null_bikes,
                countIf(num_docks_available IS NULL) as null_docks
            FROM raw_station_status
            WHERE ingestion_timestamp > now() - INTERVAL 1 HOUR
            """
        )
        null_bikes, null_docks = result.result_rows[0]
        print(f" NULL bikes: {null_bikes}, NULL docks: {null_docks}")
        
        if null_bikes > 100 or null_docks > 100:
            print(f"  Beaucoup de NULL détectés")
        
        print(" Qualité des données OK")
        
    finally:
        client.close()

def refresh_aggregations():
    """
    Rafraîchit les agrégations globales (agg_station_summary).
    """
    import clickhouse_connect
    
    print(" Rafraîchissement des agrégations...")
    
    client = clickhouse_connect.get_client(
        host='clickhouse',
        port=8123,
        username='default',
        password='clickhouse123',
        database='velib_db'
    )
    
    try:
        # Les agrégations hourly/daily se font automatiquement via Materialized Views
        # Mais agg_station_summary doit être rafraîchi manuellement
        
        # Truncate et rebuild (simplifié pour ce projet)
        client.command("TRUNCATE TABLE agg_station_summary")
        
        # INSERT des stats globales
        client.command("""
            INSERT INTO agg_station_summary
            SELECT 
                f.station_id,
                any(s.name) as station_name,
                avg(f.num_bikes_available) as avg_bikes_available_alltime,
                avg(f.occupancy_rate) as avg_occupancy_rate_alltime,
                avgIf(f.num_bikes_available, f.is_weekend = 0) as avg_bikes_weekday,
                avgIf(f.num_bikes_available, f.is_weekend = 1) as avg_bikes_weekend,
                avg(f.occupancy_rate) * 100 as popularity_score,
                if(avg(f.occupancy_rate) > 0.7, 1, 0) as is_hotspot,
                countIf(f.is_empty = 1 AND f.measurement_date >= today() - 30) as days_empty_last_month,
                countIf(f.is_full = 1 AND f.measurement_date >= today() - 30) as days_full_last_month,
                avg(f.is_operational) as avg_operational_rate,
                0 as best_hour_to_take_bike,
                0 as best_hour_to_return_bike,
                100 as reliability_score,
                100 as data_quality_score,
                min(f.measurement_date) as first_seen,
                max(f.measurement_date) as last_seen,
                count(DISTINCT f.measurement_date) as nb_days_active,
                now() as updated_at
            FROM fact_station_availability f
            LEFT JOIN dim_stations s ON f.station_id = s.station_id AND s.is_current = 1
            WHERE f.measurement_date >= today() - 30
            GROUP BY f.station_id
        """)
        
        result = client.query("SELECT count() FROM agg_station_summary")
        count = result.result_rows[0][0]
        print(f" Agrégations rafraîchies: {count} stations")
        
    finally:
        client.close()

def print_pipeline_stats():
    """
    Affiche les statistiques finales du pipeline.
    """
    import clickhouse_connect
    
    print("=" * 60)
    print(" STATISTIQUES DU PIPELINE")
    print("=" * 60)
    
    client = clickhouse_connect.get_client(
        host='clickhouse',
        port=8123,
        username='default',
        password='clickhouse123',
        database='velib_db'
    )
    
    try:
        # Stats globales
        result = client.query("SELECT count() FROM raw_station_status")
        print(f"Total lignes RAW: {result.result_rows[0][0]}")
        
        result = client.query("SELECT count() FROM fact_station_availability")
        print(f"Total lignes FACT: {result.result_rows[0][0]}")
        
        result = client.query("SELECT count() FROM agg_hourly_stats")
        print(f"Total lignes AGG_HOURLY: {result.result_rows[0][0]}")
        
        # Stats dernière heure
        result = client.query("""
            SELECT 
                count() as nb_mesures,
                count(DISTINCT station_id) as nb_stations
            FROM raw_station_status
            WHERE ingestion_timestamp > now() - INTERVAL 1 HOUR
        """)
        nb_mesures, nb_stations = result.result_rows[0]
        print(f"\nDernière heure:")
        print(f"  Mesures: {nb_mesures}")
        print(f"  Stations: {nb_stations}")
        
        print("=" * 60)
        
    finally:
        client.close()

# =================================================================
# DÉFINITION DES TASKS
# =================================================================

# Task 1 : Vérifier la santé de l'API
task_check_api = PythonOperator(
    task_id='check_api_health',
    python_callable=check_api_health,
    dag=dag,
)

# Task 2 : Produire les données dans Kafka
task_produce = PythonOperator(
    task_id='produce_data_to_kafka',
    python_callable=produce_velib_data,
    dag=dag,
)

# Task 3 : Consommer et insérer dans ClickHouse
task_consume = PythonOperator(
    task_id='consume_data_to_clickhouse',
    python_callable=consume_velib_data,
    dag=dag,
)

# Task 4 : Vérifier la qualité des données
task_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag,
)

# Task 5 : Rafraîchir les agrégations (1x par jour seulement)
task_aggregate = PythonOperator(
    task_id='refresh_aggregations',
    python_callable=refresh_aggregations,
    dag=dag,
)

# Task 6 : Afficher les stats
task_stats = PythonOperator(
    task_id='print_stats',
    python_callable=print_pipeline_stats,
    dag=dag,
)

# =================================================================
# DÉFINITION DES DÉPENDANCES (GRAPHE)
# =================================================================

# Flux principal :
# 1. Vérifier API
# 2. Produire → Consommer
# 3. Vérifier qualité
# 4. Rafraîchir agrégations
# 5. Afficher stats

task_check_api >> task_produce >> task_consume >> task_quality >> task_aggregate >> task_stats

# =================================================================
# DOCUMENTATION DU DAG
# =================================================================

dag.doc_md = """
# Pipeline Vélib' - Documentation

## Vue d'ensemble

Ce DAG orchestre l'ensemble du pipeline de données Vélib' en temps réel.

## Architecture

```
API Vélib'
    ↓
Producteur Kafka
    ↓
Kafka Topics
    ↓
Consommateur Kafka
    ↓
ClickHouse (RAW → CLEAN → AGG)
```

## Tasks

1. **check_api_health** : Vérifie que l'API Vélib' répond
2. **produce_data_to_kafka** : Récupère les données et les envoie dans Kafka
3. **consume_data_to_clickhouse** : Lit Kafka et insère dans ClickHouse
4. **check_data_quality** : Vérifie la qualité des données
5. **refresh_aggregations** : Met à jour les agrégations globales
6. **print_stats** : Affiche les statistiques

## Schedule

Exécution : **Toutes les 5 minutes**

## Retry Policy

- Nombre de tentatives : 2
- Délai entre tentatives : 1 minute
- Timeout : 10 minutes

## Monitoring

Consultez les logs de chaque task dans l'interface Airflow.
"""