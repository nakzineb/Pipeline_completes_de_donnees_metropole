"""
DAG AIRFLOW SIMPLE - TEST DU PIPELINE
======================================

Version simplifiée du DAG pour tester rapidement le pipeline.
Exécution manuelle uniquement.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'velib_simple_test',
    default_args=default_args,
    description='Test simple du pipeline Vélib\'',
    schedule_interval=None,  # Exécution manuelle uniquement
    start_date=days_ago(1),
    catchup=False,
    tags=['velib', 'test'],
)

# Task simple : Hello World
def hello():
    print("=" * 60)
    print(" Hello from Airflow!")
    print("=" * 60)
    print(" Airflow fonctionne correctement")
    print(" Python fonctionne")
    print(" Le DAG est chargé")
    return "Success!"

# Task : Vérifier Kafka
def check_kafka():
    from kafka import KafkaConsumer
    
    print(" Vérification de Kafka...")
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=['kafka:29092'],
            api_version=(2, 5, 0)
        )
        topics = consumer.topics()
        print(f"Kafka accessible - {len(topics)} topics")
        print(f"Topics: {topics}")
        consumer.close()
    except Exception as e:
        print(f" Erreur Kafka: {e}")
        raise

# Task : Vérifier ClickHouse
def check_clickhouse():
    import clickhouse_connect
    
    print(" Vérification de ClickHouse...")
    try:
        client = clickhouse_connect.get_client(
            host='clickhouse',
            port=8123,
            username='default',
            password='clickhouse123'
        )
        result = client.query("SELECT version()")
        version = result.result_rows[0][0]
        print(f" ClickHouse accessible - Version: {version}")
        
        # Lister les bases
        result = client.query("SHOW DATABASES")
        databases = [row[0] for row in result.result_rows]
        print(f"Databases: {databases}")
        
        client.close()
    except Exception as e:
        print(f" Erreur ClickHouse: {e}")
        raise

# Définir les tasks
task_hello = PythonOperator(
    task_id='hello_world',
    python_callable=hello,
    dag=dag,
)

task_kafka = PythonOperator(
    task_id='check_kafka',
    python_callable=check_kafka,
    dag=dag,
)

task_clickhouse = PythonOperator(
    task_id='check_clickhouse',
    python_callable=check_clickhouse,
    dag=dag,
)

# Ordre d'exécution
task_hello >> [task_kafka, task_clickhouse]