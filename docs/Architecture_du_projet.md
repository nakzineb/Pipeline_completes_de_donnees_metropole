#  Architecture du Pipeline Vélib'

##  Vue d'ensemble

Ce document décrit l'architecture complète du pipeline de données temps réel pour l'analyse des stations Vélib'.

---

##  Objectif du projet

Construire une **pipeline de données end-to-end** qui :
1. Collecte les données en temps réel depuis l'API Vélib'
2. Les diffuse via un système de streaming
3. Les transforme et les stocke dans un data warehouse
4. Permet leur analyse via des requêtes SQL

---

##  Schéma de l'architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          COUCHE D'INGESTION                             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────▼───────────────┐
                    │   API Vélib' Métropole        │
                    │   (GBFS JSON)                 │
                    │   - station_status.json       │
                    │   - station_information.json  │
                    └───────────────┬───────────────┘
                                    │
                                    │ HTTP GET
                                    │ (chaque minute)
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                      COUCHE DE STREAMING (KAFKA)                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────▼───────────────┐
                    │   PRODUCTEUR KAFKA            │
                    │   (Python Script)             │
                    │   - Fetch API                 │
                    │   - Parse JSON                │
                    │   - Send to Kafka             │
                    └───────────────┬───────────────┘
                                    │
                                    │ Messages JSON
                                    │
                    ┌───────────────▼───────────────┐
                    │   KAFKA BROKER                │
                    │                               │
                    │   Topics:                     │
                    │   - velib-station-status      │
                    │   - velib-station-info        │
                    └───────────────┬───────────────┘
                                    │
                                    │ Subscribe
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                    COUCHE D'ORCHESTRATION (AIRFLOW)                     │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────▼───────────────┐
                    │   AIRFLOW DAG                 │
                    │                               │
                    │   Tasks:                      │
                    │   1. Check API Health         │
                    │   2. Trigger Producer         │
                    │   3. Consume & Transform      │
                    │   4. Load to ClickHouse       │
                    │   5. Data Quality Checks      │
                    └───────────────┬───────────────┘
                                    │
                                    │
                    ┌───────────────▼───────────────┐
                    │   CONSOMMATEUR KAFKA          │
                    │   (Python Script)             │
                    │   - Read from Kafka           │
                    │   - Transform Data            │
                    │   - Batch Insert              │
                    └───────────────┬───────────────┘
                                    │
                                    │ INSERT
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                     COUCHE DE STOCKAGE (CLICKHOUSE)                     │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────▼───────────────┐
                    │   CLICKHOUSE DATABASE         │
                    │                               │
                    │   Tables:                     │
                    │   - raw_station_status        │
                    │   - raw_station_info          │
                    │   - fact_station_availability │
                    │   - dim_stations              │
                    │   - agg_hourly_stats          │
                    └───────────────┬───────────────┘
                                    │
                                    │ SELECT
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                        COUCHE D'ANALYSE (SQL)                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────▼───────────────┐
                    │   INDICATEURS & DASHBOARDS    │
                    │                               │
                    │   - Disponibilité temps réel  │
                    │   - Stations saturées         │
                    │   - Patterns temporels        │
                    │   - Mécanique vs Électrique   │
                    └───────────────────────────────┘
```

---

##  Composants détaillés

### 1️ **Ingestion - API Vélib'**

**Rôle** : Source de données externes

**Caractéristiques** :
- Format : JSON (GBFS standard)
- Mise à jour : Chaque minute
- Pas d'authentification requise
- 2 endpoints principaux

**Pourquoi ?**
- Données publiques et gratuites
- Standard GBFS largement utilisé
- Mise à jour fréquente (parfait pour du streaming)

---

### 2️ **Streaming - Apache Kafka**

**Rôle** : Bus de messages pour découpler production et consommation

**Architecture Kafka** :
```
┌────────────────┐
│  ZOOKEEPER     │ ← Coordonne le cluster Kafka
└────────────────┘
        │
┌────────────────┐
│  KAFKA BROKER  │ ← Stocke les messages dans des topics
└────────────────┘
```

**Topics créés** :
- `velib-station-status` : État temps réel (haute fréquence)
- `velib-station-info` : Métadonnées (basse fréquence)

**Pourquoi Kafka ?**
-  Découplage : producteur et consommateur indépendants
-  Scalabilité : peut gérer des millions de messages/sec
-  Résilience : messages persistés sur disque
-  Multi-consommateurs : plusieurs services peuvent lire le même flux
-  Replay : possibilité de relire l'historique

**Configuration clé** :
```python
# Producteur
bootstrap.servers = 'kafka:9092'
acks = 'all'  # Attendre confirmation de tous les replicas
compression.type = 'gzip'  # Compression des messages

# Consommateur
group.id = 'velib-consumer-group'
auto.offset.reset = 'earliest'  # Lire depuis le début si nouveau
enable.auto.commit = True
```

---

### 3️ **Orchestration - Apache Airflow**

**Rôle** : Orchestrer et planifier l'ensemble du pipeline

**DAG Principal** : `velib_pipeline_dag`

```python
# Planification : toutes les minutes
schedule_interval = '*/1 * * * *'

# Graphe de dépendances
check_api_health >> fetch_status >> consume_transform >> load_warehouse >> quality_checks
                 >> fetch_info    >> consume_transform >> load_warehouse >>
```

**Tasks du DAG** :

1. **check_api_health** 
   - Vérifie que l'API est accessible
   - Type : HTTP Sensor
   
2. **fetch_status / fetch_info**
   - Déclenche les producteurs Kafka
   - Type : PythonOperator
   
3. **consume_transform**
   - Lit Kafka, transforme les données
   - Type : PythonOperator
   
4. **load_warehouse**
   - Charge les données dans ClickHouse
   - Type : PythonOperator
   
5. **quality_checks**
   - Vérifie la qualité des données
   - Type : SQLCheckOperator

**Pourquoi Airflow ?**
-  Visualisation du pipeline (UI web)
-  Gestion des dépendances entre tâches
-  Retry automatique en cas d'échec
-  Monitoring et alerting
-  Historique des exécutions

---

### 4️ **Stockage - ClickHouse**

**Rôle** : Data Warehouse orienté colonnes pour l'analyse

**Pourquoi ClickHouse et pas PostgreSQL ?**

| Critère | PostgreSQL | ClickHouse | Choix |
|---------|-----------|------------|-------|
| Type | OLTP (transactionnel) | OLAP (analytique) |  OLAP pour notre cas |
| Structure | Orienté lignes | Orienté colonnes |  Colonnes (on lit peu de colonnes mais beaucoup de lignes) |
| Compression | Moyenne (~2x) | Excellente (~10x) |  Important pour données temps réel |
| Requêtes analytiques | Lent sur gros volumes | Ultra rapide |  Millions de mesures |
| INSERT massifs | Moyen | Très rapide |  Kafka ingestion continue |
| UPDATE/DELETE | Natif | Limité |  Pas besoin (données temps réel immuables) |

**Exemple de performance** :
```sql
-- Compter 100M de lignes groupées par heure
-- PostgreSQL : ~30 secondes
-- ClickHouse : ~0.5 secondes 
SELECT 
    toStartOfHour(timestamp) as hour,
    COUNT(*) 
FROM station_status 
GROUP BY hour;
```

**Architecture ClickHouse dans notre projet** :
```
┌─────────────────────────────────┐
│   DATABASE: velib_db            │
│                                 │
│   ┌─────────────────────────┐   │
│   │  RAW LAYER (Bronze)     │   │
│   │  - raw_station_status   │   │ ← Données brutes de Kafka
│   │  - raw_station_info     │   │
│   └─────────────────────────┘   │
│                                 │
│   ┌─────────────────────────┐   │
│   │  CLEAN LAYER (Silver)   │   │
│   │  - fact_availability    │   │ ← Données nettoyées
│   │  - dim_stations         │   │
│   └─────────────────────────┘   │
│                                 │
│   ┌─────────────────────────┐   │
│   │  AGG LAYER (Gold)       │   │
│   │  - agg_hourly_stats     │   │ ← Pré-agrégations
│   │  - agg_daily_stats      │   │
│   └─────────────────────────┘   │
└─────────────────────────────────┘
```

---

##  Conteneurisation avec Docker

**Fichier : docker-compose.yml**

```yaml
services:
  # Kafka Stack
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
  
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
  
  # Airflow Stack
  airflow-webserver:
    image: apache/airflow:2.8.0
  
  airflow-scheduler:
    image: apache/airflow:2.8.0
  
  # ClickHouse
  clickhouse:
    image: clickhouse/clickhouse-server:latest
```

**Avantages** :
- Environnement reproductible
- Isolation des dépendances
- Déploiement facile
- Networking automatique entre conteneurs

---

##  Flux de données détaillé

### Flux 1 : Données temps réel (haute fréquence)

```
Chaque minute:

1. Airflow déclenche le producteur
   ↓
2. Producteur appelle l'API station_status
   ↓
3. Producteur envoie ~1400 messages à Kafka (un par station)
   ↓
4. Consommateur lit les messages par batch (ex: 100 messages)
   ↓
5. Transformation : ajout timestamp, parsing JSON
   ↓
6. INSERT batch dans ClickHouse.raw_station_status
   ↓
7. Materialized View met à jour fact_availability
```

### Flux 2 : Métadonnées (basse fréquence)

```
Une fois par jour (00:00):

1. Airflow déclenche le producteur
   ↓
2. Producteur appelle l'API station_information
   ↓
3. Producteur envoie ~1400 messages à Kafka
   ↓
4. Consommateur lit et charge dans dim_stations
   ↓
5. Détection des changements (SCD Type 2)
```

---

##  Technologies et justifications

| Composant | Technologie | Version | Justification |
|-----------|-------------|---------|---------------|
| Streaming | Kafka | 7.5.0 | Standard industrie, haute performance |
| Orchestration | Airflow | 2.8.0 | Riche UI, communauté active |
| Data Warehouse | ClickHouse | Latest | OLAP rapide, compression excellente |
| Conteneurisation | Docker | Latest | Portabilité, isolation |
| Language | Python | 3.11+ | Écosystème data riche |

---

##  Volumétrie estimée

**Calculs** :
- 1400 stations
- Mesure toutes les minutes
- 1440 minutes par jour
- 1400 × 1440 = **2,016,000 mesures/jour**
- Sur 1 mois : **~60 millions de mesures**

**Stockage ClickHouse** (avec compression) :
- Taille moyenne d'un enregistrement : ~200 bytes
- Avec compression ClickHouse (ratio 10:1) : ~20 bytes
- 60M × 20 bytes = **1.2 GB/mois**

 ClickHouse est parfait pour ce volume !

---
---

##  Références

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [GBFS Specification](https://github.com/MobilityData/gbfs)