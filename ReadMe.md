# 🚴 Pipeline Vélib' Métropole

![CI](https://github.com/nakzineb/Pipeline_completes_de_donnees_metropole/actions/workflows/ci.yml/badge.svg)

Pipeline de données temps réel pour l'analyse des stations Vélib' Métropole, avec monitoring Prometheus/Grafana et CI/CD GitHub Actions.

---

## 🏗️ Architecture

```
API Vélib' (1500+ stations)
    ↓ HTTP GET (toutes les 5 min)
Producteur Kafka (producer.py)          → métriques :8000
    ↓ JSON messages
Kafka (2 topics)
    ↓ Consumer group
Consommateur Kafka (consumer.py)        → métriques :8001
    ↓ Batch INSERT
ClickHouse (RAW → CLEAN → AGG)
    ↓
Airflow DAG (orchestration)

Prometheus (:9090) ← scrape métriques
    ↓
Grafana (:3000) ← dashboards + alertes
```

---

## 🚀 Démarrage rapide

### Prérequis

- Docker & Docker Compose installés
- 8GB RAM minimum
- Git

### Installation

```bash
# 1. Cloner le projet
git clone https://github.com/nakzineb/Pipeline_completes_de_donnees_metropole.git
cd Pipeline_completes_de_donnees_metropole

# 2. Lancer tous les services
docker-compose up -d

# 3. Vérifier que tout fonctionne
docker-compose ps
```

### Vérification

```bash
# Tous les services doivent être "healthy"
docker-compose ps

# Tester Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Tester ClickHouse
docker exec clickhouse clickhouse-client --query "SHOW DATABASES"

# Tester Prometheus (targets UP)
curl http://localhost:9090/api/v1/targets
```

---

## 🌐 Accès aux services

| Service | URL | Identifiants |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | admin / admin |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | — |
| **ClickHouse** | http://localhost:8123 | default / clickhouse123 |
| **Kafka** | localhost:9092 | — |

---

## 📊 Monitoring & Alerting

### Métriques exposées

**Producer** (port 8000) :
- `velib_messages_produced_total` : messages envoyés vers Kafka (par topic et status)
- `velib_api_calls_total` : nombre d'appels API
- `velib_api_errors_total` : erreurs API
- `velib_api_response_seconds` : latence de l'API

**Consumer** (port 8001) :
- `velib_messages_consumed_total` : messages lus depuis Kafka
- `velib_rows_inserted_total` : lignes insérées dans ClickHouse
- `velib_insertion_errors_total` : erreurs d'insertion
- `velib_batch_processing_seconds` : temps de traitement des batchs
- `velib_buffer_size` : taille des buffers en mémoire

### Dashboard Grafana

Le dashboard est **auto-provisionné** au démarrage. Il affiche 6 panels :
1. Messages produits dans le temps
2. Messages consommés dans le temps
3. Lignes insérées dans ClickHouse
4. Erreurs de la pipeline (API + insertion)
5. Taille des buffers (gauge)
6. Latence API (percentiles p50, p95, p99)

### Alertes Prometheus

| Alerte | Condition | Sévérité |
|--------|-----------|----------|
| `ProducerInactive` | 0 message produit pendant 5 min | warning |
| `ConsumerInactive` | 0 message consommé pendant 5 min | warning |
| `HighErrorRate` | Taux d'erreurs > 10% pendant 2 min | critical |
| `APIDown` | Pas d'appel API réussi pendant 5 min | critical |

Vérifier les alertes : http://localhost:9090/alerts

---

## 🧪 Tests & CI

### Lancer les tests en local

```bash
# Installer les dépendances
pip install -r requirements.txt

# Lancer les tests
pytest tests/ -v

# Lancer le linter
ruff check velib_kafka/ tests/
```

### CI GitHub Actions

Le workflow CI s'exécute à chaque push et vérifie :
1. Installation des dépendances
2. Lint avec ruff
3. Tests unitaires avec pytest

Les tests sont **entièrement mockés** (pas besoin de Kafka/ClickHouse).

---

## 📁 Structure du projet

```
Pipeline_completes_de_donnees_metropole/
├── .github/workflows/
│   └── ci.yml                          # Workflow CI
├── monitoring/
│   ├── prometheus/
│   │   ├── prometheus.yml              # Config scraping
│   │   └── alert_rules.yml             # Règles d'alerte
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/
│       │   │   └── prometheus.yml      # Datasource auto
│       │   └── dashboards/
│       │       └── dashboard.yml       # Config provisioning
│       └── dashboards/
│           └── velib_pipeline.json     # Dashboard JSON
├── tests/
│   ├── __init__.py
│   ├── conftest.py                     # Fixtures & mocks
│   ├── test_producer.py                # Tests producer
│   └── test_consumer.py                # Tests consumer
├── velib_kafka/
│   ├── __init__.py
│   ├── producer.py                     # Producer + métriques
│   └── consumer.py                     # Consumer + métriques
├── airflow/dags/
│   ├── velib_pipeline_dag.py
│   └── velib_simple_test_dag.py
├── clickhouse/init/
│   ├── 01_raw_tables.sql
│   ├── 02_clean_tables.sql
│   └── 03_agg_tables.sql
├── docker-compose.yml
├── requirements.txt
├── pyproject.toml                      # Config ruff + pytest
└── README.md
```

---

## 🛑 Arrêt

```bash
# Arrêter tous les services
docker-compose down

# Arrêter et supprimer les données
docker-compose down -v
```