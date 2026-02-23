#  Projet Pipeline Vélib' - Architecture et Orchestration de Données

> Pipeline de données temps réel pour l'analyse des stations Vélib' à Paris
> 
> **Technos** : Kafka • Airflow • ClickHouse • Docker • Python

---

##  Table des matières

- [Vue d'ensemble](#vue-densemble)
- [Architecture](#architecture)
- [Technologies](#technologies)
- [Structure du projet](#structure-du-projet)
- [Démarrage rapide](#démarrage-rapide)
- [Documentation](#documentation)

---

##  Vue d'ensemble

Ce projet implémente un **pipeline de données complet end-to-end** pour :

1.  **Collecter** les données en temps réel de l'API Vélib' Métropole
2.  **Diffuser** ces données via Apache Kafka
3.  **Orchestrer** les traitements avec Apache Airflow
4.  **Stocker** les données dans ClickHouse (Data Warehouse)
5.  **Analyser** via des indicateurs SQL

### Cas d'usage

- Analyser les patterns d'utilisation des Vélib'
- Identifier les stations saturées ou vides
- Comparer vélos mécaniques vs électriques
- Détecter les heures de pointe
- Prévoir la demande future

---

##  Architecture

```
API Vélib' 
    ↓
Kafka (Streaming)
    ↓
Airflow (Orchestration)
    ↓
ClickHouse (Data Warehouse)
    ↓
Analyses SQL
```

### Schéma détaillé

Voir [docs/02_architecture.md](docs/02_architecture.md)

---

##  Technologies

| Composant | Technologie | Rôle |
|-----------|-------------|------|
| **Streaming** | Apache Kafka | Bus de messages temps réel |
| **Orchestration** | Apache Airflow | Planification et gestion des tâches |
| **Data Warehouse** | ClickHouse | Stockage analytique orienté colonnes |
| **Conteneurisation** | Docker & Docker Compose | Environnement reproductible |
| **Language** | Python 3.11+ | Scripts de traitement |

### Pourquoi ces choix ?

**Kafka** : Découplage production/consommation, scalabilité, résilience  
**Airflow** : Orchestration visuelle, retry automatique, monitoring  
**ClickHouse** : Performance exceptionnelle sur données analytiques (10x plus rapide que PostgreSQL sur agrégations)  
**Docker** : Portabilité, isolation, facilité de déploiement

---

##  Structure du projet

```
velib-pipeline/
│
├── docs/                          # Documentation
│   ├── 01_exploration_api.md      # Guide API Vélib'
│   └── 02_architecture.md         # Architecture détaillée
│
├── docker-compose.yml             # Configuration Docker
│
├── kafka/                         # Scripts Kafka
│   ├── producer.py                # Producteur (API → Kafka)
│   └── consumer.py                # Consommateur (Kafka → ClickHouse)
│
├── airflow/                       # DAGs et configuration
│   ├── dags/
│   │   └── velib_pipeline_dag.py  # DAG principal
│   └── config/
│       └── airflow.cfg
│
├── clickhouse/                    # Schémas et requêtes
│   ├── schemas/
│   │   ├── 01_raw_tables.sql      # Tables brutes
│   │   ├── 02_clean_tables.sql    # Tables nettoyées
│   │   └── 03_agg_tables.sql      # Tables agrégées
│   └── queries/
│       └── indicateurs.sql        # Requêtes d'analyse
│
├── scripts/                       # Scripts utilitaires
│   ├── setup.sh                   # Setup initial
│   └── test_api.py                # Test de l'API
│
└── README.md                      # Ce fichier
```

---

##  Démarrage rapide

### Prérequis

- Docker & Docker Compose installés
- 8GB RAM minimum
- Port 8080, 9092, 8123 disponibles

### Installation

```bash
# 1. Cloner le projet
git clone <your-repo>
cd velib-pipeline

# 2. Lancer tous les services
docker-compose up -d

# 3. Vérifier que tout fonctionne
docker-compose ps

# 4. Accéder aux interfaces
# - Airflow UI : http://localhost:8080 (admin/admin)
# - ClickHouse : http://localhost:8123
# - Kafka : kafka:9092 (depuis containers)
```

### Test rapide

```bash
# Tester l'API Vélib'
python scripts/test_api.py

# Vérifier Kafka
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Vérifier ClickHouse
docker exec -it clickhouse clickhouse-client --query "SHOW DATABASES"
```

---

##  Documentation

### Guides étape par étape

1. [**Exploration de l'API**](docs/01_exploration_api.md)
   - Structure des données
   - Endpoints disponibles
   - Exemples de réponses

2. [**Architecture complète**](docs/02_architecture.md)
   - Schéma détaillé
   - Justification des choix techniques
   - Flux de données

### Pour aller plus loin

- [Configuration Kafka](kafka/README.md)
- [DAGs Airflow](airflow/dags/README.md)
- [Modèle ClickHouse](clickhouse/schemas/README.md)
- [Indicateurs SQL](clickhouse/queries/README.md)

---

##  Objectifs pédagogiques couverts

-  Architecture de pipeline temps réel
-  Rôle de Kafka dans le streaming
-  Orchestration avec Airflow
-  Modélisation dans un Data Warehouse
-  Création d'indicateurs analytiques
-  Bonnes pratiques data engineering

---

##  Métriques du projet

- **~1400 stations** surveillées
- **~2M mesures/jour** collectées
- **60M mesures/mois** stockées
- **<1 minute** de latence ingestion → analyse
- **1.2 GB/mois** avec compression ClickHouse

---

##  Contribution

Ce projet est réalisé dans le cadre du module "Architecture et orchestration de pipelines de données".

### Structure de développement

1. Branche `main` : code stable
2. Branche `dev` : développement en cours
3. Feature branches : `feature/kafka-producer`, `feature/airflow-dag`, etc.

---
