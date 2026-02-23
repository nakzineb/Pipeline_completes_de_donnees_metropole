#  Documentation Docker Compose - Explications Détaillées

##  Vue d'ensemble

Ce document explique **en détail** chaque service du `docker-compose.yml`, pourquoi il est configuré ainsi, et comment les services communiquent entre eux.

---

##  Architecture des conteneurs

```
┌─────────────────────────────────────────────────────────────┐
│                     RÉSEAU: velib-network                   │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  ZOOKEEPER   │◄───┤    KAFKA     │    │  POSTGRES    │  │
│  │   :2181      │    │   :9092      │    │   :5432      │  │
│  └──────────────┘    └──────┬───────┘    └──────┬───────┘  │
│                             │                    │          │
│                             │                    │          │
│                      ┌──────▼────────────────────▼───────┐  │
│                      │      AIRFLOW WEBSERVER           │  │
│                      │           :8080                   │  │
│                      └──────────┬───────────────────────┘  │
│                                 │                          │
│                      ┌──────────▼───────────────────────┐  │
│                      │      AIRFLOW SCHEDULER           │  │
│                      └──────────────────────────────────┘  │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              CLICKHOUSE :8123 / :9000                │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 1️ Zookeeper

### Rôle
**Coordination et gestion du cluster Kafka**

Zookeeper est le "manager" de Kafka. Il garde en mémoire :
- Quels brokers Kafka sont actifs
- Qui est le leader de chaque partition
- Les métadonnées des topics
- Les offset des consumer groups

### Configuration clé

```yaml
ZOOKEEPER_CLIENT_PORT: 2181
```
→ Port sur lequel Kafka se connecte à Zookeeper

```yaml
ZOOKEEPER_TICK_TIME: 2000
```
→ Unité de temps de base (2000ms = 2 secondes)  
→ Utilisé pour les heartbeats et timeouts

### Volumes

```yaml
volumes:
  - zookeeper-data:/var/lib/zookeeper/data
  - zookeeper-logs:/var/lib/zookeeper/log
```

**Pourquoi ?**  
Sans volumes, les métadonnées seraient perdues à chaque redémarrage !

### Healthcheck

```yaml
healthcheck:
  test: ["CMD", "nc", "-z", "localhost", "2181"]
```

**Explication** :
- `nc` (netcat) teste si le port 2181 répond
- Les autres services attendront que Zookeeper soit "healthy" avant de démarrer

---

## 2️ Kafka

### Rôle
**Système de messagerie distribué pour le streaming**

Kafka est le "bus de données" qui :
- Reçoit les messages du producteur (API Vélib')
- Les stocke de manière persistante
- Les distribue aux consommateurs (qui écrivent en ClickHouse)

### Configuration des listeners

```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
```

**C'est complexe, expliquons :**

#### Pourquoi 2 listeners ?

1. **PLAINTEXT (port 29092)** :
   - Pour la communication **entre containers Docker**
   - Les autres containers accèdent via `kafka:29092`

2. **PLAINTEXT_HOST (port 9092)** :
   - Pour la communication **depuis l'hôte** (votre machine)
   - Vous accédez via `localhost:9092`

#### Schéma

```
┌─────────────────────────────────────────────────────────┐
│  Votre Machine (Hôte)                                   │
│                                                         │
│  Script Python ──► localhost:9092 ──► Port 9092        │
│                                                         │
│  ┌───────────────────────────────────────────────────┐ │
│  │  Container Kafka                                  │ │
│  │                                                   │ │
│  │  Port 9092 ◄──── PLAINTEXT_HOST                  │ │
│  │  Port 29092 ◄─── PLAINTEXT (pour autres containers)│ │
│  └───────────────────────────────────────────────────┘ │
│                                                         │
│  ┌───────────────────────────────────────────────────┐ │
│  │  Container Airflow                                │ │
│  │                                                   │ │
│  │  Connexion ──► kafka:29092                       │ │
│  └───────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

### Configuration de rétention

```yaml
KAFKA_LOG_RETENTION_HOURS: 168  # 7 jours
KAFKA_LOG_RETENTION_BYTES: 1073741824  # 1GB
```

**Pourquoi 7 jours ?**
- Permet de rejouer les messages en cas de problème
- Équilibre entre espace disque et capacité de recovery
- Pour notre projet : ~2M messages/jour × 7 jours ≈ 14M messages max

**Pourquoi 1GB max ?**
- Protection contre la saturation du disque
- Si on dépasse 1GB, Kafka supprime les messages les plus anciens

### Dépendances

```yaml
depends_on:
  zookeeper:
    condition: service_healthy
```

**Explication** :
Kafka ne démarre que quand Zookeeper est **healthy** (pas juste "started").  
Sinon, Kafka crash car il ne peut pas se connecter !

---

## 3️ PostgreSQL

### Rôle
**Base de données pour les métadonnées d'Airflow**

PostgreSQL stocke :
- L'historique des exécutions de DAGs
- Le statut des tasks (réussite, échec, en cours)
- Les configurations
- Les connexions
- Les variables
- Les logs de haut niveau

### Pourquoi PostgreSQL et pas SQLite ?

| SQLite (défaut) | PostgreSQL |
|----------------|------------|
|  Pas de concurrence |  Support multi-utilisateurs |
|  Performance limitée |  Performant sur gros volumes |
|  Pas de réseau |  Accessible depuis plusieurs containers |

### Configuration

```yaml
POSTGRES_USER: airflow
POSTGRES_PASSWORD: airflow
POSTGRES_DB: airflow
```

**Important** : Ces credentials sont utilisés par Airflow dans la connection string :
```
postgresql+psycopg2://airflow:airflow@postgres/airflow
```

---

## 4️ Airflow Webserver

### Rôle
**Interface utilisateur web pour Airflow**

Le webserver fournit :
- L'interface graphique pour visualiser les DAGs
- Les logs des tâches
- Les graphiques de dépendances
- Le monitoring des exécutions
- La gestion manuelle (trigger, pause, etc.)

### Configuration

```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
```

**LocalExecutor vs autres executors** :

| Executor | Usage | Notre cas |
|----------|-------|-----------|
| **SequentialExecutor** | 1 task à la fois |  Trop lent |
| **LocalExecutor** | Plusieurs tasks en parallèle (threads) |  Parfait pour dev/test |
| **CeleryExecutor** | Distribution sur plusieurs machines |  Overkill pour nous |
| **KubernetesExecutor** | Distribution sur k8s |  Trop complexe |

```yaml
AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
```

Désactive les DAGs d'exemple → interface plus propre !

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: kafka-python clickhouse-connect requests
```

**Packages Python installés automatiquement** :
- `kafka-python` : Pour produire/consommer depuis Kafka
- `clickhouse-connect` : Pour se connecter à ClickHouse
- `requests` : Pour appeler l'API Vélib'

### Commande de démarrage

```yaml
command: >
  bash -c "airflow db init &&
           airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin &&
           airflow webserver"
```

**Séquence** :
1. `airflow db init` : Créer les tables dans PostgreSQL
2. `airflow users create` : Créer l'utilisateur admin
3. `airflow webserver` : Démarrer le serveur web

### Volumes partagés

```yaml
volumes:
  - ./airflow/dags:/opt/airflow/dags
  - ./airflow/logs:/opt/airflow/logs
  - ./kafka:/opt/airflow/kafka
```

**Pourquoi ?**
- Modification des DAGs en direct (pas besoin de rebuild)
- Accès aux logs depuis l'hôte
- Accès aux scripts Kafka depuis Airflow

---

## 5️ Airflow Scheduler

### Rôle
**Orchestrateur qui exécute les DAGs selon leur planning**

Le scheduler :
- Surveille les DAGs toutes les X secondes
- Vérifie si c'est le moment d'exécuter une task
- Crée les task instances
- Gère les dépendances entre tasks
- Gère les retry en cas d'échec

### Différence Webserver vs Scheduler

| Webserver | Scheduler |
|-----------|-----------|
| Interface utilisateur | Moteur d'exécution |
| Affichage | Action |
| Lecture seule (sauf triggers manuels) | Écriture dans la DB |

**Analogie** :
- **Webserver** = Dashboard de voiture
- **Scheduler** = Moteur de la voiture

### Healthcheck

```yaml
healthcheck:
  test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
```

Vérifie que le scheduler tourne bien sur ce container.

---

## 6 ClickHouse

### Rôle
**Data Warehouse orienté colonnes pour l'analyse**

ClickHouse stocke :
- Toutes les mesures temps réel des stations
- Les métadonnées des stations
- Les tables agrégées pour les analyses
- L'historique complet

### Pourquoi orienté colonnes ?

#### Stockage orienté lignes (PostgreSQL)

```
Row 1: [station_id=123, timestamp=..., bikes=10, docks=5]
Row 2: [station_id=124, timestamp=..., bikes=8, docks=7]
```

**Problème** : Pour lire juste `bikes`, on doit lire toute la ligne !

#### Stockage orienté colonnes (ClickHouse)

```
Column station_id: [123, 124, 125, ...]
Column bikes:      [10, 8, 12, ...]
Column docks:      [5, 7, 3, ...]
```

**Avantage** : On lit seulement les colonnes nécessaires !

### Exemple de performance

```sql
-- Requête typique : moyenne de bikes disponibles par heure
SELECT 
    toStartOfHour(timestamp) as hour,
    AVG(num_bikes_available)
FROM station_status
GROUP BY hour;
```

**Performance** :
- PostgreSQL (lignes) : Lit 100% des données → ~30 secondes
- ClickHouse (colonnes) : Lit 2 colonnes (timestamp, bikes) → ~0.5 secondes ⚡

### Configuration

```yaml
ulimits:
  nofile:
    soft: 262144
    hard: 262144
```

**Pourquoi ?**
ClickHouse ouvre beaucoup de fichiers simultanément (un par partie de table).  
Par défaut, Linux limite à ~1024 → ClickHouse crash !

### Ports

```yaml
ports:
  - "8123:8123"  # HTTP port
  - "9000:9000"  # Native port
```

**Différence** :
- **Port 8123 (HTTP)** : Pour curl, Grafana, requêtes web
- **Port 9000 (Native)** : Pour le client CLI, plus rapide, binaire

---

##  Communication entre services

### Réseau Docker

```yaml
networks:
  velib-network:
    driver: bridge
```

Tous les containers sont sur le même réseau virtuel.  
Ils peuvent se parler via leur **nom de container**.

### Exemple de communication

```python
# Dans Airflow (container airflow-scheduler)
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='kafka:29092'  # Nom du container !
)
```

**Ne PAS utiliser** `localhost:9092` depuis un container !  
`localhost` dans un container = le container lui-même.

### Table de résolution

| Depuis | Pour accéder à Kafka | Pour accéder à ClickHouse |
|--------|---------------------|---------------------------|
| **Hôte (votre machine)** | `localhost:9092` | `localhost:8123` |
| **Container Airflow** | `kafka:29092` | `clickhouse:8123` |
| **Container producteur** | `kafka:29092` | `clickhouse:8123` |

---

##  Volumes persistants

### Pourquoi les volumes ?

Sans volumes, **toutes les données sont perdues** au redémarrage !

```yaml
volumes:
  postgres-db-volume:      # Métadonnées Airflow
  kafka-data:              # Messages Kafka
  clickhouse-data:         # Données analytiques
```

### Où sont stockés les volumes ?

```bash
# Voir les volumes
docker volume ls

# Inspecter un volume
docker volume inspect velib-pipeline_clickhouse-data
```

Généralement dans `/var/lib/docker/volumes/` (Linux).

---

##  Ordre de démarrage

```
1. Zookeeper démarre
   ↓
2. Zookeeper devient healthy
   ↓
3. Kafka démarre (depends_on zookeeper)
   ↓
4. Kafka devient healthy
   ↓
5. PostgreSQL démarre en parallèle
   ↓
6. PostgreSQL devient healthy
   ↓
7. Airflow Webserver démarre
   ↓
8. Airflow Scheduler démarre
   ↓
9. ClickHouse démarre en parallèle
```

**Grâce aux `depends_on` et `healthcheck`, tout démarre dans le bon ordre !**

---

## 📊 Ressources utilisées

### Estimations

| Service | RAM | CPU | Disque |
|---------|-----|-----|--------|
| Zookeeper | ~200 MB | 0.1 | ~100 MB |
| Kafka | ~1 GB | 0.5 | ~2 GB |
| PostgreSQL | ~200 MB | 0.1 | ~500 MB |
| Airflow Web | ~500 MB | 0.2 | - |
| Airflow Scheduler | ~500 MB | 0.3 | - |
| ClickHouse | ~500 MB | 0.3 | ~5 GB |
| **TOTAL** | **~3 GB** | **1.5** | **~8 GB** |

**Recommandé** : 8 GB RAM sur votre machine pour être confortable.

---

## 🐛 Troubleshooting

### Kafka ne démarre pas

**Vérifier Zookeeper** :
```bash
docker-compose logs zookeeper
```

### Airflow ne démarre pas

**Vérifier PostgreSQL** :
```bash
docker-compose logs postgres
docker exec postgres-airflow pg_isready
```

### ClickHouse n'accepte pas les connexions

**Vérifier les ulimits** :
```bash
docker exec clickhouse cat /proc/sys/fs/file-max
```

---

##  Ressources

- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Kafka Docker Quickstart](https://kafka.apache.org/quickstart)
- [Airflow Docker Setup](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [ClickHouse Docker](https://clickhouse.com/docs/en/install/#docker-image)