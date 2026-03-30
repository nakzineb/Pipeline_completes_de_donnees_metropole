"""
CONSOMMATEUR KAFKA - KAFKA VERS CLICKHOUSE
===========================================

Ce script consomme les messages depuis Kafka et les insère dans ClickHouse.
Expose des métriques Prometheus sur le port 8001.
"""

import json
import logging
import time
from typing import Dict, List, Optional

import clickhouse_connect
from kafka import KafkaConsumer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# =================================================================
# CONFIGURATION
# =================================================================

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC_STATUS = 'velib-station-status'
KAFKA_TOPIC_INFO = 'velib-station-info'
KAFKA_CONSUMER_GROUP = 'velib-consumer-group'

CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'clickhouse123'
CLICKHOUSE_DATABASE = 'velib_db'

BATCH_SIZE = 100
BATCH_TIMEOUT = 5
MAX_POLL_RECORDS = 500
METRICS_PORT = 8001

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =================================================================
# MÉTRIQUES PROMETHEUS
# =================================================================

MESSAGES_CONSUMED = Counter(
    'velib_messages_consumed_total',
    'Nombre total de messages consommés depuis Kafka',
    ['topic']
)

ROWS_INSERTED = Counter(
    'velib_rows_inserted_total',
    'Nombre total de lignes insérées dans ClickHouse',
    ['table']
)

INSERTION_ERRORS = Counter(
    'velib_insertion_errors_total',
    'Nombre total d\'erreurs d\'insertion dans ClickHouse',
    ['table']
)

BATCH_PROCESSING_TIME = Histogram(
    'velib_batch_processing_seconds',
    'Temps de traitement d\'un batch en secondes',
    ['table'],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

BUFFER_SIZE = Gauge(
    'velib_buffer_size',
    'Taille actuelle du buffer de messages',
    ['buffer_type']
)


# =================================================================
# CLASSE CONSOMMATEUR VÉLIB'
# =================================================================

class VelibConsumer:
    """
    Consommateur Kafka pour les données Vélib'.
    Lit les messages depuis Kafka et les insère en batch dans ClickHouse.
    """

    def __init__(
        self,
        bootstrap_servers: List[str] = None,
        consumer_group: str = KAFKA_CONSUMER_GROUP,
        clickhouse_host: str = CLICKHOUSE_HOST,
        clickhouse_port: int = CLICKHOUSE_PORT,
        batch_size: int = BATCH_SIZE
    ):
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.consumer_group = consumer_group
        self.batch_size = batch_size
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.consumer = None
        self.clickhouse_client = None
        self.status_buffer = []
        self.info_buffer = []
        self.last_insert_time = time.time()
        self.stats = {
            'messages_consumed': 0,
            'messages_inserted': 0,
            'batches_inserted': 0,
            'errors': 0
        }

    def connect_kafka(self) -> bool:
        """Établit la connexion au broker Kafka."""
        try:
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC_STATUS,
                KAFKA_TOPIC_INFO,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.consumer_group,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                max_poll_records=MAX_POLL_RECORDS,
                api_version=(2, 5, 0)
            )
            logger.info("Connexion à Kafka établie")
            return True
        except Exception as e:
            logger.error(f"Erreur de connexion à Kafka: {e}")
            return False

    def connect_clickhouse(self) -> bool:
        """Établit la connexion à ClickHouse."""
        try:
            self.clickhouse_client = clickhouse_connect.get_client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE
            )
            self.clickhouse_client.query("SELECT 1")
            logger.info("Connexion à ClickHouse établie")
            return True
        except Exception as e:
            logger.error(f"Erreur de connexion à ClickHouse: {e}")
            return False

    def transform_status_message(self, message: Dict) -> Dict:
        """Transforme un message de statut pour ClickHouse."""
        if isinstance(message.get('num_bikes_available_types'), list):
            message['num_bikes_available_types'] = json.dumps(message['num_bikes_available_types'])
        return message

    def transform_info_message(self, message: Dict) -> Dict:
        """Transforme un message d'info pour ClickHouse."""
        if isinstance(message.get('rental_methods'), list):
            message['rental_methods'] = json.dumps(message['rental_methods'])
        return message

    def insert_status_batch(self) -> bool:
        """Insère un batch de statuts dans ClickHouse avec métriques."""
        if not self.status_buffer:
            return True

        start_time = time.time()
        try:
            data = []
            for msg in self.status_buffer:
                row = (
                    msg.get('station_id', ''),
                    msg.get('num_bikes_available', 0),
                    msg.get('num_bikes_available_types', '[]'),
                    msg.get('num_docks_available', 0),
                    msg.get('is_installed', 0),
                    msg.get('is_returning', 0),
                    msg.get('is_renting', 0),
                    msg.get('last_reported', 0),
                    msg.get('ingestion_timestamp', int(time.time())),
                    msg.get('api_last_updated', 0)
                )
                data.append(row)

            self.clickhouse_client.insert(
                'raw_station_status',
                data,
                column_names=[
                    'station_id', 'num_bikes_available', 'num_bikes_available_types',
                    'num_docks_available', 'is_installed', 'is_returning', 'is_renting',
                    'last_reported', 'ingestion_timestamp', 'api_last_updated'
                ]
            )

            nb_inserted = len(self.status_buffer)
            self.stats['messages_inserted'] += nb_inserted
            self.stats['batches_inserted'] += 1

            # Métriques Prometheus
            ROWS_INSERTED.labels(table="raw_station_status").inc(nb_inserted)
            duration = time.time() - start_time
            BATCH_PROCESSING_TIME.labels(table="raw_station_status").observe(duration)

            logger.info(f"Batch inséré: {nb_inserted} statuts → ClickHouse")
            self.status_buffer.clear()
            BUFFER_SIZE.labels(buffer_type="status").set(0)
            self.last_insert_time = time.time()
            return True

        except Exception as e:
            logger.error(f"Erreur lors de l'insertion: {e}")
            self.stats['errors'] += 1
            INSERTION_ERRORS.labels(table="raw_station_status").inc()
            return False

    def insert_info_batch(self) -> bool:
        """Insère un batch d'infos dans ClickHouse avec métriques."""
        if not self.info_buffer:
            return True

        start_time = time.time()
        try:
            data = []
            for msg in self.info_buffer:
                row = (
                    msg.get('station_id', ''),
                    msg.get('stationCode', ''),
                    msg.get('name', ''),
                    msg.get('lat', 0.0),
                    msg.get('lon', 0.0),
                    msg.get('capacity', 0),
                    msg.get('rental_methods', '[]'),
                    msg.get('ingestion_timestamp', int(time.time())),
                    msg.get('api_last_updated', 0)
                )
                data.append(row)

            self.clickhouse_client.insert(
                'raw_station_info',
                data,
                column_names=[
                    'station_id', 'stationCode', 'name', 'lat', 'lon',
                    'capacity', 'rental_methods', 'ingestion_timestamp', 'api_last_updated'
                ]
            )

            nb_inserted = len(self.info_buffer)
            self.stats['messages_inserted'] += nb_inserted
            self.stats['batches_inserted'] += 1

            # Métriques Prometheus
            ROWS_INSERTED.labels(table="raw_station_info").inc(nb_inserted)
            duration = time.time() - start_time
            BATCH_PROCESSING_TIME.labels(table="raw_station_info").observe(duration)

            logger.info(f"Batch inséré: {nb_inserted} infos → ClickHouse")
            self.info_buffer.clear()
            BUFFER_SIZE.labels(buffer_type="info").set(0)
            self.last_insert_time = time.time()
            return True

        except Exception as e:
            logger.error(f"Erreur lors de l'insertion: {e}")
            self.stats['errors'] += 1
            INSERTION_ERRORS.labels(table="raw_station_info").inc()
            return False

    def should_flush(self) -> bool:
        """Détermine si on doit forcer un INSERT."""
        if len(self.status_buffer) >= self.batch_size or len(self.info_buffer) >= self.batch_size:
            return True
        if (time.time() - self.last_insert_time) > BATCH_TIMEOUT and (self.status_buffer or self.info_buffer):
            return True
        return False

    def consume(self, duration_seconds: Optional[int] = None):
        """Boucle principale de consommation."""
        logger.info("DÉMARRAGE DE LA CONSOMMATION")
        start_time = time.time()

        try:
            for message in self.consumer:
                self.stats['messages_consumed'] += 1
                topic = message.topic
                value = message.value

                # Métrique Prometheus
                MESSAGES_CONSUMED.labels(topic=topic).inc()

                if self.stats['messages_consumed'] % 100 == 0:
                    logger.info(
                        f"Consommés: {self.stats['messages_consumed']}, "
                        f"Insérés: {self.stats['messages_inserted']}, "
                        f"Batches: {self.stats['batches_inserted']}"
                    )

                if topic == KAFKA_TOPIC_STATUS:
                    transformed = self.transform_status_message(value)
                    self.status_buffer.append(transformed)
                    BUFFER_SIZE.labels(buffer_type="status").set(len(self.status_buffer))
                elif topic == KAFKA_TOPIC_INFO:
                    transformed = self.transform_info_message(value)
                    self.info_buffer.append(transformed)
                    BUFFER_SIZE.labels(buffer_type="info").set(len(self.info_buffer))

                if self.should_flush():
                    self.insert_status_batch()
                    self.insert_info_batch()

                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    logger.info(f"Durée max atteinte ({duration_seconds}s)")
                    break

        except KeyboardInterrupt:
            logger.info("Interruption par l'utilisateur")
        except Exception as e:
            logger.error(f"Erreur dans la boucle de consommation: {e}", exc_info=True)
        finally:
            self.insert_status_batch()
            self.insert_info_batch()
            self.print_stats()

    def print_stats(self):
        """Affiche les statistiques de consommation."""
        logger.info(f"Messages consommés: {self.stats['messages_consumed']}")
        logger.info(f"Messages insérés: {self.stats['messages_inserted']}")
        logger.info(f"Batches insérés: {self.stats['batches_inserted']}")
        logger.info(f"Erreurs: {self.stats['errors']}")

    def close(self):
        """Ferme proprement les connexions."""
        if self.consumer:
            self.consumer.close()
        if self.clickhouse_client:
            self.clickhouse_client.close()


# =================================================================
# FONCTION PRINCIPALE
# =================================================================

def main():
    """Point d'entrée principal du consommateur."""
    # Démarrer le serveur de métriques Prometheus
    logger.info(f"Démarrage du serveur de métriques sur le port {METRICS_PORT}")
    start_http_server(METRICS_PORT)

    consumer = VelibConsumer()
    if not consumer.connect_kafka():
        return
    if not consumer.connect_clickhouse():
        return
    try:
        consumer.consume(duration_seconds=None)
    except Exception as e:
        logger.error(f"Erreur: {e}", exc_info=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
