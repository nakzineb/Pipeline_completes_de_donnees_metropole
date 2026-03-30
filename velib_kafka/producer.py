"""
PRODUCTEUR KAFKA - API VÉLIB' VERS KAFKA
=========================================

Ce script récupère les données de l'API Vélib' et les envoie dans Kafka.
Expose des métriques Prometheus sur le port 8000.
"""

import json
import logging
import time
from typing import Dict, List, Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Histogram, start_http_server

# =================================================================
# CONFIGURATION
# =================================================================

VELIB_STATION_STATUS_URL = (
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
)
VELIB_STATION_INFO_URL = (
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
)

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC_STATUS = 'velib-station-status'
KAFKA_TOPIC_INFO = 'velib-station-info'

METRICS_PORT = 8000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =================================================================
# MÉTRIQUES PROMETHEUS
# =================================================================

MESSAGES_PRODUCED = Counter(
    'velib_messages_produced_total',
    'Nombre total de messages produits vers Kafka',
    ['topic', 'status']
)

API_CALLS = Counter(
    'velib_api_calls_total',
    'Nombre total d\'appels API',
    ['endpoint']
)

API_ERRORS = Counter(
    'velib_api_errors_total',
    'Nombre total d\'erreurs API',
    ['endpoint']
)

API_RESPONSE_TIME = Histogram(
    'velib_api_response_seconds',
    'Temps de réponse de l\'API en secondes',
    ['endpoint'],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)


# =================================================================
# CLASSE PRODUCTEUR VÉLIB'
# =================================================================

class VelibProducer:
    """
    Producteur Kafka pour les données Vélib'.
    Récupère les données de l'API et les envoie dans Kafka.
    """

    def __init__(
        self,
        bootstrap_servers: List[str] = None,
        status_topic: str = KAFKA_TOPIC_STATUS,
        info_topic: str = KAFKA_TOPIC_INFO
    ):
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.status_topic = status_topic
        self.info_topic = info_topic
        self.producer = None
        logger.info("Initialisation du producteur Vélib'")
        logger.info(f"Kafka servers: {self.bootstrap_servers}")
        logger.info(f"Topics: status={status_topic}, info={info_topic}")

    def connect(self) -> bool:
        """Établit la connexion au broker Kafka."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                compression_type='gzip',
                retries=3,
                max_in_flight_requests_per_connection=1,
                api_version=(2, 5, 0)
            )
            logger.info("Connexion à Kafka établie")
            return True
        except Exception as e:
            logger.error(f"Erreur de connexion à Kafka: {e}")
            return False

    def fetch_api(self, url: str) -> Optional[Dict]:
        """Récupère les données depuis l'API Vélib' avec métriques."""
        endpoint = "station_status" if "status" in url else "station_information"
        API_CALLS.labels(endpoint=endpoint).inc()

        start_time = time.time()
        try:
            logger.info(f"Appel API: {url}")
            response = requests.get(url, timeout=10)
            duration = time.time() - start_time
            API_RESPONSE_TIME.labels(endpoint=endpoint).observe(duration)

            if response.status_code != 200:
                logger.error(f"Erreur HTTP {response.status_code}")
                API_ERRORS.labels(endpoint=endpoint).inc()
                return None

            data = response.json()
            if 'data' not in data:
                logger.error("Pas de clé 'data' dans la réponse")
                API_ERRORS.labels(endpoint=endpoint).inc()
                return None
            if 'stations' not in data['data']:
                logger.error("Pas de clé 'stations' dans data")
                API_ERRORS.labels(endpoint=endpoint).inc()
                return None

            stations_count = len(data['data']['stations'])
            logger.info(f"API OK - {stations_count} stations récupérées")
            return data

        except requests.exceptions.Timeout:
            duration = time.time() - start_time
            API_RESPONSE_TIME.labels(endpoint=endpoint).observe(duration)
            API_ERRORS.labels(endpoint=endpoint).inc()
            logger.error("Timeout lors de l'appel API")
            return None
        except requests.exceptions.RequestException as e:
            API_ERRORS.labels(endpoint=endpoint).inc()
            logger.error(f"Erreur réseau: {e}")
            return None
        except json.JSONDecodeError as e:
            API_ERRORS.labels(endpoint=endpoint).inc()
            logger.error(f"Erreur parsing JSON: {e}")
            return None

    def send_to_kafka(self, topic: str, key: str, value: Dict) -> bool:
        """Envoie un message dans Kafka avec métriques."""
        try:
            future = self.producer.send(
                topic,
                key=str(key).encode('utf-8'),
                value=value
            )
            future.get(timeout=10)
            MESSAGES_PRODUCED.labels(topic=topic, status="success").inc()
            return True
        except KafkaError as e:
            MESSAGES_PRODUCED.labels(topic=topic, status="error").inc()
            logger.error(f"Erreur Kafka: {e}")
            return False
        except Exception as e:
            MESSAGES_PRODUCED.labels(topic=topic, status="error").inc()
            logger.error(f"Erreur inattendue lors de l'envoi: {e}")
            return False

    def produce_station_status(self) -> int:
        """Récupère et envoie les statuts des stations dans Kafka."""
        logger.info("PRODUCTION DES STATUTS DES STATIONS")
        data = self.fetch_api(VELIB_STATION_STATUS_URL)
        if not data:
            logger.error("Impossible de récupérer les données de statut")
            return 0

        stations = data['data']['stations']
        last_updated = data.get('last_updated', int(time.time()))
        success_count = 0
        error_count = 0

        for station in stations:
            station_id = str(station.get('station_id', ''))
            if not station_id or station_id == '':
                continue
            enriched_data = {
                'station_id': station_id,
                'num_bikes_available': station.get('num_bikes_available', 0),
                'num_bikes_available_types': station.get('num_bikes_available_types', []),
                'num_docks_available': station.get('num_docks_available', 0),
                'is_installed': station.get('is_installed', 0),
                'is_returning': station.get('is_returning', 0),
                'is_renting': station.get('is_renting', 0),
                'last_reported': station.get('last_reported', last_updated),
                'ingestion_timestamp': int(time.time()),
                'api_last_updated': last_updated
            }
            if self.send_to_kafka(self.status_topic, station_id, enriched_data):
                success_count += 1
            else:
                error_count += 1

        self.producer.flush()
        logger.info(f"Production terminée - Succès: {success_count}, Erreurs: {error_count}")
        return success_count

    def produce_station_information(self) -> int:
        """Récupère et envoie les informations des stations dans Kafka."""
        logger.info("PRODUCTION DES INFORMATIONS DES STATIONS")
        data = self.fetch_api(VELIB_STATION_INFO_URL)
        if not data:
            logger.error("Impossible de récupérer les informations des stations")
            return 0

        stations = data['data']['stations']
        last_updated = data.get('last_updated', int(time.time()))
        success_count = 0
        error_count = 0

        for station in stations:
            station_id = str(station.get('station_id', ''))
            if not station_id or station_id == '':
                continue
            station_data = {
                'station_id': station_id,
                'name': station.get('name', ''),
                'lat': station.get('lat', 0.0),
                'lon': station.get('lon', 0.0),
                'capacity': station.get('capacity', 0),
                'stationCode': station.get('stationCode', ''),
                'rental_methods': station.get('rental_methods', []),
                'ingestion_timestamp': int(time.time()),
                'api_last_updated': last_updated
            }
            if self.send_to_kafka(self.info_topic, station_id, station_data):
                success_count += 1
            else:
                error_count += 1

        self.producer.flush()
        logger.info(f"Production terminée - Succès: {success_count}, Erreurs: {error_count}")
        return success_count

    def close(self):
        """Ferme proprement la connexion Kafka."""
        if self.producer:
            self.producer.close()
            logger.info("Connexion fermée")


# =================================================================
# FONCTION PRINCIPALE
# =================================================================

def main():
    """Point d'entrée principal du producteur."""
    # Démarrer le serveur de métriques Prometheus
    logger.info(f"Démarrage du serveur de métriques sur le port {METRICS_PORT}")
    start_http_server(METRICS_PORT)

    logger.info("Démarrage du producteur Vélib'")
    producer = VelibProducer()
    if not producer.connect():
        logger.error("Impossible de se connecter à Kafka")
        return
    try:
        status_count = producer.produce_station_status()
        time.sleep(1)
        info_count = producer.produce_station_information()
        logger.info(f"Statuts envoyés: {status_count}, Infos envoyées: {info_count}")
    except KeyboardInterrupt:
        logger.info("Interruption par l'utilisateur")
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}", exc_info=True)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
