"""
PRODUCTEUR KAFKA - API VÉLIB' VERS KAFKA
=========================================

Ce script récupère les données de l'API Vélib' et les envoie dans Kafka.

Fonctionnement :
1. Appelle l'API Vélib' (station_status.json et station_information.json)
2. Parse les données JSON
3. Envoie chaque station dans un topic Kafka
4. Gère les erreurs et retries

Topics Kafka créés :
- velib-station-status : État temps réel des stations
- velib-station-info : Métadonnées des stations
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# =================================================================
# CONFIGURATION
# =================================================================

# URLs de l'API Vélib' GBFS
VELIB_STATION_STATUS_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
VELIB_STATION_INFO_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']  # Depuis l'hôte
# Si ce script tourne dans un container Docker : ['kafka:29092']

KAFKA_TOPIC_STATUS = 'velib-station-status'
KAFKA_TOPIC_INFO = 'velib-station-info'

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =================================================================
# CLASSE PRODUCTEUR VÉLIB'
# =================================================================

class VelibProducer:
    """
    Producteur Kafka pour les données Vélib'.
    
    Responsabilités :
    - Récupérer les données de l'API
    - Les transformer en messages Kafka
    - Les envoyer dans les topics appropriés
    """
    
    def __init__(
        self, 
        bootstrap_servers: List[str] = KAFKA_BOOTSTRAP_SERVERS,
        status_topic: str = KAFKA_TOPIC_STATUS,
        info_topic: str = KAFKA_TOPIC_INFO
    ):
        """
        Initialise le producteur Kafka.
        
        Args:
            bootstrap_servers: Liste des serveurs Kafka
            status_topic: Nom du topic pour les statuts
            info_topic: Nom du topic pour les informations
        """
        self.bootstrap_servers = bootstrap_servers
        self.status_topic = status_topic
        self.info_topic = info_topic
        self.producer = None
        
        logger.info(f"Initialisation du producteur Vélib'")
        logger.info(f"Kafka servers: {bootstrap_servers}")
        logger.info(f"Topics: status={status_topic}, info={info_topic}")
    
    def connect(self) -> bool:
        """
        Établit la connexion au broker Kafka.
        
        Returns:
            True si la connexion réussit, False sinon
        """
        try:
            # EXPLICATION DES PARAMÈTRES :
            # - bootstrap_servers : où se trouve Kafka
            # - value_serializer : comment convertir nos données Python en bytes
            #   On utilise JSON pour faciliter la lecture
            # - acks='all' : attendre la confirmation de tous les replicas
            #   (plus lent mais plus sûr, aucune perte de données)
            # - compression_type='gzip' : compresser les messages (économie réseau)
            # - retries=3 : réessayer 3 fois en cas d'échec
            
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                compression_type='gzip',
                retries=3,
                max_in_flight_requests_per_connection=1,  # Garantit l'ordre
                api_version=(2, 5, 0)  # Forcer la version de l'API Kafka
            )
            
            logger.info(" Connexion à Kafka établie")
            return True
            
        except Exception as e:
            logger.error(f" Erreur de connexion à Kafka: {e}")
            return False
    
    def fetch_api(self, url: str) -> Optional[Dict]:
        """
        Récupère les données depuis l'API Vélib'.
        
        Args:
            url: URL de l'endpoint API
            
        Returns:
            Données JSON parsées ou None en cas d'erreur
        """
        try:
            logger.info(f"📡 Appel API: {url}")
            
            # Appel HTTP GET avec timeout de 10 secondes
            response = requests.get(url, timeout=10)
            
            # Vérifier le code de statut HTTP
            if response.status_code != 200:
                logger.error(f" Erreur HTTP {response.status_code}")
                return None
            
            # Parser le JSON
            data = response.json()
            
            # Vérifications de base
            if 'data' not in data:
                logger.error(" Pas de clé 'data' dans la réponse")
                return None
            
            if 'stations' not in data['data']:
                logger.error(" Pas de clé 'stations' dans data")
                return None
            
            stations_count = len(data['data']['stations'])
            logger.info(f" API OK - {stations_count} stations récupérées")
            
            return data
            
        except requests.exceptions.Timeout:
            logger.error(" Timeout lors de l'appel API")
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f" Erreur réseau: {e}")
            return None
            
        except json.JSONDecodeError as e:
            logger.error(f" Erreur parsing JSON: {e}")
            return None
    
    def send_to_kafka(self, topic: str, key: str, value: Dict) -> bool:
        """
        Envoie un message dans Kafka.
        
        Args:
            topic: Nom du topic
            key: Clé du message (station_id)
            value: Valeur du message (données de la station)
            
        Returns:
            True si envoi réussi, False sinon
        """
        try:
            # EXPLICATION :
            # - topic : dans quel "canal" envoyer le message
            # - key : identifiant unique (permet le partitioning dans Kafka)
            #   Les messages avec la même clé vont dans la même partition
            #   → garantit l'ordre pour une station donnée
            # - value : les données à envoyer
            
            future = self.producer.send(
                topic,
                key=str(key).encode('utf-8'),  # Convertir en string d'abord
                value=value
            )
            
            # Attendre la confirmation (bloquant)
            record_metadata = future.get(timeout=10)
            
            # Log uniquement pour debug (sinon trop de logs)
            # logger.debug(
            #     f"Message envoyé - Topic: {record_metadata.topic}, "
            #     f"Partition: {record_metadata.partition}, "
            #     f"Offset: {record_metadata.offset}"
            # )
            
            return True
            
        except KafkaError as e:
            logger.error(f" Erreur Kafka: {e}")
            return False
            
        except Exception as e:
            logger.error(f" Erreur inattendue lors de l'envoi: {e}")
            return False
    
    def produce_station_status(self) -> int:
        """
        Récupère et envoie les statuts des stations dans Kafka.
        
        Returns:
            Nombre de stations envoyées avec succès
        """
        logger.info("=" * 60)
        logger.info(" PRODUCTION DES STATUTS DES STATIONS")
        logger.info("=" * 60)
        
        # Récupérer les données de l'API
        data = self.fetch_api(VELIB_STATION_STATUS_URL)
        
        if not data:
            logger.error("Impossible de récupérer les données de statut")
            return 0
        
        stations = data['data']['stations']
        last_updated = data.get('last_updated', int(time.time()))
        
        logger.info(f"Dernière mise à jour API: {datetime.fromtimestamp(last_updated)}")
        
        success_count = 0
        error_count = 0
        
        # Parcourir chaque station
        for station in stations:
            station_id = str(station.get('station_id', ''))
            
            if not station_id or station_id == '':
                logger.warning("Station sans ID, ignorée")
                continue
            
            # Enrichir les données avec des métadonnées
            enriched_data = {
                'station_id': station_id,
                'num_bikes_available': station.get('num_bikes_available', 0),
                'num_bikes_available_types': station.get('num_bikes_available_types', []),
                'num_docks_available': station.get('num_docks_available', 0),
                'is_installed': station.get('is_installed', 0),
                'is_returning': station.get('is_returning', 0),
                'is_renting': station.get('is_renting', 0),
                'last_reported': station.get('last_reported', last_updated),
                # Métadonnées ajoutées par nous
                'ingestion_timestamp': int(time.time()),
                'api_last_updated': last_updated
            }
            
            # Envoyer dans Kafka
            if self.send_to_kafka(self.status_topic, station_id, enriched_data):
                success_count += 1
            else:
                error_count += 1
        
        # Forcer l'envoi de tous les messages bufferisés
        self.producer.flush()
        
        logger.info("=" * 60)
        logger.info(f" Production terminée")
        logger.info(f"   Succès: {success_count}")
        logger.info(f"   Erreurs: {error_count}")
        logger.info(f"   Total: {len(stations)}")
        logger.info("=" * 60)
        
        return success_count
    
    def produce_station_information(self) -> int:
        """
        Récupère et envoie les informations des stations dans Kafka.
        
        Returns:
            Nombre de stations envoyées avec succès
        """
        logger.info("=" * 60)
        logger.info(" PRODUCTION DES INFORMATIONS DES STATIONS")
        logger.info("=" * 60)
        
        # Récupérer les données de l'API
        data = self.fetch_api(VELIB_STATION_INFO_URL)
        
        if not data:
            logger.error("Impossible de récupérer les informations des stations")
            return 0
        
        stations = data['data']['stations']
        last_updated = data.get('last_updated', int(time.time()))
        
        success_count = 0
        error_count = 0
        
        # Parcourir chaque station
        for station in stations:
            station_id = str(station.get('station_id', ''))
            
            if not station_id or station_id == '':
                logger.warning("Station sans ID, ignorée")
                continue
            
            # Structurer les données
            station_data = {
                'station_id': station_id,
                'name': station.get('name', ''),
                'lat': station.get('lat', 0.0),
                'lon': station.get('lon', 0.0),
                'capacity': station.get('capacity', 0),
                'stationCode': station.get('stationCode', ''),
                'rental_methods': station.get('rental_methods', []),
                # Métadonnées
                'ingestion_timestamp': int(time.time()),
                'api_last_updated': last_updated
            }
            
            # Envoyer dans Kafka
            if self.send_to_kafka(self.info_topic, station_id, station_data):
                success_count += 1
            else:
                error_count += 1
        
        # Forcer l'envoi
        self.producer.flush()
        
        logger.info("=" * 60)
        logger.info(f" Production terminée")
        logger.info(f"   Succès: {success_count}")
        logger.info(f"   Erreurs: {error_count}")
        logger.info(f"   Total: {len(stations)}")
        logger.info("=" * 60)
        
        return success_count
    
    def close(self):
        """Ferme proprement la connexion Kafka."""
        if self.producer:
            logger.info("Fermeture de la connexion Kafka...")
            self.producer.close()
            logger.info(" Connexion fermée")

# =================================================================
# FONCTION PRINCIPALE
# =================================================================

def main():
    """
    Point d'entrée principal du producteur.
    """
    logger.info(" Démarrage du producteur Vélib'")
    
    # Créer le producteur
    producer = VelibProducer()
    
    # Se connecter à Kafka
    if not producer.connect():
        logger.error("Impossible de se connecter à Kafka")
        return
    
    try:
        # Produire les données de statut
        status_count = producer.produce_station_status()
        
        # Petite pause entre les deux appels API
        time.sleep(1)
        
        # Produire les informations des stations
        info_count = producer.produce_station_information()
        
        logger.info(f" Production terminée avec succès")
        logger.info(f"   Statuts envoyés: {status_count}")
        logger.info(f"   Infos envoyées: {info_count}")
        
    except KeyboardInterrupt:
        logger.info("⚠️  Interruption par l'utilisateur")
        
    except Exception as e:
        logger.error(f" Erreur inattendue: {e}", exc_info=True)
        
    finally:
        # Toujours fermer proprement la connexion
        producer.close()

# =================================================================
# EXÉCUTION
# =================================================================

if __name__ == "__main__":
    main()