"""
CONSOMMATEUR KAFKA - KAFKA VERS CLICKHOUSE
===========================================

Ce script consomme les messages depuis Kafka et les insère dans ClickHouse.

Fonctionnement :
1. Se connecte à Kafka et ClickHouse
2. Lit les messages des topics (batch)
3. Transforme les données si nécessaire
4. Insert en batch dans ClickHouse
5. Commit les offsets Kafka

Topics consommés :
- velib-station-status : État temps réel des stations
- velib-station-info : Métadonnées des stations
"""

import json
import logging
import time
from typing import List, Dict, Optional
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import clickhouse_connect

# =================================================================
# CONFIGURATION
# =================================================================

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']  # Depuis l'hôte
# Si dans un container Docker : ['kafka:29092']

KAFKA_TOPIC_STATUS = 'velib-station-status'
KAFKA_TOPIC_INFO = 'velib-station-info'
KAFKA_CONSUMER_GROUP = 'velib-consumer-group'

# Configuration ClickHouse
CLICKHOUSE_HOST = 'localhost'  # Depuis l'hôte
# Si dans un container Docker : 'clickhouse'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'clickhouse123'
CLICKHOUSE_DATABASE = 'velib_db'

# Configuration du batch
BATCH_SIZE = 100  # Nombre de messages à accumuler avant INSERT
BATCH_TIMEOUT = 5  # Secondes max avant de forcer un INSERT
MAX_POLL_RECORDS = 500  # Messages max à lire par poll

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =================================================================
# CLASSE CONSOMMATEUR VÉLIB'
# =================================================================

class VelibConsumer:
    """
    Consommateur Kafka pour les données Vélib'.
    
    Responsabilités :
    - Lire les messages depuis Kafka
    - Les transformer au format ClickHouse
    - Les insérer en batch dans ClickHouse
    - Gérer les offsets et la fiabilité
    """
    
    def __init__(
        self,
        bootstrap_servers: List[str] = KAFKA_BOOTSTRAP_SERVERS,
        consumer_group: str = KAFKA_CONSUMER_GROUP,
        clickhouse_host: str = CLICKHOUSE_HOST,
        clickhouse_port: int = CLICKHOUSE_PORT,
        batch_size: int = BATCH_SIZE
    ):
        """
        Initialise le consommateur.
        
        Args:
            bootstrap_servers: Serveurs Kafka
            consumer_group: Groupe de consommateurs
            clickhouse_host: Hôte ClickHouse
            clickhouse_port: Port ClickHouse
            batch_size: Taille du batch pour INSERT
        """
        self.bootstrap_servers = bootstrap_servers
        self.consumer_group = consumer_group
        self.batch_size = batch_size
        
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        
        self.consumer = None
        self.clickhouse_client = None
        
        # Buffers pour batching
        self.status_buffer = []
        self.info_buffer = []
        self.last_insert_time = time.time()
        
        # Statistiques
        self.stats = {
            'messages_consumed': 0,
            'messages_inserted': 0,
            'batches_inserted': 0,
            'errors': 0
        }
        
        logger.info(f"Initialisation du consommateur Vélib'")
        logger.info(f"Kafka: {bootstrap_servers}, Groupe: {consumer_group}")
        logger.info(f"ClickHouse: {clickhouse_host}:{clickhouse_port}")
        logger.info(f"Batch size: {batch_size}")
    
    def connect_kafka(self) -> bool:
        """
        Établit la connexion au broker Kafka.
        
        Returns:
            True si succès, False sinon
        """
        try:
            # EXPLICATION DES PARAMÈTRES :
            # - bootstrap_servers : où se trouve Kafka
            # - group_id : identifiant du groupe de consommateurs
            #   (permet de répartir la charge entre plusieurs consommateurs)
            # - auto_offset_reset : 'earliest' = lire depuis le début si nouveau groupe
            #                       'latest' = lire seulement les nouveaux messages
            # - enable_auto_commit : commit automatique des offsets (simplifie)
            # - value_deserializer : comment convertir les bytes en Python dict
            # - max_poll_records : limite le nombre de messages par poll
            
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC_STATUS,
                KAFKA_TOPIC_INFO,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.consumer_group,
                auto_offset_reset='earliest',  # Commencer au début si nouveau
                enable_auto_commit=True,       # Commit auto (plus simple)
                auto_commit_interval_ms=1000,  # Commit toutes les 1s
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                max_poll_records=MAX_POLL_RECORDS,
                api_version=(2, 5, 0)          # Même version que le producteur
            )
            
            logger.info(" Connexion à Kafka établie")
            logger.info(f"Topics souscrits: {self.consumer.subscription()}")
            return True
            
        except Exception as e:
            logger.error(f" Erreur de connexion à Kafka: {e}")
            return False
    
    def connect_clickhouse(self) -> bool:
        """
        Établit la connexion à ClickHouse.
        
        Returns:
            True si succès, False sinon
        """
        try:
            # EXPLICATION :
            # clickhouse_connect est le client Python officiel
            # Plus moderne et performant que l'ancien clickhouse-driver
            
            self.clickhouse_client = clickhouse_connect.get_client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE
            )
            
            # Test de connexion
            result = self.clickhouse_client.query("SELECT 1")
            
            logger.info(" Connexion à ClickHouse établie")
            logger.info(f"Base de données: {CLICKHOUSE_DATABASE}")
            return True
            
        except Exception as e:
            logger.error(f" Erreur de connexion à ClickHouse: {e}")
            return False
    
    def transform_status_message(self, message: Dict) -> Dict:
        """
        Transforme un message de statut pour ClickHouse.
        
        Args:
            message: Message brut de Kafka
            
        Returns:
            Message transformé pour ClickHouse
        """
        # Le message de Kafka contient déjà tous les champs nécessaires
        # On s'assure juste que num_bikes_available_types est une string JSON
        
        if isinstance(message.get('num_bikes_available_types'), list):
            # Convertir le tableau en JSON string pour ClickHouse
            message['num_bikes_available_types'] = json.dumps(
                message['num_bikes_available_types']
            )
        
        return message
    
    def transform_info_message(self, message: Dict) -> Dict:
        """
        Transforme un message d'info pour ClickHouse.
        
        Args:
            message: Message brut de Kafka
            
        Returns:
            Message transformé pour ClickHouse
        """
        # Convertir rental_methods en JSON string si c'est une liste
        if isinstance(message.get('rental_methods'), list):
            message['rental_methods'] = json.dumps(message['rental_methods'])
        
        return message
    
    def insert_status_batch(self) -> bool:
        """
        Insère un batch de statuts dans ClickHouse.
        
        Returns:
            True si succès, False sinon
        """
        if not self.status_buffer:
            return True
        
        try:
            # EXPLICATION DE L'INSERT :
            # - On utilise insert() avec une liste de dicts
            # - ClickHouse attend des colonnes dans un ordre précis
            # - On crée une liste de tuples dans le bon ordre
            
            # Préparer les données pour INSERT
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
            
            # INSERT dans ClickHouse
            self.clickhouse_client.insert(
                'raw_station_status',
                data,
                column_names=[
                    'station_id',
                    'num_bikes_available',
                    'num_bikes_available_types',
                    'num_docks_available',
                    'is_installed',
                    'is_returning',
                    'is_renting',
                    'last_reported',
                    'ingestion_timestamp',
                    'api_last_updated'
                ]
            )
            
            # Statistiques
            nb_inserted = len(self.status_buffer)
            self.stats['messages_inserted'] += nb_inserted
            self.stats['batches_inserted'] += 1
            
            logger.info(f" Batch inséré: {nb_inserted} statuts → ClickHouse")
            
            # Vider le buffer
            self.status_buffer.clear()
            self.last_insert_time = time.time()
            
            return True
            
        except Exception as e:
            logger.error(f" Erreur lors de l'insertion: {e}")
            self.stats['errors'] += 1
            # En cas d'erreur, on garde le buffer pour retry
            return False
    
    def insert_info_batch(self) -> bool:
        """
        Insère un batch d'infos dans ClickHouse.
        
        Returns:
            True si succès, False sinon
        """
        if not self.info_buffer:
            return True
        
        try:
            # Préparer les données
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
            
            # INSERT
            self.clickhouse_client.insert(
                'raw_station_info',
                data,
                column_names=[
                    'station_id',
                    'stationCode',
                    'name',
                    'lat',
                    'lon',
                    'capacity',
                    'rental_methods',
                    'ingestion_timestamp',
                    'api_last_updated'
                ]
            )
            
            nb_inserted = len(self.info_buffer)
            self.stats['messages_inserted'] += nb_inserted
            self.stats['batches_inserted'] += 1
            
            logger.info(f" Batch inséré: {nb_inserted} infos → ClickHouse")
            
            # Vider le buffer
            self.info_buffer.clear()
            self.last_insert_time = time.time()
            
            return True
            
        except Exception as e:
            logger.error(f" Erreur lors de l'insertion: {e}")
            self.stats['errors'] += 1
            return False
    
    def should_flush(self) -> bool:
        """
        Détermine si on doit forcer un INSERT (timeout ou buffer plein).
        
        Returns:
            True si on doit flush
        """
        # Buffer plein ?
        if len(self.status_buffer) >= self.batch_size or \
           len(self.info_buffer) >= self.batch_size:
            return True
        
        # Timeout dépassé et buffer non vide ?
        if (time.time() - self.last_insert_time) > BATCH_TIMEOUT and \
           (self.status_buffer or self.info_buffer):
            return True
        
        return False
    
    def consume(self, duration_seconds: Optional[int] = None):
        """
        Boucle principale de consommation.
        
        Args:
            duration_seconds: Durée max en secondes (None = infini)
        """
        logger.info("=" * 60)
        logger.info(" DÉMARRAGE DE LA CONSOMMATION")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        try:
            # Boucle infinie de consommation
            for message in self.consumer:
                # Statistiques
                self.stats['messages_consumed'] += 1
                
                # Déterminer le type de message selon le topic
                topic = message.topic
                value = message.value
                
                # Log périodique
                if self.stats['messages_consumed'] % 100 == 0:
                    logger.info(
                        f" Consommés: {self.stats['messages_consumed']}, "
                        f"Insérés: {self.stats['messages_inserted']}, "
                        f"Batches: {self.stats['batches_inserted']}"
                    )
                
                # Router vers le bon buffer
                if topic == KAFKA_TOPIC_STATUS:
                    transformed = self.transform_status_message(value)
                    self.status_buffer.append(transformed)
                    
                elif topic == KAFKA_TOPIC_INFO:
                    transformed = self.transform_info_message(value)
                    self.info_buffer.append(transformed)
                
                # Flush si nécessaire
                if self.should_flush():
                    self.insert_status_batch()
                    self.insert_info_batch()
                
                # Arrêt si durée dépassée
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    logger.info(f"⏱  Durée max atteinte ({duration_seconds}s)")
                    break
        
        except KeyboardInterrupt:
            logger.info("  Interruption par l'utilisateur")
        
        except Exception as e:
            logger.error(f" Erreur dans la boucle de consommation: {e}", exc_info=True)
        
        finally:
            # Flush final des buffers
            logger.info(" Flush final des buffers...")
            self.insert_status_batch()
            self.insert_info_batch()
            
            # Afficher les stats finales
            self.print_stats()
    
    def print_stats(self):
        """Affiche les statistiques de consommation."""
        logger.info("=" * 60)
        logger.info(" STATISTIQUES FINALES")
        logger.info("=" * 60)
        logger.info(f"Messages consommés: {self.stats['messages_consumed']}")
        logger.info(f"Messages insérés: {self.stats['messages_inserted']}")
        logger.info(f"Batches insérés: {self.stats['batches_inserted']}")
        logger.info(f"Erreurs: {self.stats['errors']}")
        logger.info("=" * 60)
    
    def close(self):
        """Ferme proprement les connexions."""
        logger.info("Fermeture des connexions...")
        
        if self.consumer:
            self.consumer.close()
            logger.info("✅ Kafka fermé")
        
        if self.clickhouse_client:
            self.clickhouse_client.close()
            logger.info("✅ ClickHouse fermé")

# =================================================================
# FONCTION PRINCIPALE
# =================================================================

def main():
    """
    Point d'entrée principal du consommateur.
    """
    logger.info(" Démarrage du consommateur Vélib'")
    
    # Créer le consommateur
    consumer = VelibConsumer()
    
    # Se connecter à Kafka
    if not consumer.connect_kafka():
        logger.error("Impossible de se connecter à Kafka")
        return
    
    # Se connecter à ClickHouse
    if not consumer.connect_clickhouse():
        logger.error("Impossible de se connecter à ClickHouse")
        return
    
    try:
        # Lancer la consommation
        # duration_seconds=None → infini (Ctrl+C pour arrêter)
        # duration_seconds=60 → arrêt après 60 secondes
        consumer.consume(duration_seconds=None)
        
    except Exception as e:
        logger.error(f" Erreur: {e}", exc_info=True)
        
    finally:
        # Toujours fermer proprement
        consumer.close()
        logger.info(" Consommateur arrêté")

# =================================================================
# EXÉCUTION
# =================================================================

if __name__ == "__main__":
    main()