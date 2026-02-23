#!/usr/bin/env python3
"""
SCRIPT DE TEST DU PRODUCTEUR
=============================

Ce script teste le producteur Kafka de manière simple.
Il permet de vérifier que tout fonctionne avant d'intégrer à Airflow.

Usage:
    python scripts/test_producer.py
"""

import sys
import os

# Ajouter le dossier parent au path pour importer le module velib_kafka
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from velib_kafka.producer import VelibProducer
import logging

# Configuration du logging plus verbeux pour les tests
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_connection():
    """Teste la connexion à Kafka."""
    logger.info(" Test 1 : Connexion à Kafka")
    logger.info("-" * 60)
    
    producer = VelibProducer()
    
    if producer.connect():
        logger.info(" Test réussi : Connexion établie")
        producer.close()
        return True
    else:
        logger.error(" Test échoué : Impossible de se connecter")
        return False

def test_api_fetch():
    """Teste la récupération des données de l'API."""
    logger.info("\n Test 2 : Appel API Vélib'")
    logger.info("-" * 60)
    
    producer = VelibProducer()
    
    # Test station_status
    logger.info("Test station_status...")
    status_data = producer.fetch_api(
        "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
    )
    
    if status_data:
        logger.info(f" station_status OK - {len(status_data['data']['stations'])} stations")
    else:
        logger.error(" Erreur récupération station_status")
        return False
    
    # Test station_information
    logger.info("Test station_information...")
    info_data = producer.fetch_api(
        "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
    )
    
    if info_data:
        logger.info(f" station_information OK - {len(info_data['data']['stations'])} stations")
    else:
        logger.error(" Erreur récupération station_information")
        return False
    
    return True

def test_produce_status():
    """Teste l'envoi des statuts dans Kafka."""
    logger.info("\n Test 3 : Production des statuts")
    logger.info("-" * 60)
    
    producer = VelibProducer()
    
    if not producer.connect():
        logger.error(" Impossible de se connecter")
        return False
    
    try:
        count = producer.produce_station_status()
        
        if count > 0:
            logger.info(f" Test réussi : {count} stations envoyées")
            return True
        else:
            logger.error(" Aucune station envoyée")
            return False
            
    finally:
        producer.close()

def test_produce_info():
    """Teste l'envoi des informations dans Kafka."""
    logger.info("\n Test 4 : Production des informations")
    logger.info("-" * 60)
    
    producer = VelibProducer()
    
    if not producer.connect():
        logger.error(" Impossible de se connecter")
        return False
    
    try:
        count = producer.produce_station_information()
        
        if count > 0:
            logger.info(f" Test réussi : {count} stations envoyées")
            return True
        else:
            logger.error(" Aucune station envoyée")
            return False
            
    finally:
        producer.close()

def verify_kafka_topics():
    """Vérifie que les messages sont bien dans Kafka."""
    logger.info("\n🧪 Test 5 : Vérification des messages dans Kafka")
    logger.info("-" * 60)
    
    try:
        from kafka import KafkaConsumer
        import json
        
        # Créer un consommateur temporaire
        consumer = KafkaConsumer(
            'velib-station-status',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=5000,  # 5 secondes max
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Lire quelques messages
        message_count = 0
        for message in consumer:
            message_count += 1
            if message_count == 1:
                # Afficher le premier message pour vérification
                logger.info(f"Premier message (station {message.value['station_id']}):")
                logger.info(json.dumps(message.value, indent=2, ensure_ascii=False))
            
            if message_count >= 5:  # Lire max 5 messages
                break
        
        consumer.close()
        
        if message_count > 0:
            logger.info(f" Test réussi : {message_count} messages lus dans Kafka")
            return True
        else:
            logger.warning("⚠️  Aucun message trouvé (peut-être que le topic est vide)")
            return False
            
    except Exception as e:
        logger.error(f" Erreur lors de la lecture Kafka: {e}")
        return False

def main():
    """Exécute tous les tests."""
    logger.info("=" * 60)
    logger.info(" TESTS DU PRODUCTEUR VELIB")
    logger.info("=" * 60)
    
    tests = [
        ("Connexion Kafka", test_connection),
        ("Appel API", test_api_fetch),
        ("Production statuts", test_produce_status),
        ("Production infos", test_produce_info),
        ("Vérification Kafka", verify_kafka_topics),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f" Erreur inattendue dans {test_name}: {e}")
            results.append((test_name, False))
    
    # Afficher le résumé
    logger.info("\n" + "=" * 60)
    logger.info(" RÉSUMÉ DES TESTS")
    logger.info("=" * 60)
    
    for test_name, result in results:
        status = " PASS" if result else " FAIL"
        logger.info(f"{status} - {test_name}")
    
    total_pass = sum(1 for _, result in results if result)
    total_tests = len(results)
    
    logger.info("=" * 60)
    logger.info(f"Résultat : {total_pass}/{total_tests} tests réussis")
    logger.info("=" * 60)
    
    if total_pass == total_tests:
        logger.info("🎉 Tous les tests sont passés !")
        return 0
    else:
        logger.warning(f"  {total_tests - total_pass} test(s) échoué(s)")
        return 1

if __name__ == "__main__":
    sys.exit(main())