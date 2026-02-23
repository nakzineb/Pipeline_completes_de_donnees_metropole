#!/usr/bin/env python3
"""
SCRIPT DE TEST DU CONSOMMATEUR
===============================

Ce script teste le consommateur Kafka de manière contrôlée.

Usage:
    python scripts/test_consumer.py
"""

import sys
import os
import time

# Ajouter le dossier parent au path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from velib_kafka.consumer import VelibConsumer
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_connections():
    """Teste les connexions Kafka et ClickHouse."""
    logger.info(" Test : Connexions")
    logger.info("-" * 60)
    
    consumer = VelibConsumer()
    
    # Test Kafka
    if not consumer.connect_kafka():
        logger.error(" Connexion Kafka échouée")
        return False
    logger.info(" Kafka OK")
    
    # Test ClickHouse
    if not consumer.connect_clickhouse():
        logger.error(" Connexion ClickHouse échouée")
        return False
    logger.info(" ClickHouse OK")
    
    consumer.close()
    return True

def test_consume_limited():
    """Teste la consommation pendant 30 secondes."""
    logger.info("\n Test : Consommation (30 secondes)")
    logger.info("-" * 60)
    
    consumer = VelibConsumer(batch_size=10)  # Petit batch pour test
    
    if not consumer.connect_kafka():
        logger.error(" Impossible de se connecter à Kafka")
        return False
    
    if not consumer.connect_clickhouse():
        logger.error(" Impossible de se connecter à ClickHouse")
        return False
    
    try:
        # Consommer pendant 30 secondes
        logger.info("Consommation pendant 30 secondes...")
        consumer.consume(duration_seconds=30)
        
        # Vérifier les stats
        if consumer.stats['messages_consumed'] > 0:
            logger.info(" Messages consommés avec succès")
            return True
        else:
            logger.warning("  Aucun message consommé (topics vides ?)")
            return True  # Pas une erreur si topics vides
            
    finally:
        consumer.close()

def verify_clickhouse_data():
    """Vérifie que les données sont dans ClickHouse."""
    logger.info("\n Test : Vérification ClickHouse")
    logger.info("-" * 60)
    
    try:
        import clickhouse_connect
        
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='default',
            password='clickhouse123',
            database='velib_db'
        )
        
        # Compter les lignes dans raw_station_status
        result = client.query("SELECT count() FROM raw_station_status")
        count_status = result.result_rows[0][0]
        
        # Compter les lignes dans raw_station_info
        result = client.query("SELECT count() FROM raw_station_info")
        count_info = result.result_rows[0][0]
        
        logger.info(f" raw_station_status : {count_status} lignes")
        logger.info(f" raw_station_info : {count_info} lignes")
        
        # Afficher quelques exemples
        if count_status > 0:
            result = client.query(
                "SELECT station_id, num_bikes_available, num_docks_available "
                "FROM raw_station_status "
                "ORDER BY ingestion_timestamp DESC "
                "LIMIT 3"
            )
            logger.info("\n Exemples de données (raw_station_status):")
            for row in result.result_rows:
                logger.info(f"  Station {row[0]}: {row[1]} vélos, {row[2]} bornes")
        
        # Vérifier les vues matérialisées
        result = client.query("SELECT count() FROM fact_station_availability")
        count_fact = result.result_rows[0][0]
        logger.info(f"\n fact_station_availability : {count_fact} lignes")
        logger.info("   (Devrait être ~= raw_station_status grâce à la Materialized View)")
        
        client.close()
        return True
        
    except Exception as e:
        logger.error(f" Erreur ClickHouse: {e}")
        return False

def main():
    """Exécute tous les tests."""
    logger.info("=" * 60)
    logger.info(" TESTS DU CONSOMMATEUR VELIB")
    logger.info("=" * 60)
    
    tests = [
        ("Connexions", test_connections),
        ("Consommation limitée", test_consume_limited),
        ("Vérification ClickHouse", verify_clickhouse_data),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f" Erreur dans {test_name}: {e}")
            results.append((test_name, False))
    
    # Résumé
    logger.info("\n" + "=" * 60)
    logger.info(" RÉSUMÉ DES TESTS")
    logger.info("=" * 60)
    
    for test_name, result in results:
        status = " PASS" if result else "❌ FAIL"
        logger.info(f"{status} - {test_name}")
    
    total_pass = sum(1 for _, result in results if result)
    total_tests = len(results)
    
    logger.info("=" * 60)
    logger.info(f"Résultat : {total_pass}/{total_tests} tests réussis")
    logger.info("=" * 60)
    
    if total_pass == total_tests:
        logger.info(" Tous les tests sont passés !")
        return 0
    else:
        logger.warning(f"  {total_tests - total_pass} test(s) échoué(s)")
        return 1

if __name__ == "__main__":
    sys.exit(main())