"""
Tests unitaires pour le consommateur Kafka Vélib'.
Tous les appels externes (Kafka, ClickHouse) sont mockés.
"""

import json
from unittest.mock import patch

from velib_kafka.consumer import VelibConsumer


class TestVelibConsumerInit:
    """Tests d'initialisation du consommateur."""

    def test_init_default_values(self):
        consumer = VelibConsumer()
        assert consumer.bootstrap_servers == ['localhost:9092']
        assert consumer.consumer_group == 'velib-consumer-group'
        assert consumer.batch_size == 100
        assert consumer.clickhouse_host == 'localhost'
        assert consumer.clickhouse_port == 8123
        assert consumer.consumer is None
        assert consumer.clickhouse_client is None
        assert consumer.status_buffer == []
        assert consumer.info_buffer == []
        assert consumer.stats['messages_consumed'] == 0

    def test_init_custom_values(self):
        consumer = VelibConsumer(
            bootstrap_servers=['kafka:29092'],
            consumer_group='test-group',
            clickhouse_host='clickhouse',
            clickhouse_port=9000,
            batch_size=50
        )
        assert consumer.bootstrap_servers == ['kafka:29092']
        assert consumer.consumer_group == 'test-group'
        assert consumer.batch_size == 50


class TestVelibConsumerConnect:
    """Tests de connexion."""

    def test_connect_kafka_success(self, mock_kafka_consumer):
        consumer = VelibConsumer()
        result = consumer.connect_kafka()
        assert result is True
        assert consumer.consumer is not None

    @patch('velib_kafka.consumer.KafkaConsumer', side_effect=Exception("Connection refused"))
    def test_connect_kafka_failure(self, mock_cls):
        consumer = VelibConsumer()
        result = consumer.connect_kafka()
        assert result is False

    def test_connect_clickhouse_success(self, mock_clickhouse_client):
        consumer = VelibConsumer()
        result = consumer.connect_clickhouse()
        assert result is True
        assert consumer.clickhouse_client is not None

    @patch('velib_kafka.consumer.clickhouse_connect')
    def test_connect_clickhouse_failure(self, mock_ch):
        mock_ch.get_client.side_effect = Exception("Connection refused")
        consumer = VelibConsumer()
        result = consumer.connect_clickhouse()
        assert result is False


class TestVelibConsumerTransform:
    """Tests de transformation des messages."""

    def test_transform_status_message_with_list(self):
        consumer = VelibConsumer()
        message = {
            'station_id': '123',
            'num_bikes_available_types': [{"mechanical": 3}, {"ebike": 2}]
        }
        result = consumer.transform_status_message(message)
        assert isinstance(result['num_bikes_available_types'], str)
        parsed = json.loads(result['num_bikes_available_types'])
        assert len(parsed) == 2

    def test_transform_status_message_already_string(self):
        consumer = VelibConsumer()
        message = {
            'station_id': '123',
            'num_bikes_available_types': '[{"mechanical": 3}]'
        }
        result = consumer.transform_status_message(message)
        assert result['num_bikes_available_types'] == '[{"mechanical": 3}]'

    def test_transform_info_message_with_list(self):
        consumer = VelibConsumer()
        message = {
            'station_id': '123',
            'rental_methods': ['CREDITCARD', 'KEY']
        }
        result = consumer.transform_info_message(message)
        assert isinstance(result['rental_methods'], str)

    def test_transform_info_message_already_string(self):
        consumer = VelibConsumer()
        message = {
            'station_id': '123',
            'rental_methods': '["CREDITCARD"]'
        }
        result = consumer.transform_info_message(message)
        assert result['rental_methods'] == '["CREDITCARD"]'


class TestVelibConsumerInsertBatch:
    """Tests d'insertion en batch."""

    def test_insert_status_batch_empty(self):
        consumer = VelibConsumer()
        result = consumer.insert_status_batch()
        assert result is True

    def test_insert_status_batch_success(self, mock_clickhouse_client):
        consumer = VelibConsumer()
        consumer.connect_clickhouse()

        consumer.status_buffer = [
            {
                'station_id': '123',
                'num_bikes_available': 5,
                'num_bikes_available_types': '[]',
                'num_docks_available': 15,
                'is_installed': 1,
                'is_returning': 1,
                'is_renting': 1,
                'last_reported': 1700000000,
                'ingestion_timestamp': 1700000000,
                'api_last_updated': 1700000000
            }
        ]

        result = consumer.insert_status_batch()
        assert result is True
        assert consumer.status_buffer == []
        assert consumer.stats['messages_inserted'] == 1
        assert consumer.stats['batches_inserted'] == 1
        mock_clickhouse_client.insert.assert_called_once()

    def test_insert_status_batch_error(self, mock_clickhouse_client):
        mock_clickhouse_client.insert.side_effect = Exception("Insert failed")

        consumer = VelibConsumer()
        consumer.connect_clickhouse()
        consumer.status_buffer = [{'station_id': '123'}]

        result = consumer.insert_status_batch()
        assert result is False
        assert consumer.stats['errors'] == 1

    def test_insert_info_batch_empty(self):
        consumer = VelibConsumer()
        result = consumer.insert_info_batch()
        assert result is True

    def test_insert_info_batch_success(self, mock_clickhouse_client):
        consumer = VelibConsumer()
        consumer.connect_clickhouse()

        consumer.info_buffer = [
            {
                'station_id': '123',
                'stationCode': '12345',
                'name': 'Station Test',
                'lat': 48.8566,
                'lon': 2.3522,
                'capacity': 20,
                'rental_methods': '["CREDITCARD"]',
                'ingestion_timestamp': 1700000000,
                'api_last_updated': 1700000000
            }
        ]

        result = consumer.insert_info_batch()
        assert result is True
        assert consumer.info_buffer == []
        assert consumer.stats['messages_inserted'] == 1


class TestVelibConsumerShouldFlush:
    """Tests de la logique de flush."""

    def test_should_flush_buffer_full(self):
        consumer = VelibConsumer(batch_size=2)
        consumer.status_buffer = [{'a': 1}, {'b': 2}]
        assert consumer.should_flush() is True

    def test_should_flush_buffer_empty(self):
        consumer = VelibConsumer()
        assert consumer.should_flush() is False

    def test_should_flush_timeout(self):
        consumer = VelibConsumer()
        consumer.status_buffer = [{'a': 1}]
        consumer.last_insert_time = 0  # Très ancien → timeout dépassé
        assert consumer.should_flush() is True


class TestVelibConsumerClose:
    """Tests de fermeture."""

    def test_close_with_connections(self, mock_kafka_consumer, mock_clickhouse_client):
        consumer = VelibConsumer()
        consumer.connect_kafka()
        consumer.connect_clickhouse()
        consumer.close()
        mock_kafka_consumer.close.assert_called_once()
        mock_clickhouse_client.close.assert_called_once()

    def test_close_without_connections(self):
        consumer = VelibConsumer()
        consumer.close()  # Ne doit pas lever d'exception
