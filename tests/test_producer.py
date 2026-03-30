"""
Tests unitaires pour le producteur Kafka Vélib'.
Tous les appels externes (API, Kafka) sont mockés.
"""

from unittest.mock import MagicMock, patch

import requests

from velib_kafka.producer import VelibProducer


class TestVelibProducerInit:
    """Tests d'initialisation du producteur."""

    def test_init_default_values(self):
        producer = VelibProducer()
        assert producer.bootstrap_servers == ['localhost:9092']
        assert producer.status_topic == 'velib-station-status'
        assert producer.info_topic == 'velib-station-info'
        assert producer.producer is None

    def test_init_custom_values(self):
        producer = VelibProducer(
            bootstrap_servers=['kafka:29092'],
            status_topic='custom-status',
            info_topic='custom-info'
        )
        assert producer.bootstrap_servers == ['kafka:29092']
        assert producer.status_topic == 'custom-status'
        assert producer.info_topic == 'custom-info'


class TestVelibProducerConnect:
    """Tests de connexion à Kafka."""

    def test_connect_success(self, mock_kafka_producer):
        producer = VelibProducer()
        result = producer.connect()
        assert result is True
        assert producer.producer is not None

    @patch('velib_kafka.producer.KafkaProducer', side_effect=Exception("Connection refused"))
    def test_connect_failure(self, mock_cls):
        producer = VelibProducer()
        result = producer.connect()
        assert result is False


class TestVelibProducerFetchAPI:
    """Tests de récupération des données API."""

    def test_fetch_api_success(self, mock_requests_status):
        producer = VelibProducer()
        result = producer.fetch_api("https://example.com/station_status.json")
        assert result is not None
        assert 'data' in result
        assert len(result['data']['stations']) == 2

    @patch('velib_kafka.producer.requests.get')
    def test_fetch_api_http_error(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        producer = VelibProducer()
        result = producer.fetch_api("https://example.com/station_status.json")
        assert result is None

    @patch('velib_kafka.producer.requests.get')
    def test_fetch_api_no_data_key(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"error": "bad response"}
        mock_get.return_value = mock_response

        producer = VelibProducer()
        result = producer.fetch_api("https://example.com/station_status.json")
        assert result is None

    @patch('velib_kafka.producer.requests.get')
    def test_fetch_api_no_stations_key(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"other": []}}
        mock_get.return_value = mock_response

        producer = VelibProducer()
        result = producer.fetch_api("https://example.com/station_status.json")
        assert result is None

    @patch('velib_kafka.producer.requests.get', side_effect=requests.exceptions.Timeout("Timeout"))
    def test_fetch_api_timeout(self, mock_get):
        producer = VelibProducer()
        result = producer.fetch_api("https://example.com/station_status.json")
        assert result is None


class TestVelibProducerSendToKafka:
    """Tests d'envoi de messages Kafka."""

    def test_send_to_kafka_success(self, mock_kafka_producer):
        producer = VelibProducer()
        producer.connect()

        result = producer.send_to_kafka(
            'velib-station-status',
            '123',
            {'station_id': '123', 'num_bikes_available': 5}
        )
        assert result is True
        mock_kafka_producer.send.assert_called_once()

    def test_send_to_kafka_failure(self, mock_kafka_producer):
        from kafka.errors import KafkaError
        mock_kafka_producer.send.side_effect = KafkaError("Send failed")

        producer = VelibProducer()
        producer.connect()

        result = producer.send_to_kafka(
            'velib-station-status',
            '123',
            {'station_id': '123'}
        )
        assert result is False


class TestVelibProducerProduceStatus:
    """Tests de production des statuts."""

    def test_produce_station_status_success(self, mock_kafka_producer, mock_requests_status):
        producer = VelibProducer()
        producer.connect()

        count = producer.produce_station_status()
        assert count == 2  # 2 stations dans les données de test
        assert mock_kafka_producer.send.call_count == 2
        mock_kafka_producer.flush.assert_called_once()

    @patch('velib_kafka.producer.requests.get')
    def test_produce_station_status_api_error(self, mock_get, mock_kafka_producer):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        producer = VelibProducer()
        producer.connect()

        count = producer.produce_station_status()
        assert count == 0
        mock_kafka_producer.send.assert_not_called()

    def test_produce_station_status_empty(self, mock_kafka_producer):
        with patch('velib_kafka.producer.requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "last_updated": 1700000000,
                "data": {"stations": []}
            }
            mock_get.return_value = mock_response

            producer = VelibProducer()
            producer.connect()

            count = producer.produce_station_status()
            assert count == 0


class TestVelibProducerProduceInfo:
    """Tests de production des informations."""

    def test_produce_station_information_success(self, mock_kafka_producer, mock_requests_info):
        producer = VelibProducer()
        producer.connect()

        count = producer.produce_station_information()
        assert count == 2
        assert mock_kafka_producer.send.call_count == 2
        mock_kafka_producer.flush.assert_called_once()


class TestVelibProducerClose:
    """Tests de fermeture de connexion."""

    def test_close_with_producer(self, mock_kafka_producer):
        producer = VelibProducer()
        producer.connect()
        producer.close()
        mock_kafka_producer.close.assert_called_once()

    def test_close_without_producer(self):
        producer = VelibProducer()
        producer.close()  # Ne doit pas lever d'exception
