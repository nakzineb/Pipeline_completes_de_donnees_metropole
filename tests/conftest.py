"""
Fixtures partagées pour les tests.
Tous les mocks sont définis ici pour éviter toute dépendance à Kafka ou ClickHouse.
"""

from unittest.mock import MagicMock, patch

import pytest

# =================================================================
# DONNÉES DE TEST
# =================================================================

SAMPLE_API_STATUS_RESPONSE = {
    "last_updated": 1700000000,
    "data": {
        "stations": [
            {
                "station_id": "123",
                "num_bikes_available": 5,
                "num_bikes_available_types": [
                    {"mechanical": 3},
                    {"ebike": 2}
                ],
                "num_docks_available": 15,
                "is_installed": 1,
                "is_returning": 1,
                "is_renting": 1,
                "last_reported": 1700000000
            },
            {
                "station_id": "456",
                "num_bikes_available": 0,
                "num_bikes_available_types": [
                    {"mechanical": 0},
                    {"ebike": 0}
                ],
                "num_docks_available": 20,
                "is_installed": 1,
                "is_returning": 1,
                "is_renting": 0,
                "last_reported": 1700000000
            }
        ]
    }
}

SAMPLE_API_INFO_RESPONSE = {
    "last_updated": 1700000000,
    "data": {
        "stations": [
            {
                "station_id": "123",
                "name": "Station Test 1",
                "lat": 48.8566,
                "lon": 2.3522,
                "capacity": 20,
                "stationCode": "12345",
                "rental_methods": ["CREDITCARD"]
            },
            {
                "station_id": "456",
                "name": "Station Test 2",
                "lat": 48.8600,
                "lon": 2.3400,
                "capacity": 30,
                "stationCode": "67890",
                "rental_methods": ["CREDITCARD", "KEY"]
            }
        ]
    }
}

SAMPLE_EMPTY_RESPONSE = {
    "last_updated": 1700000000,
    "data": {
        "stations": []
    }
}

SAMPLE_INVALID_RESPONSE = {
    "error": "something went wrong"
}


# =================================================================
# FIXTURES
# =================================================================

@pytest.fixture
def sample_status_response():
    """Réponse API station_status valide."""
    return SAMPLE_API_STATUS_RESPONSE


@pytest.fixture
def sample_info_response():
    """Réponse API station_information valide."""
    return SAMPLE_API_INFO_RESPONSE


@pytest.fixture
def sample_empty_response():
    """Réponse API avec 0 stations."""
    return SAMPLE_EMPTY_RESPONSE


@pytest.fixture
def mock_kafka_producer():
    """Mock du KafkaProducer."""
    with patch('velib_kafka.producer.KafkaProducer') as mock_cls:
        mock_instance = MagicMock()
        # Simuler un envoi réussi
        future = MagicMock()
        future.get.return_value = MagicMock(
            topic='velib-station-status',
            partition=0,
            offset=42
        )
        mock_instance.send.return_value = future
        mock_cls.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_kafka_consumer():
    """Mock du KafkaConsumer."""
    with patch('velib_kafka.consumer.KafkaConsumer') as mock_cls:
        mock_instance = MagicMock()
        mock_instance.subscription.return_value = {'velib-station-status', 'velib-station-info'}
        mock_cls.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_clickhouse_client():
    """Mock du client ClickHouse."""
    with patch('velib_kafka.consumer.clickhouse_connect') as mock_module:
        mock_client = MagicMock()
        mock_client.query.return_value = MagicMock()
        mock_client.insert.return_value = None
        mock_module.get_client.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_requests_status(sample_status_response):
    """Mock requests.get pour station_status."""
    with patch('velib_kafka.producer.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_status_response
        mock_get.return_value = mock_response
        yield mock_get


@pytest.fixture
def mock_requests_info(sample_info_response):
    """Mock requests.get pour station_information."""
    with patch('velib_kafka.producer.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_info_response
        mock_get.return_value = mock_response
        yield mock_get
