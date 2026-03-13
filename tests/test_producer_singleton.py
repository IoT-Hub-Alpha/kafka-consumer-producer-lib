from IoTKafka.producer_singleton import KafkaProducerManager


def teardown_function():
    KafkaProducerManager.close_all()


def test_get_single_producer_returns_same_instance_for_same_config():
    p1 = KafkaProducerManager.get_single_producer(
        bootstrap_servers="kafka:9092", client_id="svc-a"
    )
    p2 = KafkaProducerManager.get_single_producer(
        bootstrap_servers="kafka:9092", client_id="svc-a"
    )
    assert p1 is p2


def test_get_single_producer_returns_different_instances_for_different_configs():
    p1 = KafkaProducerManager.get_single_producer(
        bootstrap_servers="kafka:9092", client_id="svc-a"
    )
    p2 = KafkaProducerManager.get_single_producer(
        bootstrap_servers="kafka:9092", client_id="svc-b"
    )
    assert p1 is not p2


def test_close_all_flushes_and_clears_instances():
    p1 = KafkaProducerManager.get_single_producer(
        bootstrap_servers="kafka:9092", client_id="svc-a"
    )
    p2 = KafkaProducerManager.get_single_producer(
        bootstrap_servers="kafka:9092", client_id="svc-b"
    )
    KafkaProducerManager.close_all()
    assert p1.flush_calls == [None]
    assert p2.flush_calls == [None]
    assert KafkaProducerManager._instances == {}
