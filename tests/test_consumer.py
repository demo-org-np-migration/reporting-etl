"""Tests de etl/consumer.process_message: no debe caerse por un mensaje malo."""

from etl.consumer import process_message


class FakeMsg:
    def __init__(self, value: bytes, offset: int = 0):
        self._value = value
        self._offset = offset

    def value(self):
        return self._value

    def offset(self):
        return self._offset


class _FakeCounter:
    def __init__(self):
        self.value = 0

    def inc(self):
        self.value += 1


class FakeMetrics:
    def __init__(self):
        self.events_processed_total = _FakeCounter()
        self.events_duplicate_total = _FakeCounter()

    @property
    def processed(self):
        return self.events_processed_total.value

    @property
    def duplicate(self):
        return self.events_duplicate_total.value


class FailingLoader:
    """Simula Postgres caído: cualquier load_event revienta."""

    def load_event(self, event):
        raise RuntimeError("connection refused")


class OkLoader:
    def __init__(self):
        self.events = []

    def load_event(self, event):
        self.events.append(event)
        return True


def test_mensaje_con_json_invalido_no_explota_y_se_commitea():
    msg = FakeMsg(b"esto no es json{", offset=7)
    metrics = FakeMetrics()

    should_commit = process_message(msg, OkLoader(), metrics)

    assert should_commit is True
    assert metrics.processed == 0
    assert metrics.duplicate == 0


def test_error_de_db_no_explota_y_no_commitea():
    msg = FakeMsg(b'{"payment_id": "x"}', offset=3)
    metrics = FakeMetrics()

    should_commit = process_message(msg, FailingLoader(), metrics)

    assert should_commit is False
    assert metrics.processed == 0


def test_mensaje_valido_carga_y_commitea():
    msg = FakeMsg(b'{"payment_id": "x"}', offset=1)
    metrics = FakeMetrics()
    loader = OkLoader()

    should_commit = process_message(msg, loader, metrics)

    assert should_commit is True
    assert metrics.processed == 1
    assert loader.events == [{"payment_id": "x"}]
