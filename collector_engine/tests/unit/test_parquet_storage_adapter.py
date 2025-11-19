import pyarrow as pa
from pathlib import Path

from collector_engine.app.shell.adapters.storage.parquet_store import ParquetDatasetStore
from collector_engine.app.shell.factories.storage_factory import create_dataset_store


def test_list_names__filters_non_parquet(monkeypatch, tmp_path):
    store = ParquetDatasetStore(tmp_path)
    returned = ["a.parquet", "b.txt", "c.parquet", "README", "d.PARQUET"]

    def fake_get_pq_names(path):
        assert path == tmp_path
        return returned

    monkeypatch.setattr(
        "collector_engine.app.shell.adapters.storage.parquet_store.get_pq_names", fake_get_pq_names
    )

    names = store.list_names()

    assert names == ["a.parquet", "c.parquet"]


def test_create_dataset_store_factory_parquet(tmp_path):
    store = create_dataset_store("parquet", tmp_path)
    assert isinstance(store, ParquetDatasetStore)
    assert store.base_path == Path(tmp_path)


def test_list_names__empty(monkeypatch, tmp_path):
    store = ParquetDatasetStore(tmp_path)

    def fake_get_pq_names(path):
        return []

    monkeypatch.setattr(
        "collector_engine.app.shell.adapters.storage.parquet_store.get_pq_names", fake_get_pq_names
    )

    names = store.list_names()

    assert names == []


def test_read_table__delegates_to_pyarrow(monkeypatch, tmp_path):
    store = ParquetDatasetStore(tmp_path)
    expected = pa.table({"a": [1, 2, 3]})
    file_name = "sample.parquet"

    def fake_read_table(path):
        assert path == tmp_path / file_name
        return expected

    monkeypatch.setattr(
        "collector_engine.app.shell.adapters.storage.parquet_store.pq.read_table", fake_read_table
    )

    table = store.read_table(file_name)

    assert table.equals(expected)


def test_write_buffer__delegates_all_parameters(monkeypatch, tmp_path):
    store = ParquetDatasetStore(tmp_path)
    buffer = {"a": [1, 2]}
    schema = pa.schema([("a", pa.int32())])
    file_name = "out.parquet"
    rows_per_file = 100
    force = True
    returned = {"a": []}

    def fake_write_and_flush_if_needed(*, buffer, schema, pq_path, file_name, rows_per_file, force):
        assert buffer is buffer
        assert schema == schema
        assert pq_path == tmp_path
        assert file_name == file_name
        assert rows_per_file == 100
        assert force is True
        return returned

    monkeypatch.setattr(
        "collector_engine.app.shell.adapters.storage.parquet_store.write_and_flush_if_needed",
        fake_write_and_flush_if_needed,
    )

    result = store.write_buffer(
        buffer=buffer,
        schema=schema,
        file_name=file_name,
        rows_per_file=rows_per_file,
        force=force,
    )

    assert result is returned


def test_write_buffer__force_false(monkeypatch, tmp_path):
    store = ParquetDatasetStore(tmp_path)
    buffer = {"a": [1]}
    schema = pa.schema([("a", pa.int32())])
    file_name = "out.parquet"

    def fake_write_and_flush_if_needed(*, buffer, schema, pq_path, file_name, rows_per_file, force):
        assert force is False
        return buffer

    monkeypatch.setattr(
        "collector_engine.app.shell.adapters.storage.parquet_store.write_and_flush_if_needed",
        fake_write_and_flush_if_needed,
    )

    result = store.write_buffer(
        buffer=buffer,
        schema=schema,
        file_name=file_name,
        rows_per_file=10,
        force=False,
    )

    assert result is buffer
