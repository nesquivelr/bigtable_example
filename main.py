import datetime
import os
import struct
from ast import literal_eval

import bson
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable.row_set import RowSet


def create_prefix_row_set(prefix: str | bytes) -> RowSet:
    """Creates a RowSet that search all keys starting with prefix"""
    row_set = RowSet()
    row_set.add_row_range_with_prefix(f"{prefix}#")
    return row_set


def encode_float(value: float) -> bytes:
    """Transform a float into bytes using big endian notation"""
    return struct.pack(">d", value)


def decode_float(value: bytes) -> float:
    """Returns the first item since unpack returns a tuple like (value,)"""
    return struct.unpack(">d", value)[0]


def encode_datetime(value: datetime.datetime) -> bytes:
    """Transform a datetime timestamp value which is a float into bytes"""
    return encode_float(value.replace(tzinfo=datetime.timezone.utc).timestamp())


def decode_datetime(value: bytes) -> datetime.datetime:
    """Returns a datetime after retrieving the timestamp from the bytes"""
    return datetime.datetime.utcfromtimestamp(decode_float(value))


def encode_boolean(value: bool) -> bytes:
    """Transform a boolean to bytes"""
    return struct.pack(">?", value)


def decode_boolean(value: bytes) -> bool:
    """Returns a boolean created from bytes"""
    return struct.unpack(">?", value)[0]


def encode_int(value: int) -> bytes:
    """Transform an integer to bytes"""
    return struct.pack(">q", value)


def decode_int(value: bytes) -> int:
    """Returns an integer created from bytes"""
    return struct.unpack(">q", value)[0]


def main():
    os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8086"
    project_id = "some_random_project_id"
    instance_id = "some_random_instance_id"

    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)

    table_id = "some_random_table_id"
    print("Creating the {} table.".format(table_id))
    table = instance.table(table_id)

    print("Creating column family cf1 with Max Version GC rule...")
    # If we search with a column family that doesn't exist, the program get stuck
    column_families = {"W": column_family.MaxVersionsGCRule(1)}
    if not table.exists():
        table.create(column_families=column_families)
    else:
        table.truncate()
        print("Table {} already exists.".format(table_id))

    print("Adding row_keys")
    row = table.direct_row(b"123#1")
    row.set_cell("W", b"col_bool", encode_boolean(False))  # Rows must have at least one column
    row.commit()
    row = table.direct_row(b"123#2")
    row.set_cell("W", b"col_bool", encode_boolean(True))
    row.set_cell("W", b"col_int", encode_int(1))
    row.set_cell("W", b"col_str", "str".encode())
    row.set_cell("W", b"col_float", encode_float(1.0))
    row.set_cell("W", b"col_dict", bson.dumps({"a": "b"}))
    row.set_cell("W", b"col_list", str(["a", "b"]).encode())
    row.set_cell("W", b"col_timestamp", encode_datetime(datetime.datetime(2022, 1, 1)))
    row.commit()
    row = table.direct_row(b"124#1")
    row.set_cell("W", b"col_bool", encode_boolean(False))  # Rows must have at least one column
    row.commit()

    rows = table.read_rows()
    rows = [row for row in rows]
    assert len(rows) == 3
    rows = table.read_rows(row_set=create_prefix_row_set("123"))
    row_keys = [row.row_key.decode() for row in rows]
    assert row_keys == ["123#1", "123#2"]
    row = table.read_row(b"124#1")
    assert row is not None
    row = table.read_row(b"125#1")
    assert row is None
    row = table.read_row(b"123#2")
    assert decode_boolean(row.cell_value("W", b"col_bool")) is True
    assert decode_int(row.cell_value("W", b"col_int")) == 1
    assert row.cell_value("W", b"col_str").decode() == "str"
    assert decode_float(row.cell_value("W", b"col_float")) == 1.0
    assert bson.loads(row.cell_value("W", b"col_dict")) == {"a": "b"}
    assert literal_eval(row.cell_value("W", b"col_list").decode()) == ["a", "b"]
    assert decode_datetime(row.cell_value("W", b"col_timestamp")) == datetime.datetime(2022, 1, 1)


if __name__ == "__main__":
    main()
