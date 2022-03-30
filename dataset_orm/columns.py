import json

from dataset.types import Types
from sqlalchemy import LargeBinary


class Column:
    db_type = Types.string
    column_kwargs = None

    def __init__(self, name=None, db_type=None, **column_kwargs):
        self.name = name
        self.db_type = db_type if db_type is not None else self.db_type
        self.column_kwargs = column_kwargs or self.column_kwargs or {}

    def __set_name__(self, model, name):
        self.model = model
        model._all_models.add(model)
        self.name = self.name or name
        model.columns = model.columns or {}
        model.columns[name] = self

    def __get__(self, obj, objtype=None):
        return obj._values[self.name]

    def __set__(self, obj, value):
        obj._values[self.name] = value

    def to_db(self, value):
        return value

    def to_python(self, value):
        return value


class JSONColumn(Column):
    db_type = Types.JSON

    def to_db(self, value):
        return json.dumps(value)

    def to_python(self, value):
        return json.loads(value) if value is not None else {}


class TextColumn(Column):
    db_type = Types.string


class IntegerColumn(Column):
    db_type = Types.bigint


class FloatColumn(Column):
    db_type = Types.float


class BlobColumn(Column):
    db_type = LargeBinary


class DateColumn(Column):
    db_type = Types.date


class DatetimeColumn(Column):
    db_type = Types.datetime
