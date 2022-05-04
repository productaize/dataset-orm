import json
from uuid import uuid4

import dataset
from dataset.types import Types, JSON, String
from sqlalchemy import LargeBinary
from sqlalchemy.dialects.postgresql import JSONB


class Column:
    db_type = Types.string
    column_kwargs = None

    def __init__(self, db_type=None, name=None, model=None, **column_kwargs):
        self.db_type = db_type if db_type is not None else self.db_type
        self.name = name
        self.column_kwargs = column_kwargs or self.column_kwargs or {}
        typecls = self.type_key(db_type)
        self.to_db = getattr(self, f'to_db_{typecls}', self.to_db)
        self.to_python = getattr(self, f'to_python_{typecls}', self.to_python)
        self.default_value = getattr(self, f'default_{typecls}', self.default_value)
        if model and not name:
            raise ValueError('Must specify name= and model=, or just name=')
        if model:
            self._set_model(model, name)

    def _set_model(self, model, name):
        from dataset_orm import Model
        self.model = model
        self.name = self.name or name
        if not hasattr(model, '_spec'):
            model._spec_init()
        model._spec.columns = model._spec.columns
        model._spec.columns[name] = self
        Model._all_models.add(self.model)

    def __set_name__(self, model, name):
        self._set_model(model, name)

    def __get__(self, obj, objtype=None):
        return obj._values[self.name]

    def __set__(self, obj, value):
        obj._values[self.name] = value

    @property
    def default(self):
        return self.default_value()

    def default_value(self):
        value = self.column_kwargs.get('default')
        return value if not callable(value) else value()

    def default_file(self):
        from dataset_orm.files import DatasetFile
        return DatasetFile.open(uuid4().hex, 'rw')

    def default_integer(self):
        return 0

    def default_float(self):
        return 0.0

    def to_db(self, value):
        return value

    def to_python(self, value):
        return value

    def to_db_json(self, value):
        return json.dumps(value)

    def to_python_json(self, value):
        return json.loads(value) if value is not None else {}

    def to_db_file(self, value):
        return value.name

    def to_python_file(self, value):
        from dataset_orm.files import DatasetFile,  FileLike
        dsfile = DatasetFile.objects.get(filename=value)
        return FileLike(dsfile).open('rw')

    def type_for_database(self, db):
        type_map = self.ColumnTypes._engine_types.get(db.engine.dialect.name, {})
        return type_map.get(self.db_type, self.db_type)

    def type_key(self, db_type):
        key = Column.ColumnTypes._type_key.get(db_type)
        key = key or Column.ColumnTypes._type_key.get(db_type.__class__)
        key = key or 'generic'
        return key

    def __repr__(self):
        return f'Column(db_type={self.db_type}, name={self.name})'

    class ColumnTypes(dataset.types.Types):
        json = JSON
        binary = LargeBinary
        file = String

        # engine.dialect.name => type
        _engine_types = {
            'postgresql': {
                JSON: JSONB,
            }
        }

        # type => canonical name
        # -- used to get the to_db_<key> / to_python_<key> methods
        _type_key = {
            json: 'json',
            binary: 'binary',
            file: 'file',
            dataset.types.Types.string: 'string',
            dataset.types.Types.bigint: 'bigint',
            dataset.types.Types.date: 'date',
            dataset.types.Types.datetime: 'datetime',
            dataset.types.Types.integer: 'integer',
            dataset.types.Types.float: 'float',
            dataset.types.Types.boolean: 'boolean',
            dataset.types.Types.text: 'text',
        }


types = Column.ColumnTypes()
