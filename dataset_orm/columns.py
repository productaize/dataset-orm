import json
from uuid import uuid4

import dataset
from dataset.types import Types, JSON, String
from sqlalchemy import LargeBinary
from sqlalchemy.dialects.postgresql import JSONB


class Column:
    db_type = Types.string
    column_kwargs = None

    def __init__(self, db_type=None, name=None, model=None, on_update=None, **column_kwargs):
        """
        default=value or callable
        on_update=value or callable
        """
        self.db_type = db_type if db_type is not None else self.db_type
        self.name = name
        self.column_kwargs = column_kwargs or self.column_kwargs or {}
        self.orm_column_kwargs = {
            'on_update': on_update,
        }
        typecls = self.type_key(db_type)
        self.to_db = getattr(self, f'to_db_{typecls}', self.to_db)
        self.to_python = getattr(self, f'to_python_{typecls}', self.to_python)
        self.default_value = getattr(self, f'default_{typecls}', self.default_value)
        self.on_update = getattr(self, f'on_update_{typecls}', self.on_update)
        if model and not name:
            raise ValueError('Must specify name= and model=, or just name=')
        if model:
            self._set_model(model, name)

    def _set_model(self, model, name):
        from dataset_orm import Model
        self.model = model
        self.name = self.name or name
        if getattr(model, '_spec', None) is None:
            model._spec_init()
        model._spec.columns = model._spec.columns
        model._spec.columns[name] = self
        Model._all_models.add(self.model)

    def __set_name__(self, model, name):
        self._set_model(model, name)

    def __get__(self, obj, objtype=None):
        return obj._values.get(self.name)

    def __set__(self, obj, value):
        obj._values[self.name] = value

    @property
    def default(self):
        return self.default_value()

    def default_value(self):
        value = self.column_kwargs.get('default')
        return value if not callable(value) else value()

    def default_file(self):
        return self.to_python_file(uuid4().hex, mode='w')

    def default_integer(self):
        return self.column_kwargs.get('default', 0)

    def default_float(self):
        return self.column_kwargs.get('default', 0.0)

    def default_json(self):
        return dict(self.column_kwargs.get('default', {}))

    def to_db(self, value):
        return value

    def to_python(self, value):
        return value

    def to_db_json(self, value):
        from dataset_orm.util import SpecialEncoder
        return json.dumps(value, cls=SpecialEncoder)

    def to_python_json(self, value):
        from dataset_orm.util import SpecialDecoder
        return json.loads(value, cls=SpecialDecoder) if value is not None else {}

    def to_db_file(self, value):
        return value.name

    def to_python_file(self, value, mode='rw'):
        from dataset_orm.files import FileLike
        dsfile = self._DatasetFile(filename=value)
        return FileLike(dsfile, mode=mode)

    def type_for_database(self, db):
        type_map = self.ColumnTypes._engine_types.get(db.engine.dialect.name, {})
        if self.db_type is self.ColumnTypes.file:
            # resolve localized model
            # -- any db interactions must be done at time of db binding (here)
            #    because the to_python() and to_db() must be free of db side-effects.
            #    If not adhere to this rule, ModelQueries can fail due to the cursor being closed pre-maturely
            self._DatasetFile_model = None
            _ = self._DatasetFile
        return type_map.get(self.db_type, self.db_type)

    def type_key(self, db_type):
        key = Column.ColumnTypes._type_key.get(db_type)
        key = key or Column.ColumnTypes._type_key.get(db_type.__class__)
        key = key or 'generic'
        return key

    def on_update(self, value):
        updater = self.orm_column_kwargs.get('on_update')
        return updater() if callable(updater) else value

    def __repr__(self):
        return f'Column(db_type={self.db_type}, name={self.name})'

    @property
    def _DatasetFile(self):
        if getattr(self, '_DatasetFile_model', None) is None:
            from dataset_orm.files import DatasetFile
            self._DatasetFile_model = DatasetFile._make_using(self.model._db)
        return self._DatasetFile_model

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
