import contextlib
import json
import os
from base64 import b64encode, b64decode
from datetime import datetime
from json import JSONDecoder

import dataset

DB_CONNECTIONS = {}
DB_CONTEXTS = {}


def connect(url=None, recreate=False, alias=None, **kwargs):
    # return the connection for the given alias, or the same URL
    alias = alias or 'default'
    # replicate default URL handling from dataset.connect
    if url is None:
        url = os.environ.get("DATABASE_URL", "sqlite://")
    # auto connect all registered models
    db = DB_CONNECTIONS.get(alias) or DB_CONNECTIONS.get(url)
    if db is None or db.engine is None:
        if url.startswith('sqlite://'):
            # enable multi threaded sqlite
            # https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#using-a-memory-database-in-multiple-threads
            # https://docs.sqlalchemy.org/en/14/core/pooling.html#connection-pool-configuration
            from sqlalchemy.pool import StaticPool, NullPool
            pool = StaticPool if ':memory:' in url else NullPool
            kwargs.setdefault('engine_kwargs', dict(connect_args=dict(check_same_thread=False),
                                                    poolclass=pool, pool_pre_ping=True))
            kwargs.setdefault('sqlite_wal_mode', False)
        db = dataset.connect(url=url, **kwargs)
        DB_CONNECTIONS[url] = db
    if alias not in DB_CONNECTIONS:
        DB_CONNECTIONS[alias] = db
    if alias == 'default':
        setup_models(db, recreate=recreate)
    else:
        with using(alias, recreate=recreate):
            pass
    return db


def setup_models(db, recreate=False):
    from dataset_orm import Model

    for m in list(Model._all_models):
        m.use(db, recreate=recreate)


@contextlib.contextmanager
def using(alias=None, recreate=False, models=None):
    from dataset_orm import Model
    _using_models = models or list(Model._all_models)
    context = DB_CONTEXTS.get(alias)
    if context is None:
        db = DB_CONNECTIONS[alias]
        # create new models
        context = DBContext({
            m.__name__: m._make_using(db, recreate=recreate)
            for m in _using_models
        })
        DB_CONTEXTS[alias] = context
    yield context


class SpecialEncoder(json.JSONEncoder):
    # TODO check if we can use ujson, orjson etc.
    """ Special json encoder for some datatypes """

    def default(self, obj):  # noqa
        try:
            import numpy as np
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
        except Exception:
            pass
        try:
            import pandas as pd
            if isinstance(obj, pd.DataFrame):
                return obj.to_dict(orient='records')
            elif isinstance(obj, pd.Series):
                return obj.tolist()
        except Exception:
            pass
        if isinstance(obj, datetime):
            return {'_dt_': obj.isoformat()}
        elif isinstance(obj, bytes):
            return {'_bytes_': b64encode(obj).decode('utf8')}
        elif isinstance(obj, (range, set)):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


class SpecialDecoder(JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, object_hook=self.decode_dict, **kwargs)

    def decode_dict(self, d):
        for k, v in d.items():
            if isinstance(v, dict) and '_dt_' in v:
                d[k] = datetime.fromisoformat(v.get('_dt_'))
            elif isinstance(v, dict) and '_bytes_' in v:
                d[k] = b64decode(v.get('_bytes_'))
            elif isinstance(v, dict):
                d[k] = self.decode_dict(v)
        return d


class DBContext(dict):
    def __init__(self, *args, **kwargs):
        super(DBContext, self).__init__(*args, **kwargs)
        self.__dict__ = self
