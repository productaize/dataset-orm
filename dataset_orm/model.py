import re

import dataset
from dataset import Table
from sqlalchemy.util import classproperty

from dataset_orm import Column
from dataset_orm.query import ModelQuery


class TableSpec:
    """ TableSpec represents the required Table definition

    It should not be necessary to work directly with TableSpec, unless
    for a few cases:

    * different primary key or type (other than 'id', Integer)

        class Customer(Model):
            _spec = TableSpec(primary_id='username',
            primary_type=types.string)

    * different table name::

        class Customer(Model):
            _spec = TableSpec(primary_id='username',
            primary_type=types.string, table_name='intl_customers')


        Note it may be user to use Model.from_table::

            Customer = Model.from_table(db['intl_customers'])


    Why do we need TableSpec

    Essentially, it wraps boilerplate code like this:

        def create_tables(db):
            table = table['customer']
            table.create_column('name', types.string)
            table.create_column('place', types.string)

        db = dataset.connect('sqlite://...')
        create_tables(db)

    Instead we can write:

        from dataset_orm import Model, Column, connect

        # models.py (or anywhere)
        class Customer(Model):
            name = Column(types.string)
            place = Column(types.string)

        # app.py (at startup)
        connect('sqlite://....')

    The TableSpec remembers all Column() statements and upon
    connecting to a database, it creates the dataset.Table and
    all columns.
    """
    model = None
    columns = None
    primary_id = dataset.Table.PRIMARY_DEFAULT
    primary_type = dataset.types.Types.integer
    primary_increment = None  # use the Table default, depending on primary_type
    table_name = None
    table_obj = None
    auto_create = True

    def __init__(self, table_name=None, primary_id=None, primary_type=None,
                 primary_increment=None, auto_create=None):
        ifnotnone = lambda v, other: v if v is not None else other  # noqa
        self.table_name = ifnotnone(table_name, self.table_name)
        self.primary_id = ifnotnone(primary_id, self.primary_id)
        self.primary_type = ifnotnone(primary_type, self.primary_type)
        self.primary_increment = ifnotnone(primary_increment, self.primary_increment)
        self.auto_create = ifnotnone(auto_create, self.auto_create)
        self.columns = dict()

    def __set_name__(self, model, name):
        camel2snake = lambda v: re.sub(r'(?<!^)(?=[A-Z])', '_', v).lower()  # noqa
        self.model = model
        if not hasattr(model, '_spec'):
            model._spec_init()
        self.table_name = camel2snake(self.table_name or model.__name__)
        Model._all_models.add(self.model)

    def create_table(self):
        db = self.model._db
        if self.model and self.table_obj is None:
            self.table_obj = Table(self.model._db, self.table_name,
                                   primary_id=self.primary_id,
                                   primary_type=self.primary_type,
                                   primary_increment=self.primary_increment,
                                   auto_create=self.auto_create)
        table = self.table_obj
        if self.primary_id not in self.columns:
            self.columns[self.primary_id] = Column(self.primary_type,
                                                   self.primary_id, unique=True)
        for colname, column in self.columns.items():
            table.create_column(colname, column.type_for_database(db),
                                **column.column_kwargs)
        table._sync_columns = lambda row, ensure, types=None: row
        return self

    def drop_table(self):
        table = self.table_obj or self.model._db[self.table_name]
        table.drop()
        self.table_obj = None

    def sync_table(self, table, model):
        self.table_obj = table
        self.primary_id = table._primary_id
        self.primary_type = table._primary_type
        self.primary_increment = table._primary_increment
        table.find_one(order_by=self.primary_id)
        for column in table._table.columns:
            self.columns[column.name] = Column(column.type,
                                               name=column.name,
                                               model=model)


class Model:
    """ Model is the object representation of a single row in a dataset.Table

    Usage::

        from dataset_orm import Model, Column, types, connect

        # define the model
        class Customer(Model)
            firstname = Column(types.string)
            lastname = Column(types.string)
            preferences = Column(types.json)

        # connect to the database and link all models
        # -- this automatically creates and updates the tables
        db = connect('sqlite:///db.sqlite')

        # query existing table
        customers = Customer.objects.find(city='London')
        for cus in customers:
            print(cus.firstname, cus.lastname)

        # create new records, or update existing
        Customer(firstname='John', lastname='Walker').save()
        customer = Customer.objects.find_one(pk=459)
        customer['lastname'] = 'Walker-Meyers'
        customer.save()

        # work with dict data and store as a JSON field
        customer.preferences = { 'sports': ['basketball'] }
        customer.save()
        customer.refresh() # reload from database
        customer.preferences
        =>
        { 'sports': ['basketball'] }

        # delete single objects, or matching rows
        # -- all at once
        customers = Customer.objects.find(city='Fakeplace')
        customers.delete()
        # -- single objects
        customer = Customer.objects.find_one(city='Fakeplace')
        customer.delete()

        # if you already have a db object from dataset, link models like this
        db = dataset.connect('sqlite:///db.sqlite')
        dataset_orm.setup_models(db)

        # if you already have existing tables, create Model objects
        Customer = Model.from_table(db['customer_table'])

        # if you don't like classes
        Customer = Model.from_spec(name='Customer',
                                   columns=[Column(types.string, 'name')],
                                   db=db)
    """
    _all_models = set()
    _spec_cls = TableSpec
    _objects_cls = ModelQuery

    def __init__(self, **values):
        columns = self.columns
        self.__dict__['_values'] = {
            k: v for k, v in values.items()
            if k in columns or k == self._spec.primary_id
        }
        for k, col in columns.items():
            if k == self._spec.primary_id:
                continue
            self.__dict__['_values'].setdefault(k, col.default)

    @classmethod
    def from_table(cls, table):
        model = type(table.name, (Model,), dict())
        model.use(table.db)
        model._spec.sync_table(table, model)
        return model

    @classmethod
    def from_spec(cls, name, columns, db):
        table = db[name]
        for column in columns:
            table.create_column(column.name, column.db_type)
        return cls.from_table(table)

    @classmethod
    def _spec_init(cls):
        # should be in a metaclass, kept here for simplicity
        if not hasattr(cls, '_spec'):
            setattr(cls, '_db', None)
            setattr(cls, '_spec', cls._spec_cls())
            setattr(cls, 'objects', cls._objects_cls())
            cls._spec.__set_name__(cls, '_spec')
            cls.objects.__set_name__(cls, 'objects')
        cls._spec.columns = cls._spec.columns
        cls._spec.table_obj = None

    # user api
    @classmethod
    def use(cls, db, recreate=False):
        """ links the Model with a dataset.Table """
        cls._spec_init()
        cls._db = db
        if recreate:
            cls._spec.drop_table()
        if cls._spec.auto_create:
            cls._spec.create_table()

    @classproperty
    def objects(cls):
        """ Return a ModelQuery

        Usage::

            customers = Customer.objects.all()
            customers = Customer.objects.find(city='London')
            customers = Customer.objects.find(city__like='London%')

            for cus in customers:
                ...
        """
        return ModelQuery(cls)

    @classproperty
    def table(cls):
        """ Returns the corresponding dataset.Table """
        return cls._spec.table_obj

    @classproperty
    def table_name(cls):
        """ The table name as used to create the dataset.Table """
        return cls._spec.table_name

    @classproperty
    def columns(cls):
        """ The table columns as used to create the dataset.Table """
        return cls._spec.columns

    @classmethod
    def save_many(cls, models, chunk_size=1000):
        """ Save many models from a sequence. This will not update the models with pks """
        # if the primary key is autoincrement, we drop it bc the db will assign a value
        to_drop = [cls._spec.primary_id] if cls.table._primary_increment else None
        rows = (m._to_db(drop=to_drop) for m in models)
        cls.table.insert_many(list(rows), chunk_size=chunk_size)

    @property
    def pk(self):
        """ return the primary key """
        return self.__dict__['_values'].get(self._spec.primary_id)

    def save(self):
        """ save the model instance """
        if self.pk is None:
            # if the primary key is autoincrement, we drop it bc the db will assign a value
            to_drop = [self._spec.primary_id] if self.table._primary_increment else None
            pk = self.table.insert(self._to_db(drop=to_drop))
            # table.insert returns True or the actual primary key value
            if not isinstance(pk, bool):
                setattr(self, self._spec.primary_id, pk)
        else:
            self.table.upsert(self._to_db(), [self._spec.primary_id])
        return self

    def refresh(self):
        """ re-read by calling Table.find_one() and update the instance """
        self._values.update(self._to_python(self.table.find_one(**{self._spec.primary_id: self.pk,
                                                                   'order_by': self._spec.primary_id})))
        return self

    def delete(self):
        return self.table.delete(**{self._spec.primary_id: self.pk})

    def to_dict(self):
        """ return a dict of all column values """
        return self._values

    # internal api
    @classmethod
    def _from_db(cls, **values):
        return cls(**{k: f.to_python(values[k]) for k, f in cls._spec.columns.items()})

    @property
    def _values(self):
        return self.__dict__.setdefault('_values', {})

    def _to_db(self, drop=None):
        drop = drop or []
        return {k: f.to_db(self._values.get(k)) for k, f in self._spec.columns.items() if k not in drop}

    def _to_python(self, values):
        return {k: f.to_python(values[k]) for k, f in self._spec.columns.items()}

    # column values as attributes
    def __getattr__(self, k):
        k = k.replace('pk', self.__class__._spec.primary_id)
        return self._values[k] if k in self._values else self.__dict__[k]

    def __setattr__(self, k, v):
        if k in self._spec.columns:
            self._values[k.replace('pk', self._spec.primary_id)] = v
        else:
            self.__dict__[k] = v

    def __repr__(self):
        return f'<{self.__class__.__name__}(pk={self.pk})>'


class ResultModel(Model):
    """ Results of Model.objects.query() with columns different from
    original Model """
    _spec = TableSpec(auto_create=False, table_name=None)

    @classmethod
    def use(cls, db, **kwargs):
        cls._db = db
        cls._spec.columns = cls._spec.columns

    @classmethod
    def _from_db(cls, **values):
        instance = cls()
        instance._spec.columns = dict(cls._spec.columns)
        # add dummy columns
        for k, v in values.items():
            if k not in instance._spec.columns:
                instance._spec.columns[k] = Column(name=k)
            f = instance._spec.columns[k]
            setattr(instance, k, f.to_python(v))
        return instance
