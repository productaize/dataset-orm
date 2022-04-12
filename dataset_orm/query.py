class ModelQuery:
    """ ModelQuery is Model.objects used for filtering

    The ModelQuery methods are similar to Table.all(), Table.find(),
    and Table.find_one(), however they return Model instances instead
    of dict() when the iterator is resolved.

    The iterator is a QueryResult. It provides caching and thus can
    be processed multiple times. To get a non-cached version use
    Model.objects.all().nocache().
    """

    def __init__(self, model=None):
        self.model = model

    def __set_name__(self, model, name):
        self.model = model

    def all(self, **kwargs):
        kwargs.setdefault('order_by', self.model._spec.primary_id)
        kwargs.update(**kwargs)
        query = lambda: (self.model._from_db(**row) for row in self.model.table.all(**kwargs)) # noqa
        return QueryResult(query)

    def find(self, *_clauses, **kwargs):
        """ Return all model instances for the given clause """
        self._build_query_clauses(kwargs)
        kwargs.setdefault('order_by', self.model._spec.primary_id)
        kwargs.update(**kwargs)
        query = lambda: (self.model._from_db(**row) for row in self.model.table.find(*_clauses, **kwargs)) # noqa
        return QueryResult(query)

    def find_one(self, *_clauses, **kwargs):
        """ Return a single model instance for the given clause """
        return self.get(*_clauses, **kwargs)

    def get(self, *_clauses, **kwargs):
        """ Return a single model instance for the given clause """
        self._build_query_clauses(kwargs)
        kwargs.setdefault('order_by', self.model._spec.primary_id)
        if 'pk' in kwargs:
            kwargs['id'] = kwargs.pop('pk')
        kwargs.update(**kwargs)
        row = self.model.table.find_one(*_clauses, **kwargs)
        if row is None:
            raise ValueError(f'cannot find row for {_clauses} {kwargs}')
        return self.model._from_db(**row)

    def query(self, statement, *args, placeholder=':t', model=None, **kwargs):
        """ Return model instances from a SQL statement

        In your SQL statement, the table name can be referenced as the
        :t placeholder::

            Model.objects.query('select * from :t')

        If your query returns column names that are different from your
        Model columns, a ResultModel instance is created that has all
        the columns.
        """
        from dataset_orm.model import ResultModel
        # select * from :t
        if isinstance(statement, str):
            tbname = self.model.table_name
            statement = statement.replace(placeholder, tbname)
        result = self.model._db.query(statement, *args, **kwargs)
        model = model or self.model
        column_mismatch = not set(model._spec.columns.keys()) <= set(result.keys)
        if column_mismatch:
            model = model if issubclass(model, ResultModel) else ResultModel
            model.use(self.model._db)
        query = lambda: (model._from_db(**row) for row in result) # noqa
        return QueryResult(query)

    def _build_query_clauses(self, kwargs):
        # enable django-style column__op=value filters
        for k, v in dict(kwargs).items():
            if '__' in k:
                column, op = k.split('__')
                kwargs.pop(k)
                kwargs[column] = {op: v}
            if k == 'pk':
                kwargs.pop(k)
                kwargs[self.model._spec.primary_id] = v
        return kwargs


class QueryResult:
    def __init__(self, query, caching=True):
        self.query = query
        self.caching = caching
        self.resolved = False
        self.cache = []

    def __iter__(self):
        data = self.as_list() if self.caching else self.query()
        for o in data:
            yield o

    def nocache(self):
        return QueryResult(self.query, caching=False)

    def delete(self):
        # possibly too slow for many results, should process in chunks
        for o in self:
            o.delete()

    def as_list(self):
        if self.caching and not self.resolved:
            data = [o for o in self.query()]
            self.cache = data
            self.resolved = True
        elif not self.caching:
            data = [o for o in self.query()]
            self.resolved = True
        else:
            data = self.cache
        return data

    def as_raw(self):
        for m in self:
            yield m.to_dict()

    def first(self):
        allobjs = self.as_list()
        return allobjs[0] if len(allobjs) > 0 else None

    def last(self):
        return self.as_list()[-1]

    def count(self):
        return sum(1 for _ in self)

    def __len__(self):
        return self.count()
