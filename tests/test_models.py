import os
from datetime import datetime, date
from unittest import TestCase

import sqlalchemy

from dataset_orm import Column, types
from dataset_orm.model import ResultModel, Model
from dataset_orm.util import connect
from tests.examples import User, QueryResultModel, City


class DatasetOrmTests(TestCase):
    def setUp(self):
        db_url = os.environ.get('TEST_DATABASE_URL', 'sqlite:///testdb.sqlite3')
        self.db = connect(db_url, recreate=True)

    def test_model_create_retrieve(self):
        user = User(username='john').save()
        self.assertEqual(user.pk, 1)
        user.refresh()
        self.assertEqual(user.username, 'john')
        user2 = User.objects.get(pk=1)
        self.assertEqual(user2.username, 'john')
        self.assertEqual(User.objects.all().count(), 1)
        self.assertEqual(user.to_dict(), user._values)

    def test_multiple_models(self):
        user = User(username='john').save()
        City(name='London').save()
        # since City.name is a primary key we can use it to query
        city2 = City.objects.get(pk='London')
        self.assertEqual(user.pk, 1)
        self.assertEqual(city2.pk, 'London')
        raw_db = (dict(row) for row in self.db['user'].all(order_by='id'))
        raw_model = (m.to_dict() for m in user.objects.all(order_by='id'))
        for db, model in zip(raw_db, raw_model):
            # remove attributes, cannot compare json
            db.pop('attributes')
            model.pop('attributes')
            self.assertEqual(db, model)

    def test_save_many(self):
        users = []
        for i in range(10):
            users.append(User(username=f'user{i}'))
        User.save_many(users)
        users_indb = User.objects.find().as_list()
        self.assertEqual(len(users_indb), len(users))
        self.assertTrue(all(u.pk is not None for u in users_indb))

    def test_save_many_custom_pk(self):
        cities = []
        for i in range(10):
            cities.append(City(name=f'London-{i}'))
        City.save_many(cities)
        cities_indb = City.objects.find().as_list()
        self.assertEqual(len(cities_indb), len(cities))

    def test_model_delete(self):
        user = User(username='john').save()
        users = User.objects.all()
        self.assertEqual(len(users), 1)
        # delete on user
        user.delete()
        users = User.objects.all()
        self.assertEqual(len(users), 0)
        # delete many
        User(username='john').save()
        User(username='gil').save()
        User.objects.all().delete()

    def test_unique(self):
        User(username='john', is_nice=True).save()
        with self.assertRaises(sqlalchemy.exc.IntegrityError):
            User(username='john', is_nice=True).save()

    def test_update(self):
        User(username='evil', is_nice=True).save()
        user = User.objects.get(username='evil')
        user.is_nice = False
        user.save()
        user = User.objects.get(username='evil')
        self.assertEqual(user.is_nice, False)

    def test_find_models_all(self):
        User(username='john').save()
        User(username='gil').save()
        # find all users, expect User objects
        users = User.objects.all()
        self.assertEqual(User.objects.all().count(), 2)
        self.assertEqual(len(users), 2)
        self.assertTrue(all(isinstance(u, User) for u in users.as_list()))
        # find all users, expect dicts
        self.assertTrue(all(isinstance(u, dict) for u in users.as_raw()))
        # check we get all users
        users = User.objects.all()
        self.assertEqual(set(u.username for u in users), {'john', 'gil'})
        # check we get all users
        users = User.objects.all()
        self.assertEqual(set(u['username'] for u in users.as_raw()), {'john', 'gil'})

    def test_find_models_query(self):
        User(username='john').save()
        User(username='gil').save()
        # find specific user by name
        users = User.objects.find(username='john')
        self.assertEqual(len(users), 1)
        # find specific user by pk
        users = User.objects.find(pk=1)
        self.assertEqual(len(users), 1)
        # find users by pattern
        users = User.objects.find(username__like='%il')
        self.assertEqual(len(users), 1)
        # find users by sequence
        users = User.objects.find(username=['gil', 'john'])
        self.assertEqual(len(users), 2)
        users = User.objects.find(username__in=['gil', 'john'])
        self.assertEqual(len(users), 2)

    def test_find_models_caching(self):
        User(username='john').save()
        User(username='gil').save()
        # not both calls provide the same result, thanks to caching
        users = User.objects.all()
        self.assertEqual(users.as_list(), users.as_list())
        self.assertEqual(list(users.as_raw()), list(users.as_raw()))
        self.assertEqual(set(u.username for u in users), {'gil', 'john'})
        self.assertEqual(users.count(), 2)

    def test_find_models_nocaching(self):
        User(username='john').save()
        User(username='gil').save()
        # get cached and uncached results
        users = User.objects.all()
        users_nc = users.nocache()
        self.assertFalse(users.resolved)
        # if cached, we always get the same list (i.e. the cache)
        self.assertEqual(id(users.as_list()), id(users.as_list()))
        self.assertListEqual([u for u in users], [u for u in users])
        self.assertEqual(users.count(), 2)
        # if not cached, we get new lists with the same content (query is re-run)
        l1 = users_nc.as_list()
        l2 = users_nc.as_list()
        self.assertNotEqual(id(l1), id((l2)))
        self.assertListEqual([u.username for u in l1], [u.username for u in l2])
        self.assertEqual(users_nc.count(), 2)

    def test_json(self):
        User(username='john', attributes=dict(foo=5)).save()
        user = User.objects.get(username='john')
        self.assertEqual(user.attributes, dict(foo=5))

    def test_datetime(self):
        now = datetime(2022, 3, 25, 11, 53, 45)
        User(username='john', created_dt=now).save()
        user = User.objects.get(username='john')
        self.assertEqual(user.created_dt.isoformat(), now.isoformat())

    def test_date(self):
        now = date(2022, 3, 25)
        User(username='john', updated_date=now).save()
        user = User.objects.get(username='john')
        self.assertEqual(user.updated_date, now)

    def test_float(self):
        User(username='john', value=3.2).save()
        user = User.objects.get(username='john')
        self.assertEqual(user.value, 3.2)

    def test_binary(self):
        User(username='john', data=bytearray(b'hello world')).save()
        user = User.objects.get(username='john')
        self.assertEqual(user.data, b'hello world')

    def test_boolean(self):
        User(username='john', is_nice=True).save()
        user = User.objects.get(username='john')
        self.assertEqual(user.is_nice, True)
        User(username='eveil', is_nice=False).save()
        user = User.objects.get(username='eveil')
        self.assertEqual(user.is_nice, False)

    def test_query(self):
        User(username='john', is_nice=True).save()
        user = User.objects.query('select * from ":t"').first()
        self.assertIsInstance(user, User)
        self.assertEqual(user.username, 'john')
        user = User.objects.query('select username, username as uname from ":t"').first()
        self.assertIsInstance(user, ResultModel)
        self.assertEqual(user.username, 'john')
        self.assertEqual(user.uname, 'john')

    def test_query_auto_resultmodel(self):
        User(username='john walker', is_nice=True).save()
        sql = check_sql(self.db, '''
        select substring(username, 1, 4) as firstname
             , substring(username, 6, 10) as lastname
        from ":t"
        ''')
        results = User.objects.query(sql).first()
        self.assertIsInstance(results, ResultModel)
        self.assertEqual(results.firstname, 'john')
        self.assertEqual(results.lastname, 'walker')

    def test_query_custom_resultmodel(self):
        User(username='john walker', is_nice=True).save()
        sql = check_sql(self.db, '''
        select substring(username, 1, 4) as firstname
             , substring(username, 6, 10) as lastname
        from ":t"
        ''')
        results = User.objects.query(sql, model=QueryResultModel).first()
        self.assertIsInstance(results, QueryResultModel)
        self.assertEqual(results.firstname, 'john')
        self.assertEqual(results.lastname, 'walker')

    def test_query_aggregate(self):
        User(username='john', is_nice=True).save()
        sql = '''
        select username as "group"
             , count(*) as "count"
        from ":t"
        group by username
        '''
        counts = User.objects.query(sql).first()
        self.assertIsInstance(counts, ResultModel)
        self.assertEqual(counts.group, 'john')
        self.assertEqual(counts.count, 1)

    def test_create_new_tables(self):
        Lake = Model.from_spec(name='lake',
                               columns=[
                                   Column(types.string, 'name'),
                               ],
                               db=self.db)
        self.assertEqual(len(Lake.objects.all()), 0)
        Lake(name='Great Lake').save()
        Lake(name='Small Lake').save()
        self.assertEqual(len(Lake.objects.all()), 2)

    def test_models_from_tables(self):
        table = self.db['country']
        table.drop()
        table.insert_many([
            dict(name='Germany'),
            dict(name='United Kingdom'),
        ])
        # we know there is a table, use it as a Model
        table = self.db['country']
        Country = Model.from_table(table)
        self.assertNotEqual(Country, Model)
        self.assertEqual(list(Country.columns.keys()), ['id', 'name'])
        countries = Country.objects.all()
        self.assertEqual(countries.count(), 2)
        for country in countries:
            self.assertIsInstance(country, Country)
            self.assertIn('name', country.to_dict())
            self.assertIn('name', country.columns)

    def test_models_for_tables(self):
        table = self.db['country']
        table.drop()
        table.insert_many([
            dict(name='Germany'),
            dict(name='United Kingdom'),
        ])

        # define a model for a an existing table
        class Country(Model):
            name = Column(types.string)

        Country.use(self.db)
        # use the model
        self.assertEqual(list(Country.columns.keys()), ['name', 'id'])
        countries = Country.objects.all()
        self.assertEqual(countries.count(), 2)
        for country in countries:
            self.assertIsInstance(country, Country)
            self.assertIn('name', country.to_dict())
            self.assertIn('name', country.columns)


def check_sql(db, sql):
    if "sqlite" in db.engine.dialect.name:
        sql = sql.replace('substring', 'substr')
    return sql
