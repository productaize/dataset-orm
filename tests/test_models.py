from unittest import TestCase

from dataset_orm.tests.examples import User


class DatasetOrmTests(TestCase):
    def test_model_definition(self):
        user = User(username='john')
        user.save()

