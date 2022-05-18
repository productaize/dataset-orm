import os
from datetime import datetime
from io import BytesIO
from unittest import TestCase

from dataset_orm import files
from dataset_orm.files import FileLike
from dataset_orm.util import connect, using, DB_CONNECTIONS, DB_CONTEXTS
from tests.examples import City, Image


class MultiDbTests(TestCase):
    def setUp(self):
        self.db_url1 = os.environ.get('TEST_DATABASE_URL', 'sqlite:///testdb.sqlite3')
        self.db_url2 = os.environ.get('TEST_DATABASE_URL2', 'sqlite:///testdb2.sqlite3')
        print(self.db_url1, self.db_url2)
        for alias, db in DB_CONNECTIONS.items():
            db.close() if db.engine is not None else None
        DB_CONNECTIONS.clear()
        DB_CONTEXTS.clear()
        self.db1 = connect(self.db_url1, recreate=True)
        self.db2 = connect(self.db_url2, recreate=True, alias='other')

    def test_directmodel(self):
        """ one Model using another db """
        City(name='London').save()
        self.assertEqual(len(City.objects.all()), 1)
        with City.using(self.db2) as other:
            self.assertEqual(len(other.City.objects.all()), 0)
            other.City(name='London').save()
            self.assertEqual(len(other.City.objects.all()), 1)
        with City.using(self.db2) as other:
            self.assertEqual(len(other.City.objects.all()), 1)
            other.City(name='New York').save()
            self.assertEqual(len(other.City.objects.all()), 2)
        self.assertEqual(len(City.objects.all()), 1)

    def test_using_alias(self):
        """ all models using another db """
        City(name='London').save()
        City(name='Paris').save()
        self.assertEqual(len(City.objects.all()), 2)
        with using('other', recreate=True) as other:
            # check the original model is still connected to db1
            self.assertEqual(len(City.objects.all()), 2)
            # check the other model is not connected to db1
            self.assertEqual(len(other.City.objects.all()), 0)
            other.City(name='New York').save()
            self.assertEqual(len(other.City.objects.all()), 1)
        # check the original model is still connected to db1
        self.assertEqual(len(City.objects.all()), 2)

    def test_usingfiles(self):
        """ files.using another db """
        data = BytesIO(b'thedogjumpsoverthelazyfox')
        files.put(data, 'testfile')
        self.assertEqual(files.list(), ['testfile'])
        with files.using('other') as ofiles:
            data = BytesIO(b'thedogjumpsoverthelazyfox')
            ofiles.put(data, 'testfile2')
            self.assertEqual(files.list(), ['testfile'])
            self.assertEqual(ofiles.list(), ['testfile2'])
        with files.using('other') as ofiles:
            data = BytesIO(b'thedogjumpsoverthelazyfox')
            ofiles.put(data, 'testfile3')
            self.assertEqual(files.list(), ['testfile'])
            self.assertEqual(ofiles.list(), ['testfile2', 'testfile3'])
        self.assertEqual(files.list(), ['testfile'])

    def test_imagemodel_file_column(self):
        """ Model.filefield using another db """
        aimage = Image()
        aimage.imagefile.write(b'foo')
        aimage.save()
        bimage = Image.objects.get(id=aimage.id)
        self.assertEqual(len(Image.objects.all()), 1)
        self.assertIsInstance(bimage.imagefile, FileLike)
        self.assertIsInstance(bimage.imagefile.open('r'), FileLike)
        self.assertEqual(bimage.imagefile.read(), b'foo')
        with files.using('other') as ofiles:
            self.assertEqual(ofiles.list(), [])
        with using('other') as other:
            # no image objects so far
            self.assertEqual(len(other.Image.objects.all()), 0)
            # create and check it is there
            oimage = other.Image()
            ifile = oimage.imagefile
            ifile.write(b'foo')
            oimage.save()
            self.assertEqual(len(Image.objects.all()), 1)
            self.assertEqual(len(other.Image.objects.all()), 1)
        with files.using('other') as ofiles:
            self.assertIn(oimage.imagefile.name, ofiles.list())
            self.assertNotIn(oimage.imagefile.name, files.list())

    def test_usingfiles_manytimes(self):
        """ files.using another db """
        data = BytesIO(b'thedogjumpsoverthelazyfox')
        files.put(data, 'testfile')
        self.assertEqual(files.list(), ['testfile'])
        times = []
        for i in range(100):
            t1 = datetime.now()
            with files.using('other') as ofiles:
                data = BytesIO(b'thedogjumpsoverthelazyfox')
                ofiles.put(data, 'testfile2')
                self.assertEqual(files.list(), ['testfile'])
                self.assertEqual(ofiles.list(), ['testfile2'])
            t2 = datetime.now()
            d = (t2 - t1).microseconds
            times.append(d)
        print(times)


