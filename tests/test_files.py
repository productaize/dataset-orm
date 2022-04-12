import os
import unittest
from io import BytesIO

from dataset_orm import connect
from dataset_orm.files import DatasetFilePart, DatasetFile
from tests.examples import Image


class DatasetFileTests(unittest.TestCase):
    def setUp(self):
        db_url = os.environ.get('TEST_DATABASE_URL', 'sqlite:///testdb.sqlite3')
        self.db = connect(db_url, recreate=True)

    def test_filemodel_bare(self):
        # bare chunking / read back test
        data = BytesIO(b'thedogjumpsoverthelazyfox' * 1000)
        chunksize = 255
        part_no = 0
        filename = 'foo'
        # chunk it up, save each chunk
        dsfile = DatasetFile(filename=filename)
        dsfile.save()
        part_data = data.read(chunksize)
        while part_data:
            filepart = DatasetFilePart(file_id=dsfile.id, part_no=part_no, data=part_data)
            filepart.save()
            part_data = data.read(chunksize)
            part_no += 1
        # read back in order
        parts = DatasetFilePart.objects.find(file_id=dsfile.id, order_by='part_no')
        read_back = BytesIO()
        for part in parts:
            read_back.write(part.data)
        self.assertEqual(read_back.getvalue(), data.getvalue())
        # read back in wrong order
        parts = DatasetFilePart.objects.find(file_id=dsfile.id, order_by='-part_no')
        read_back = BytesIO()
        for part in parts:
            read_back.write(part.data)
        self.assertNotEqual(read_back.getvalue(), data.getvalue())

    def test_filelike(self):
        data = BytesIO(b'thedogjumpsoverthelazyfox' * 1000)
        with DatasetFile.open('testfile', 'w') as testf:
            testf.write(data)
        with DatasetFile.open('testfile') as testf:
            read_back = testf.read()
        self.assertEqual(data.getvalue(), read_back)
        self.assertEqual(testf.size, len(data.getvalue()))
        self.assertEqual(testf.size, len(read_back))

    def test_filelike_append(self):
        data = BytesIO(b'thedogjumpsoverthelazyfox' * 1000)
        with DatasetFile.open('testfile', 'w') as testf:
            testf.write(data)
        more_data = BytesIO(b'thedogjumpsoverthelazyfox' * 1000)
        with DatasetFile.open('testfile', 'w+') as testf:
            testf.write(more_data)
        with DatasetFile.open('testfile') as testf:
            read_back = testf.read()
        self.assertEqual(data.getvalue() + more_data.getvalue(), read_back)
        self.assertEqual(testf.size, len(data.getvalue()) + len(more_data.getvalue()))
        self.assertEqual(testf.size, len(read_back))

    def test_delete(self):
        data = BytesIO(b'thedogjumpsoverthelazyfox' * 1000)
        with DatasetFile.open('testfile', 'w') as testf:
            testf.write(data)
        DatasetFile.remove('testfile')
        with self.assertRaises(FileNotFoundError):
            DatasetFile.open('testfile')

    def test_exists(self):
        data = BytesIO(b'thedogjumpsoverthelazyfox' * 1000)
        self.assertFalse(DatasetFile.exists('testfile'))
        with DatasetFile.open('testfile', 'w') as testf:
            testf.write(data)
        self.assertTrue(DatasetFile.exists('testfile'))

    def test_find(self):
        data = BytesIO(b'thedogjumpsoverthelazyfox' * 1000)
        with DatasetFile.open('testfile', 'w') as testf:
            testf.write(data)
        self.assertEqual(DatasetFile.find('test*'), ['testfile'])

    def test_list(self):
        data = BytesIO(b'thedogjumpsoverthelazyfox' * 1000)
        with DatasetFile.open('testfile', 'w') as testf:
            testf.write(data)
        with DatasetFile.open('otherfile', 'w') as testf:
            testf.write(data)
        self.assertEqual(DatasetFile.list(), ['testfile', 'otherfile'])

    def test_model(self):
        aimage = Image()
        aimage.imagefile.write('foo')

    if __name__ == '__main__':
        unittest.main()


if __name__ == '__main__':
    unittest.main()
