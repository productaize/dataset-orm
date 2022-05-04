import os
import unittest
from functools import partial
from io import BytesIO
from timeit import timeit

from unittest import skip

from dataset_orm import connect
from dataset_orm.files import DatasetFilePart, DatasetFile, FileLike
from tests.examples import Image


class DatasetFileTests(unittest.TestCase):
    def setUp(self):
        db_url = os.environ.get('TEST_DATABASE_URL', 'sqlite:///testdb.sqlite3')
        self.db = connect(db_url, recreate=True)
        print(db_url)

    def tearDown(self):
        self.db.close()

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
        data = BytesIO(b'thedogjumpsoverthelazyfox')
        with DatasetFile.open('testfile', 'w') as testf:
            testf.write(data)
        self.assertTrue(len(DatasetFilePart.objects.all().as_list()) > 0)
        # reading whole file
        with DatasetFile.open('testfile') as testf:
            read_back = testf.read()
        self.assertEqual(data.getvalue(), read_back)
        self.assertEqual(testf.size, len(data.getvalue()))
        self.assertEqual(testf.size, len(read_back))
        # reading chunks, one by one
        with DatasetFile.open('testfile') as testf:
            data_ = BytesIO()
            for chunk in testf.readchunks():
                self.assertIsInstance(chunk, bytes)
                data_.write(chunk)
            self.assertEqual(data_.getvalue(), data.getvalue())

    def test_files_api(self):
        from dataset_orm import files
        data = b'foobar'
        files.write('testfile', data)
        read_back = files.read('testfile')
        self.assertEqual(data, read_back)
        self.assertTrue(files.exists('testfile'))
        self.assertEqual(files.list(), ['testfile'])
        self.assertEqual(files.find('test*'), ['testfile'])
        files.remove('testfile')
        self.assertEqual(files.list(), [])

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
        data = BytesIO(b'thedogjumpsoverthelazyfox' * 10000)
        with DatasetFile.open('testfile', 'w') as testf:
            testf.write(data)
        self.assertEqual(DatasetFile.find('test*'), ['testfile'])

    def test_list(self):
        data = BytesIO(b'thedogjumpsoverthelazyfox' * 10000)
        with DatasetFile.open('testfile', 'w') as testf:
            testf.write(data)
        with DatasetFile.open('otherfile', 'w') as testf:
            testf.write(data)
        self.assertEqual(DatasetFile.list(), ['testfile', 'otherfile'])

    def test_putget_explicit(self):
        from dataset_orm import files
        data = BytesIO(b'thedogjumpsoverthelazyfox')
        dsfile = files.put(data, filename='foo')
        self.assertIsInstance(dsfile, FileLike)
        dsfile = files.get('foo')
        self.assertIsInstance(dsfile, FileLike)
        data_ = dsfile.read()
        self.assertEqual(data_, data.getvalue())

    def test_putget_implicit(self):
        from dataset_orm import files
        data = BytesIO(b'thedogjumpsoverthelazyfox')
        dsfile = files.put(data)
        self.assertIsInstance(dsfile, FileLike)
        dsfile = files.get(dsfile.name)
        self.assertIsInstance(dsfile, FileLike)
        data_ = dsfile.read()
        self.assertEqual(data_, data.getvalue())

    def test_imagemodel_file_column(self):
        aimage = Image()
        aimage.imagefile.write(b'foo')
        aimage.save()
        bimage = Image.objects.get(id=aimage.id)
        self.assertIsInstance(bimage.imagefile, FileLike)
        self.assertIsInstance(bimage.imagefile.open('r'), FileLike)
        self.assertEqual(bimage.imagefile.read(), b'foo')

    def test_imagemodel_rewrite(self):
        aimage = Image()
        aimage.imagefile.write(b'foo')
        aimage.save()
        bimage = Image.objects.get(id=aimage.id)
        self.assertEqual(bimage.imagefile.read(), b'foo')
        # append
        bimage.imagefile.open('w+').write(b'foo')
        bimage.save()
        cimage = Image.objects.get(id=aimage.id)
        self.assertEqual(b'foofoo', cimage.imagefile.read())
        # overwrite
        bimage.imagefile.truncate().write(b'foo')
        bimage.save()
        cimage = Image.objects.get(id=aimage.id)
        self.assertEqual(cimage.imagefile.read(), b'foo')
        # delete
        cimage.imagefile.remove()
        with self.assertRaises(FileNotFoundError):
            cimage.imagefile.read()

    @skip("for interactive use only")
    def test_parallel_write_performance(self):
        with open('/tmp/testfile', 'wb') as fout:
            fout.write(b'thedogjumpsoverthelazyfox' * 100000)

        def write(chunksize=None):
            with open('/tmp/testfile', 'rb') as fin:
                with DatasetFile.open(f'testfile-{chunksize}', 'w') as testf:
                    testf.write(fin, chunksize=chunksize)

        t_seq = timeit(partial(write, chunksize=-1), number=1)
        t_small = timeit(partial(write, chunksize=255), number=1)
        t_large = timeit(partial(write, chunksize=1024 * 512), number=1)
        print(t_seq, t_small, t_large)

    @skip("for interactive use only")
    def test_parallel_read_performance(self):
        with open('/tmp/testfile', 'wb') as fout:
            # 10'000 = 240KB
            # 100'000 = 2.4M
            fout.write(b'thedogjumpsoverthelazyfox' * 1000000)

        def write(chunksize=None):
            with open('/tmp/testfile', 'rb') as fin:
                with DatasetFile.open(f'testfile', 'w') as testf:
                    testf.write(fin, chunksize=chunksize)

        def read(chunksize=None):
            with DatasetFile.open(f'testfile', 'r') as testf:
                data = testf.read()

        N = 30
        write(chunksize=-1)
        t_seq = timeit(read, number=N)
        write(chunksize=1024 * 512)
        t_small = timeit(read, number=N)
        write(chunksize=1024 * 256)
        t_large = timeit(read, number=N)
        print(t_seq, t_small, t_large)


if __name__ == '__main__':
    unittest.main()
