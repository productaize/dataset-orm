import os
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from itertools import repeat

from dataset_orm import Model, Column, types


class FilesMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def open(cls, filename, mode='r'):
        dsfile = DatasetFile.objects.find(filename=filename).first()
        if dsfile is None:
            if mode == 'r':
                raise FileNotFoundError(f'{filename} does not exist in {cls}')
            else:
                dsfile = DatasetFile(filename=filename, size=0, parts=0)
                dsfile.save()
        elif mode == 'w':
            DatasetFilePart.objects.find(file_id=dsfile.id).delete()
            dsfile.size = 0
            dsfile.parts = 0
            dsfile.save()
        else:
            pass
        return FileLike(dsfile, mode=mode)

    @classmethod
    def remove(cls, filename):
        dsfile = DatasetFile.objects.find(filename=filename).first()
        if dsfile is None:
            raise FileNotFoundError(f'{filename} does not exist in {cls}')
        DatasetFilePart.objects.find(file_id=dsfile.id).delete()
        dsfile.delete()

    @classmethod
    def exists(cls, filename):
        dsfile = DatasetFile.objects.find(filename=filename).first()
        if dsfile is None:
            return False
        return True

    @classmethod
    def find(cls, pattern):
        pattern = pattern.replace('*', '%')
        return [f.filename for f in DatasetFile.objects.find(filename__like=pattern)]

    @classmethod
    def list(cls):
        return cls.find('*')


class FileLike:
    # a multithreaded file-like API to DatasetFile
    chunksize = 1024 * 256  # 256KB file parts
    # testing resulted in deadlocks due to too many threads, blocking on BytesIO.read() calls
    # backport from python 3.8, see https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
    max_workers = min(32, os.cpu_count() + 4)

    def __init__(self, dsfile, mode='r'):
        self._dsfile = dsfile
        self._mode = mode

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def open(self, mode='r'):
        return self._dsfile.__class__.open(self._dsfile.filename, mode=mode)

    def write(self, filelike, chunksize=None, batchsize=1000, replace=False):
        # chunksize controls reading of filelike (should be multiples of 8KB)
        # batchsize controls batching of db inserts (should be large)
        # note:
        #    no parts = size(filelike) / chunksize
        #    no inserts = (no parts) / batchsize
        # on writing,
        #    - minimize the number of inserts, however => self.max_workers / 2
        #      so we get parallel writes and reads
        # on reading,
        #    - reads are parallelized, so we can read at most self.max_workers * batchsize parts at a time
        #    - the larger the parts, the smaller the batchsize(reading) should be
        #    - thus batchsize(reading) << batchsize(writing)
        chunksize = chunksize if chunksize is not None else self.chunksize
        self._check_writeable()
        if replace:
            self.truncate()
        if isinstance(filelike, bytes):
            filelike = BytesIO(filelike)

        def _write_parts(*parts):
            DatasetFilePart.save_many(parts)

        # When using SQLite, this may result in "not the same thread" exceptions raised on program exit.
        # To avoid, use the check_same_thread=False argument on connecting
        # see https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#threading-pooling-behavior
        with ThreadPoolExecutor(max_workers=self.max_workers,
                                thread_name_prefix='dataset-orm-write') as tp:
            # readfirst chunk
            part_no = self._dsfile.parts
            size = self._dsfile.size
            part_data = filelike.read(chunksize)
            buffer = []
            while part_data:
                # collect batches of chunks
                size += len(part_data)
                part = DatasetFilePart(file_id=self._dsfile.id,
                                       part_no=part_no,
                                       data=part_data)
                buffer.append(part)
                # write out in background once batch is full
                if len(buffer) >= batchsize:
                    tp.submit(_write_parts, *buffer)
                    buffer = []
                # read next chunk
                part_no += 1
                part_data = filelike.read(chunksize)
            # flush buffer - this may happen on filelike EOF
            if len(buffer) > 0:
                tp.submit(_write_parts, *buffer)

        # update file info
        self._dsfile.size = size
        self._dsfile.parts = part_no
        self._dsfile.save()

    def truncate(self):
        self._check_writeable()
        DatasetFilePart.objects.find(file_id=self._dsfile.id).delete()
        self._dsfile.size = 0
        self._dsfile.parts = 0
        self._dsfile.save()
        return self

    def read(self, size=-1, batchsize=10):
        def _read_part(job):
            # each job is to read a range of parts
            start_no, file_id, batchsize = job
            stop_no = start_no + batchsize - 1
            objects = DatasetFilePart.objects
            # we only need the bare data, not the actual object, no need to cache
            data = [part.data for part in objects.find(file_id=file_id,
                                                       part_no__between=(start_no, stop_no),
                                                       order_by='part_no').nocache()]
            if not data:
                raise FileNotFoundError(f'Cannot read from file {self._dsfile.filename}')
            return start_no, data

        # retrieve all parts in parallel, in batches
        buffer = BytesIO()
        read_size = 0
        with ThreadPoolExecutor(max_workers=self.max_workers,
                                thread_name_prefix='dataset-orm-read') as tp:
            jobs = zip(range(0, self._dsfile.parts, batchsize),
                       repeat(self._dsfile.id), repeat(batchsize))
            batches = tp.map(_read_part, jobs)
        # combine parts in right order, each batch is sorted already
        for start_no, batch_data in sorted(batches, key=lambda v: v[0]):
            for part_data in batch_data:
                buffer.write(part_data)
                read_size += len(part_data)
                if read_size >= size > -1:
                    break

        return buffer.getvalue()

    @property
    def size(self):
        return self._dsfile.size

    @property
    def name(self):
        return self._dsfile.filename

    def readchunks(self, size=-1):
        read_size = 0
        for part_no in range(0, self._dsfile.parts):
            part = DatasetFilePart.objects.get(file_id=self._dsfile.id, part_no=part_no)
            yield part
            read_size += len(part.data)
            if read_size >= size > -1:
                break

    def remove(self):
        self._dsfile.remove(self._dsfile.filename)

    def _check_writeable(self):
        if 'w' not in self._mode:
            raise ValueError(f'Cannot write to {self._dsfile.filename}, mode is {self._mode}')


class DatasetFile(FilesMixin, Model):
    filename = Column(types.string(length=255), unique=True)
    size = Column(types.integer)  # size in bytes
    parts = Column(types.integer)  # count of parts


class DatasetFilePart(Model):
    file_id = Column(types.integer, index=True)
    part_no = Column(types.integer, index=True)
    data = Column(types.binary)
