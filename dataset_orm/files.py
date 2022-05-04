import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from itertools import repeat

from dataset_orm import Model, Column, types

__all__ = ['open', 'remove', 'exists', 'find', 'list', 'write', 'read', 'FileLike']


class FilesMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def open(cls, filename, mode='r'):
        """ open a database-stored file

        Args:
            filename (str): the name of the file, max length 255 bytes to
              support efficient indexing.
            mode (str): the open mode, use 'w' to create a new file or replace
              an existing one, 'w+' to append to an existing file, 'r' to read
              from an exiting file. defaults to 'r'

        Returns:
            FileLike
        """
        dsfile = DatasetFile.objects.find(filename=filename).first()
        if dsfile is None:
            # non-existing file
            if mode == 'r':
                raise FileNotFoundError(f'{filename} does not exist in {cls}')
            else:
                dsfile = DatasetFile(filename=filename, size=0, parts=0)
                dsfile.save()
                flike = FileLike(dsfile, mode=mode)
        elif mode in ('w', 'wb'):
            # write new
            DatasetFilePart.objects.find(file_id=dsfile.id).delete()
            dsfile.size = 0
            dsfile.parts = 0
            dsfile.save()
            flike = FileLike(dsfile, mode=mode)
        elif any(v in mode for v in ('r', 'b', '+')):
            # read or append
            flike = FileLike(dsfile, mode=mode)
        else:
            raise ValueError(f'mode {mode} is not supported')
        return flike

    @classmethod
    def remove(cls, filename):
        """ remove a file

        Args:
            filename (str): the filename

        Returns:
            None
        """
        dsfile = DatasetFile.objects.find(filename=filename).first()
        if dsfile is None:
            raise FileNotFoundError(f'{filename} does not exist in {cls}')
        DatasetFilePart.objects.find(file_id=dsfile.id).delete()
        dsfile.delete()

    @classmethod
    def exists(cls, filename):
        """ check if a file exists

        Args:
            filename (str): the filename

        Returns:
            True if the file exists, False otherwise
        """
        dsfile = DatasetFile.objects.find(filename=filename).first()
        if dsfile is None:
            return False
        return True

    @classmethod
    def find(cls, pattern):
        """ find all files matching a given pattern

        Args:
            pattern (str): the filename pattern. This allows for POSIX and
              SQL-like wildcards * or %.

        Returns:
            list of filenames matching the given pattern
        """
        pattern = pattern.replace('*', '%')
        return [f.filename for f in DatasetFile.objects.find(filename__like=pattern)]

    @classmethod
    def list(cls):
        """ shortcut to files.find('*') """
        return cls.find('*')

    @classmethod
    def write(cls, filename, data, mode='w'):
        """ write data to a file

        Args:
            filename (str): the name of the file
            data (bytes): the byte array to write
            mode (str): the file mode, defaults to 'w'

        Returns:
            FileLike
        """
        return DatasetFile.open(filename, mode=mode).write(data)

    @classmethod
    def read(cls, filename):
        """ read data from a file
        Args:
            filename (str): the name of the file

        Returns:
            bytes
        """
        return DatasetFile.open(filename, mode='r').read()

    @classmethod
    def close(cls):
        """ close a FileLike object """
        pass


class FileLike:
    # a multithreaded file-like API to DatasetFile
    chunksize = 1024 * 256  # 256KB file parts, these show the best average read performance on sqlite + mssql
    # testing resulted in deadlocks due to too many threads, blocking on BytesIO.read() calls
    # backport from python 3.8, see https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
    max_workers = min(32, os.cpu_count() + 4)

    def __init__(self, dsfile, mode='r'):
        self._dsfile = dsfile
        self._mode = mode

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def file(self):
        """ return the DatasetFile associated with this FileLike"""
        return self._dsfile

    @property
    def name(self):
        """ return the name of the DatasetFile """
        return self.file.filename

    @property
    def size(self):
        """ return the size of the DatasetFile """
        return self._dsfile.size

    def open(self, mode='r'):
        """ open the file for reading (default) or writing (mode='w'), or both (mode='rw') """
        return self._dsfile.__class__.open(self._dsfile.filename, mode=mode)

    def close(self):
        """ close the file for further processing """
        pass

    def write(self, filelike, chunksize=None, batchsize=1000, replace=False):
        """ multi-threaded write low-level interface

        Unless you need specific chunking support, you should use files.put()
        or files.write()

        Args:
              filelike (file-like|bytes): a file-like object that supports
                 read(chunksize), or a bytes object
              chunksize (int|None): the number of bytes for each chunk written,
                 if None it defaults to reading the file in 256K blocks.
              batchsize (int): maximum number of chunks in local buffer before it is
                 flushed to the file. This determines the number of inserts to the
                 database and should be chosen in order to minimize total inserts.
              replace (bool): if  True will replace the file if it exists, otherwise
                 will append. Defaults to False

        """
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
            DatasetFilePart._db.commit()

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
        return self.file

    def truncate(self):
        """ deletes all file content, keeps the DatasetFile (same id) """
        self._check_writeable()
        DatasetFilePart.objects.find(file_id=self._dsfile.id).delete()
        self._dsfile.size = 0
        self._dsfile.parts = 0
        self._dsfile.save()
        return self

    def read(self, size=-1, batchsize=10):
        """ multi-threaded reading of file contents

        Unless you need specific chunking support, you should use files.get()

        Args:
            size (int, -1): the number of bytes to be returned, if -1, returns all data (default)
            batchsize (int): the number of chunks to read for each batch. defaults to 10
        """

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
            # build jobs, each job reads a range of file parts
            jobs: list[int, int, int]  # start_no, file_id, batchsize
            jobs = zip(range(0, self._dsfile.parts, batchsize),
                       repeat(self._dsfile.id), repeat(batchsize))
            # submit jobs and sort by starting part_no
            results: list[int, list]  # part_no, batch_data
            results = sorted(tp.map(_read_part, jobs), key=lambda v: v[0])
            # combine parts
            batch_data = (batch_data for start_no, batch_data in results)
            for part_data in batch_data:
                buffer.write(b''.join(part_data))
                read_size += len(part_data)
                if read_size >= size > -1:
                    tp.shutdown(wait=False)
                    break

        return buffer.getvalue()

    def readchunks(self, size=-1):
        """ read one chunk at a time """
        read_size = 0
        for part_no in range(0, self._dsfile.parts):
            part = DatasetFilePart.objects.get(file_id=self._dsfile.id, part_no=part_no)
            yield part.data
            read_size += len(part.data)
            if read_size >= size > -1:
                break

    def remove(self):
        self._dsfile.remove(self._dsfile.filename)

    def _check_writeable(self):
        if 'w' not in self._mode:
            raise ValueError(f'Cannot write to {self._dsfile.filename}, mode is {self._mode}')


# the files Models. Should not be directly used by applications
class DatasetFile(FilesMixin, Model):
    filename = Column(types.string(length=255), unique=True)
    size = Column(types.integer)  # size in bytes
    parts = Column(types.integer)  # count of parts


class DatasetFilePart(Model):
    file_id = Column(types.integer, index=True)
    part_no = Column(types.integer, index=True)
    data = Column(types.binary)


# public methods
open = DatasetFile.open
remove = DatasetFile.remove
exists = DatasetFile.exists
find = DatasetFile.find
list = DatasetFile.list
write = DatasetFile.write
read = DatasetFile.read


# convenience methods
def put(data, filename=None, mode='w'):
    """ create a new file and write data

    This is a convenience method and functionally equivalent to

        filename = uuid4().hex
        with files.open(filename, data) as fout:
            fout.write(data)

    Args:
        data (bytes): the data to write
        filename (str): the name of the file, if not specified defaults to
            uuid4().hex
        mode (str): specify the open mode, use 'w' to create a new file or
            replace an existing one, use 'w+' to append to an existing file.
            defaults to 'w'

    Returns:
        DatasetFile
    """
    from uuid import uuid4
    filename = filename or uuid4().hex
    with DatasetFile.open(filename, mode='w') as fout:
        fout.write(data)
    return fout


def get(filename):
    """ return the FileLike object to the given filename

    Args:
        filename (str): the name of the file

    Returns:
        FileLike
    """
    return DatasetFile.open(filename)
