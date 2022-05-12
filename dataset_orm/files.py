"""
dataset_orm.files implements a file-like API to efficiently store files of
arbitrary size into an SQL database .

The files modules provides typical operations of a filesystem:

* open() - to create and access a file, return a FileLike object
* exists() - to check if a file exists
* list() - to list all files
* read() - to read a file
* write() - to write to a file
* remove() - to remove a file

In addition, convenience methods are provided to simplify writing and reading:

* put() - to store a binary string
* get() - to retrieve a file's content as a binary string

The implementation provides for efficiency in several ways:

1. Reading and writing of files is done in parallel, whereby a
   file's content is split into smaller chunks and the chunks are
   written and read in parallel. This increases throughput by about 25% v.v.
   the storage of a single BLOB object.

2. The filenames and its chunks are indexed which means that access is O(1).

3. The directory of files is stored separately from the files contents,
   which means that the list(), exists() and remove() operations are fast.

See the comments on Prior Work at the end of this module for references on
inspiration drawn from other libraries.
"""
import contextlib
import os
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from itertools import repeat

from sqlalchemy.util import classproperty

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
        dsfile = cls.objects.find(filename=filename).first()
        if dsfile is None:
            # non-existing file
            if mode == 'r':
                raise FileNotFoundError(f'{filename} does not exist in {cls}')
            else:
                dsfile = cls(filename=filename, size=0, parts=0)
                dsfile.save()
                flike = FileLike(dsfile, mode=mode)
        elif mode in ('w', 'wb'):
            # write new
            cls.DatasetFilePart.objects.find(file_id=dsfile.id).delete()
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
    def remove(cls, filename, errors=True):
        """ remove a file

        Args:
            filename (str): the filename
            errors (bool): if True and file does not exist, raise FileNotFoundError

        Returns:
            None
        """
        dsfile = cls.objects.find(filename=filename).first()
        if dsfile is None and errors:
            raise FileNotFoundError(f'{filename} does not exist in {cls}')
        cls.DatasetFilePart.objects.find(file_id=dsfile.id).delete()
        dsfile.delete()

    @classmethod
    def exists(cls, filename):
        """ check if a file exists

        Args:
            filename (str): the filename

        Returns:
            True if the file exists, False otherwise
        """
        dsfile = cls.objects.find(filename=filename).first()
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
        return [f.filename for f in cls.objects.find(filename__like=pattern)]

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
        return cls.open(filename, mode=mode).write(data)

    @classmethod
    def read(cls, filename):
        """ read data from a file
        Args:
            filename (str): the name of the file

        Returns:
            bytes
        """
        return cls.open(filename, mode='r').read()

    @classmethod
    def close(cls):
        """ close a FileLike object """
        pass

    # convenience methods
    @classmethod
    def put(cls, data, filename=None, mode='rw'):
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
        with cls.open(filename, mode=mode) as fout:
            fout.write(data)
        return fout

    @classmethod
    def get(cls, filename):
        """ return the FileLike object to the given filename

        Args:
            filename (str): the name of the file

        Returns:
            FileLike
        """
        return cls.open(filename)


class FileLike:
    # a multithreaded file-like API to DatasetFile
    chunksize = 1024 * 256  # 256KB file parts, these show the best average read performance on sqlite + mssql
    # testing resulted in deadlocks due to too many threads, blocking on BytesIO.read() calls
    # backport from python 3.8
    # see https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
    max_workers = min(32, os.cpu_count() + 4)

    def __init__(self, dsfile, mode='r'):
        self._dsfile = dsfile
        self._mode = mode
        self._readpos = 0
        self._readline_iter = None
        self._readbuffer = None

    def __enter__(self):
        return self.open(mode=self._mode)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def file(self):
        """ return the DatasetFile associated with this FileLike"""
        return self._dsfile

    @property
    def DatasetFilePart(self):
        return self.file.DatasetFilePart

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
        if self._readbuffer:
            self._readbuffer.close()
            self._readbuffer = None

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
            self.DatasetFilePart.save_many(parts)
            self.DatasetFilePart._db.commit()

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
                part = self.DatasetFilePart(file_id=self._dsfile.id,
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
        self.DatasetFilePart.objects.find(file_id=self._dsfile.id).delete()
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
        self._check_readable()

        def _read_part(job):
            # each job is to read a range of parts
            start_no, file_id, batchsize = job
            stop_no = start_no + batchsize - 1
            objects = self.DatasetFilePart.objects
            # we only need the bare data, not the actual object, no need to cache
            data = [part.data for part in objects.find(file_id=file_id,
                                                       part_no__between=(start_no, stop_no),
                                                       order_by='part_no').nocache()]
            if not data:
                raise FileNotFoundError(f'Cannot read from file {self._dsfile.filename}')
            return start_no, data

        def _read_full(buffer):
            # retrieve all parts in parallel, in batches
            # we always read all the data, so we can use BytesIO for size support
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
            buffer.seek(self._readpos)
            return buffer

        # read all data into buffer
        if self._readbuffer is None:
            self._readbuffer = BytesIO()
            _read_full(self._readbuffer)
        data = self._readbuffer.read(size)
        return data

    def readchunks(self, size=-1):
        """ read one chunk at a time """
        self._check_readable()
        read_size = 0
        for part_no in range(0, self._dsfile.parts):
            part = self.DatasetFilePart.objects.get(file_id=self._dsfile.id, part_no=part_no)
            yield part.data
            read_size += len(part.data)
            if read_size >= size > -1:
                break

    def remove(self):
        self._check_readable(force=True)
        self.close()
        self._dsfile.remove(self._dsfile.filename)

    def _check_writeable(self):
        if 'w' not in self._mode:
            raise ValueError(f'Cannot write to {self._dsfile.filename}, mode is {self._mode}')
        if getattr(self._dsfile, 'id', None) is None:
            self._dsfile = self.open(self._mode)._dsfile

    def _check_readable(self, force=False):
        if not force and 'r' not in self._mode:
            raise ValueError(f'Cannot read from {self._dsfile.filename}, mode is {self._mode}')
        if getattr(self._dsfile, 'id', None) is None:
            self._dsfile = self.open(mode=self._mode)._dsfile

    def seek(self, pos):
        self._readpos = pos
        if self._readbuffer:
            self._readbuffer.seek(pos)

    def readline(self, size=-1):
        if self._readbuffer is None:
            self.read()
        return self._readbuffer.readline(size)


# the files Models. Should not be directly used by applications
class DatasetFile(FilesMixin, Model):
    filename = Column(types.string(length=255), unique=True)
    size = Column(types.integer)  # size in bytes
    parts = Column(types.integer)  # count of parts

    _DatasetFilePart = None

    @classproperty
    def DatasetFilePart(cls):
        # return DataSetFilePart linked to the same db
        # the global DatasetFilePart may be linked to the default db while cls is not
        if cls._DatasetFilePart is None:
            cls._DatasetFilePart = DatasetFilePart._make_using(cls._db)
        return cls._DatasetFilePart


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
put = DatasetFile.put
get = DatasetFile.get


@contextlib.contextmanager
def using(alias=None):
    from dataset_orm.util import DBContext, using as orm_using, DB_CONTEXTS

    alias = alias or 'default'
    files_alias = f'{alias}.files'
    files_using = DB_CONTEXTS.get(files_alias)
    if not files_using:
        with orm_using(alias, models=(DatasetFile,)) as models:
            files_using = DBContext({
                'open': models.DatasetFile.open,
                'remove': models.DatasetFile.remove,
                'exists': models.DatasetFile.exists,
                'find': models.DatasetFile.find,
                'list': models.DatasetFile.list,
                'write': models.DatasetFile.write,
                'read': models.DatasetFile.read,
                'put': models.DatasetFile.put,
                'get': models.DatasetFile.get,
            })
        DB_CONTEXTS[files_alias] = files_using
    yield files_using


# Related Work
#
# The API of this module is inspired by mongoengine's GridFS implementation
# of a FileField. The chunking of files is inspired by the SQLGrid
# implementation of GridFS spec and the ReGrid spec. Both specify
# ways to store large files to a database, namely by splitting the files
# into chunks. It should be noted that these specifications are pre-dated
# by a large body of knowledge around storage of data, where a key concept
# is to store large files in smaller items (chunks) and organize the chunks
# by means of an index and an API for easy access and retrieval.
#
# See Also:
# * ReGrid Spec
#   https://github.com/internalfx/regrid-spec
# * GridFS and FileField API in mongoengine
#   https://docs.mongoengine.org/guide/gridfs.html
# * The formal documentation of a Block Storage Service
#   https://www.cs.ox.ac.uk/files/3383/PRG62.pdf
# * The Google File System
#   https://blough.ece.gatech.edu/6102/presentations/gfs-sosp2003.pdf
