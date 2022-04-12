from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

from dataset_orm import Model, Column, types


class FilesMixin:
    # a multithreaded file-like API to DatasetFile

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
    chunksize = 4096

    def __init__(self, dsfile, mode='r'):
        self._dsfile = dsfile
        self._mode = mode

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def open(self, mode='r'):
        return self._dsfile.__class__.open(self._dsfile.filename, mode=mode)

    def write(self, filelike, chunksize=None, replace=False):
        chunksize = chunksize if chunksize is not None else self.chunksize
        self._check_writeable()
        if replace:
            self.truncate()
        if isinstance(filelike, bytes):
            filelike = BytesIO(filelike)

        def write_part(part_data, part_no):
            filepart = DatasetFilePart(file_id=self._dsfile.id, part_no=part_no, data=part_data)
            filepart.save()

        # When using SQLite, this may result in "not the same thread" exceptions raised on program exit.
        # To avoid, use the check_same_thread=False argument on connecting
        # see https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#threading-pooling-behavior
        with ThreadPoolExecutor() as tp:
            # readfirst chunk
            part_no = self._dsfile.parts
            size = self._dsfile.size
            part_data = filelike.read(chunksize)
            while part_data:
                # background write current chunk
                size += len(part_data)
                tp.submit(write_part, part_data, part_no)
                # read next chunk
                part_no += 1
                part_data = filelike.read(chunksize)

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

    def read(self, size=-1):
        def read_part(part_no):
            try:
                filepart = DatasetFilePart.objects.get(file_id=self._dsfile.id, part_no=part_no)
            except ValueError as e:
                raise FileNotFoundError(f'Cannot read from file {self._dsfile.filename} due to {e}')
            return part_no, filepart.data

        # read all parts, sort by part_no, return buffer's value
        buffer = BytesIO()
        read_size = 0
        with ThreadPoolExecutor() as tp:
            parts = tp.map(read_part, range(0, self._dsfile.parts))
            for part_no, part_data in sorted(parts, key=lambda v: v[0]):
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
