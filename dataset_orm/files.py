from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

from dataset_orm import Model, Column, types


class FileLikeMixin:
    # a multithreaded file-like API to DatasetFile
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
        return dsfile

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def write(self, filelike, chunksize=255):
        if isinstance(filelike, bytes):
            filelike = BytesIO(filelike)

        def write_part(part_data, part_no):
            filepart = DatasetFilePart(file_id=self.id, part_no=part_no, data=part_data)
            filepart.save()

        with ThreadPoolExecutor() as tp:
            # readfirst chunk
            part_no = self.parts
            size = self.size
            part_data = filelike.read(chunksize)
            while part_data:
                # background write current chunk
                size += len(part_data)
                tp.submit(write_part, part_data, part_no)
                # read next chunk
                part_no += 1
                part_data = filelike.read(chunksize)

        # update file info
        self.size = size
        self.parts = part_no
        self.save()

    def read(self, size=-1):
        def read_part(part_no):
            part = DatasetFilePart.objects.get(file_id=self.id, part_no=part_no)
            return part_no, part.data

        # read all parts, sort by part_no, return buffer's value
        buffer = BytesIO()
        read_size = 0
        with ThreadPoolExecutor() as tp:
            parts = tp.map(read_part, range(0, self.parts))
            for part_no, part_data in sorted(parts, key=lambda v: v[0]):
                buffer.write(part_data)
                read_size += len(part_data)
                if read_size >= size > -1:
                    break

        return buffer.getvalue()

    def readchunks(self, size=-1):
        read_size = 0
        for part_no in range(0, self.parts):
            part = DatasetFilePart.objects.get(file_id=self.id, part_no=part_no)
            yield part
            read_size += len(part.data)
            if read_size >= size > -1:
                break


class DatasetFile(FileLikeMixin, Model):
    filename = Column(types.string, unique=True)
    size = Column(types.integer)  # size in bytes
    parts = Column(types.integer)  # count of parts


class DatasetFilePart(Model):
    file_id = Column(types.integer, index=True)
    part_no = Column(types.integer, index=True)
    data = Column(types.binary)
