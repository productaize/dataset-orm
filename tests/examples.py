from dataset_orm import Model, Column, types, ResultModel
from dataset_orm.model import TableSpec


class User(Model):
    # in some dbs, unique strings must be limited in length
    username = Column(types.string(length=100), unique=True)
    name = Column(types.string)
    attributes = Column(types.json)
    data = Column(types.binary)
    value = Column(types.float)
    created_dt = Column(types.datetime)
    updated_date = Column(types.date)
    is_nice = Column(types.boolean)


class City(Model):
    _spec = TableSpec(primary_id='name', primary_type=types.string(length=100))


class Image(Model):
    imagefile = Column(types.file)


class QueryResultModel(ResultModel):
    firstname = Column(types.string)
    lastname = Column(types.string)
