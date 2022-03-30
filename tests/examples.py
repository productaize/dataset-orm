from dataset_orm import Model, Column, types


class User(Model):
    username = Column(types.string, unique=True)
    attributes = Column(types.json)
    data = Column(types.binary)
    value = Column(types.float)
