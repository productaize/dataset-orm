Fast Active Record ORM for the dataset library
==============================================

Why?
----

The dataset library is a great and easy tool to work with any SQL database. Unfortunately, it lacks an object mapper (
ORM) - if you need one you are left with the complexity that is sqlalchemy.

Enter dataset-orm

Features
--------

* class-based ORM models (backed by SQLAlchemy, just without the complexity)
* dynamic ORM models from existing tables or in-code table specs
* create rows from Python objects
* get back Python objects using Model.objects.all()/find()/get() or by native SQL
* stores dicts as json, files as binary automatically
* use multiple databases with the same ORM models, concurrently

dataset-orm also includes a file-system alike that works with any SQL database supported by SQlAlchemy. This is useful 
for all cloud-deployed and other 12-factor applications that cannot use a server's native file system.  

* transparent in-database storage for files of any size
* files can be stored inside ORM Models or by using the files API from anywhere (a Model is not required)
* simple put()/get() semantics with automatic filename generation (optional) 
* parallelized writing and reading provides up to 25% speed-up to binary fields

How?
----

Installation

    $ pip install dataset-orm

Define classes that define a dataset.Table:

    from dataset_orm import Model, connect

    class User(Model):
        username = Column(types.string, unique=True)
        data = Column(types.json)

    connect('sqlite:///mydb.sqlite')

Alternatively, use the functional API, e.g. to create models dynamically:

    User = ds.Model.from_spec(name='User',
                              columns=[ds.Column(ds.types.string, 'name', unique=True),
                                       ds.Column(ds.types.json, 'data')],
                              db=db)

Then create rows directly from Python objects:

    user = User(username='dave', data={'sports': ['football', 'tennis']'})
    user.save()

    user = User.objects.find_one(username='dave')
    user.data
    => 
    {'sports': ['football', 'tennis']'}

Query exiting tables, ORM-style:

    User = Model.from_table(db['customer'])
    User.objects.all()
    =>
    [ User(pk=1), User(pk=2), User(pk=3)]
    
    user = User.objects.find_one(name='John Walker')
    print(user.pk, user.name)
    => 
    1 John Walker

Update and delete

    user = User.objects.find_one(name='John Walker')
    user.place = 'New York'
    user.save()

    users = User.objects.find(place='London')
    users.delete()

Store and access any data types, including json and binary values

    class User(Model):
        # in some dbs, unique strings must be limited in length
        username = Column(types.string(length=100), unique=True)
        picture = Column(types.binary)

    user = User.objects.get(name='Dave')
    with open('image.png', 'rb') as fimg:
        user.picture = fimg.read()  
        user.save()

File-like Storage
-----------------

Use the file column type for transparently storing binary data:

    class Image(Model):
        imagefile = Column(types.file)

Usage:

    img = Image()
    with open('/path/to/image') as f:
       img.imagefile.write(f)
    img.save()
    data = img.imagefile.read()

Here the imagefile field provides a file-like API. This is an efficient way to store binary data in the database. The
file's data is split in chunks and written to the database in multiple parts. On reading back, the chunks are retrieved
from the db in parallel, in order to improve performance for large files. Tests indicate a 25% speed up is possible v.v.
a binary field.

You may use the `dataset.files` API to get a filesystem-like API to binary data stored in the database, without the need
to use a model:

    from dataset_orm import files

    connect('sqlite:///test.sqlite')

    files.write('myfile', b'some data')
    files.read('myfile')
    => b'some data'

    files.exists('myfile') 
    => True
    
    files.list()
    => ['myfile']

    files.find('*file*')
    => ['myfile']

    files.remove('myfile')

The convenience methods `put()` and `get()` allow for an even simpler use of the files api:

    files.put(b'some data', 'myfile')
    data = files.get('myfile').read()
    => b'some data'

Using multiple databases
------------------------

Using multiple databases is straight forward:

    from dataset_orm import connect, using, Model, Column, types

    db1 = connect('sqlite:///db1.sqlite')
    db2 = connect('sqlite:///db2.sqlite', alias='other')

    class City(Model):
        name = Column(types.string)

    # this will be saved in db1, since it is the default db (alias='default')
    City(name='London').save()

    # this will be saved in db2, note how it uses other.City instead of City
    with using('other') as other:
        other.City(name='New York').save()

It is also possible to switch db for just one model:

    # this will be saved in db2, note how it uses a different name for the model
    with City.using(db2) as OtherCity:
        OtherCity(name='New York').save()

DBMS Support
------------

* Currently tested against SQLite, SQL Server
* Should work with any DBMS supported by SQLAlchemy
