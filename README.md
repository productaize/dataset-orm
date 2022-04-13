Fast Active Record ORM for the dataset library
==============================================

Why?
----

The dataset library is a great and easy tool to work with any SQL database.
Unfortunately, it lacks an object mapper (ORM) - if you need one you are 
left with the complexity that is sqlalchemy. 

Enter dataset-orm

How?
----

Installation

    $ pip install dataset-orm

Define classes that define a dataset.Table:

    from dataset_orm import Model, connect

    class User(Model):
        # in some dbs, unique strings must be limited in length
        username = Column(types.string(length=100), unique=True)
        data = Column(types.json)

    connect('sqlite:///mydb.sqlite') 

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
        
Use the file column type for transparently storing binary data:


    class Image(Model):
        imagefile = Column(types.file)


Usage:

    img = Image()
    with open('/path/to/image') as f:
       img.imagefile.write(f)
    img.save()
    data = img.imagefile.read()


The imagefile field is the filename of a dataset_orm.files.DatasetFile. The file's data 
is split in chunks and written to the database in multiple parts. On reading back, the 
chunks are retrieved from the db in parallel, in order to improve performance for large 
files. Tests indicate a 25% speed up is possible v.v. a single BLOB. 
