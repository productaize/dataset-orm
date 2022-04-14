from setuptools import setup, find_packages

with open('README.md', 'r') as fin:
    docs = fin.read()

setup(
    name='dataset-orm',
    version='0.2',
    packages=find_packages(exclude=["ez_setup", "examples", "test"]),
    url='',
    license='MIT',
    long_description=docs,
    long_description_content_type='text/markdown',
    author='patrick',
    author_email='patrick@productaize.io',
    description='ORM for the dataset library',
    install_requires=[
        'dataset',
    ],
    extras_require={
        'mssql': [
            'pyodbc'
        ]
    }
)
