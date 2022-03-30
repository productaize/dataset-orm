from setuptools import setup, find_packages

setup(
    name='dataset-orm',
    version='0.1',
    packages=find_packages(exclude=["ez_setup", "examples", "test"]),
    url='',
    license='MIT',
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
