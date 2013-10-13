import os

from setuptools import setup


root = os.path.abspath(os.path.dirname(__file__))
path = lambda *p: os.path.join(root, *p)


setup(
    name='bzETL',
    version=0.2,
    description='Mozilla Bugzilla Bug Version ETL',
    long_description=open(path('README.md')).read(),
    author='Kyle Lahnakoski',
    author_email='kyle@lahnakoski.com',
    url='https://github.com/klahnakoski/Bugzilla-ETL',
    license='MPL 2.0',
    packages=['bzETL'],
    install_requires=['pymysql', 'requests'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "console_scripts":[
            "bzetl = bzETL.bz_etl:start",
            "bzreplicate = bz.ETL.bzReplicate:start"
        ]
    }

)
