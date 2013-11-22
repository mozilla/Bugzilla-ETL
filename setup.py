# encoding: utf-8
#
import os

from setuptools import setup

root = os.path.abspath(os.path.dirname(__file__))
path = lambda *p: os.path.join(root, *p)
try:
    long_desc = open(path('README.txt')).read()
except Exception:
    long_desc = "<Missing README.txt>"
    print "Missing README.txt"

setup(
    name='bzETL',
    version='0.3.0',
    description='Mozilla Bugzilla Bug Version ETL',
    long_description=long_desc,
    author='Kyle Lahnakoski',
    author_email='kyle@lahnakoski.com',
    url='https://github.com/klahnakoski/Bugzilla-ETL',
    license='MPL 2.0',
    packages=['bzETL'],
    install_requires=['pymysql', 'requests', 'pytest'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "console_scripts":[
            "bzetl = bzETL.bz_etl:start",
            "bzreplicate = bz.ETL.replicate:start"
        ]
    },
    classifiers=[  #https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 4 - Beta",
        "Topic :: Utilities",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
    ]


)
