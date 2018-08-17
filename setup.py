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
    print("Missing README.txt")


def get_resources(source, destination):
    # RETURN list OF PAIRS, EACH OF FORM (<dir name>, list(<files>))
    # SEE http://docs.python.org/2/distutils/setupscript.html#installing-additional-files
    output = []
    files = []
    for name in os.listdir(source):
        source_child = "/".join([source, name])
        dest_child = "/".join([destination, name])
        if os.path.isdir(source_child):
            output.extend(get_resources(source=source_child, destination=dest_child))
        elif os.path.isfile(source_child):
            files.append(source_child)
    output.append((destination, files))
    return output


setup(
    name='Bugzilla-ETL',
    version="2.0.13353",
    description='Mozilla Bugzilla Bug Version ETL',
    long_description=long_desc,
    author='Kyle Lahnakoski',
    author_email='kyle@lahnakoski.com',
    url='https://github.com/mozilla/Bugzilla-ETL',
    license='MPL 2.0',
    packages=['bugzilla_etl'],
    install_requires=['pymysql', 'requests', 'pytest'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "console_scripts": [
            "bzetl = bugzilla_etl.bz_etl:start",
            "bzreplicate = bz.ETL.replicate:start"
        ]
    },
    classifiers=[  # https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 4 - Beta",
        "Topic :: Utilities",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
    ],
    data_files=get_resources(source="resources", destination="resources")
)
