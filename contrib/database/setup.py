# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import os
import sys

from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))

# add ourselves to the package path so we can get the version from the source tree
sys.path.insert(0, here)
import piped.contrib.database

setup(
    name = 'piped.contrib.database',
    license = 'MIT',

    author = 'Piped Project Contributors',
    author_email = 'piped@librelist.com',
    url = 'http://piped.io',

    packages = find_packages(where=here),
    namespace_packages = ['piped', 'piped.contrib'],

    version = str(piped.contrib.database.version),
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Environment :: Plugins',
        'Framework :: Twisted',
        'Operating System :: OS Independent',
        'Topic :: Database'
    ],
    description = 'Database provider for Piped.',

    tests_require = ['psycopg2'],

    install_requires = ['piped', 'sqlalchemy', 'mocker', 'setuptools']
)