# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import os
import sys

from setuptools import setup, find_packages

# add piped to the package path so we can get the version from the source tree
here = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, here)
import piped


packages = find_packages(where=here)
package_data = {
    'piped': ['conf.yml', 'service.tac'],
    'piped.test': [
        'data/bar.txt', 'data/foo.txt', 'data/test_conf.yml',
        'data/test_includes.yml', 'data/baz/bar.baz', 'data/baz/foo.bar',
        'data/test_config_nesting.yml'
    ],
    'piped.providers.test': [
        'data/bar', 'data/foo',
        'data/persisted_json.json', 'data/persisted_pickle.pickle',
        'data/server.crt', 'data/server.key'
    ]
}


# sdist completely ignores package_data, so we generate a MANIFEST.in
# http://docs.python.org/distutils/sourcedist.html#specifying-the-files-to-distribute
with open(os.path.join(here, 'MANIFEST.in'), 'w') as manifest:
    for package, files in package_data.items():
        for file in files:
            manifest.write('include %s \n'%os.path.join(package.replace('.', os.path.sep), file))

    manifest.write('include LICENSE\n')
    manifest.write('include README.rst\n')


version_specific_requirements = []
if sys.version_info < (2, 7):
    version_specific_requirements += ['ordereddict']


setup(
    name = 'piped',
    license = 'MIT',

    author = 'Piped Project Contributors',
    author_email = 'piped@librelist.com',
    url = 'http://piped.io',

    packages = packages,
    package_data = package_data,
    zip_safe = False,

    version = str(piped.version),
    classifiers = [
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Operating System :: OS Independent',
        'Framework :: Twisted',
        'Topic :: Software Development :: Libraries :: Application Frameworks'
    ],
    description = 'A framework and application server with pipelines.',
    long_description = """
        See http://piped.io for more details.
    """,

    entry_points = dict(
        console_scripts = [
            'piped = piped.scripts:run_piped',
        ]
    ),

    install_requires = version_specific_requirements +
                     ['Twisted>=11', 'argparse', 'pyOpenSSL', 'PyYAML', 'networkx>=1.4', 'setuptools', 'mock']
)
