# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import os
import sys

from setuptools import setup, find_packages

# When pip installs anything from packages, py_modules, or ext_modules that
# includes a twistd plugin (which are installed to twisted/plugins/),
# setuptools/distribute writes a Package.egg-info/top_level.txt that includes
# "twisted".  If you later uninstall Package with `pip uninstall Package`,
# pip removes all of twisted/ instead of just Package's twistd plugins.  See
# https://github.com/pypa/pip/issues/355
#
# To work around this problem, we monkeypatch
# setuptools.command.egg_info.write_toplevel_names to not write the line
# "twisted".  This fixes the behavior of `pip uninstall Package`.  Note that
# even with this workaround, `pip uninstall Package` still correctly uninstalls
# Package's twistd plugins from twisted/plugins/, since pip also uses
# Package.egg-info/installed-files.txt to determine what to uninstall,
# and the paths to the plugin files are indeed listed in installed-files.txt.
try:
    from setuptools.command import egg_info
    egg_info.write_toplevel_names
except (ImportError, AttributeError):
    pass
else:
    def _top_level_package(name):
        return name.split('.', 1)[0]

    def _hacked_write_toplevel_names(cmd, basename, filename):
        pkgs = dict.fromkeys(
            [_top_level_package(k)
                for k in cmd.distribution.iter_distribution_names()
                if _top_level_package(k) != 'piped'
            ]
        )
        cmd.write_file("top-level names", filename, '\n'.join(pkgs) + '\n')

    egg_info.write_toplevel_names = _hacked_write_toplevel_names


here = os.path.abspath(os.path.dirname(__file__))

# add ourselves to the package path so we can get the version from the source tree
sys.path.insert(0, here)
import piped_cyclone


packages = find_packages(where=here) + ['piped.plugins']
package_data = {
    'piped_cyclone': ['templates/debugger.html']
}

# sdist completely ignores package_data, so we generate a MANIFEST.in
# http://docs.python.org/distutils/sourcedist.html#specifying-the-files-to-distribute
with open(os.path.join(here, 'MANIFEST.in'), 'w') as manifest:
    for package, files in package_data.items():
        for file in files:
            manifest.write('include %s \n'%os.path.join(package.replace('.', os.path.sep), file))

setup(
    name = 'piped_cyclone',
    license = 'MIT',

    author = 'Piped Project Contributors',
    author_email = 'piped@librelist.com',
    url = 'http://piped.io',

    packages = packages,
    include_package_data = True,
    package_data = package_data,

    version = str(piped_cyclone.version),
    classifiers = [
        'Development Status :: 4 - Beta',
        'Environment :: Plugins',
        'Framework :: Twisted',
        'Operating System :: OS Independent'
    ],
    description = 'Cyclone provider for Piped.',

    install_requires = ['piped', 'cyclone', 'setuptools']
)