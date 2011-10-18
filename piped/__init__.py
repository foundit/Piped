# This is a namespace package.
#
# Setuptools would like us to use:
#
#    import pkg_resources
#    pkg_resources.declare_namespace(__name__)
#
# But trial runs unit tests multiple times using this technique,
# so we use the python built-in method of declaring a namespace
# package instead (http://www.python.org/dev/peps/pep-0382/)

import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)

# See http://www.python.org/dev/peps/pep-0386/ for version numbering, especially NormalizedVersion
from distutils import version
version = version.LooseVersion('0.2.0')