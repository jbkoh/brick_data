from distutils.core import setup

__author__ = 'Jason Koh for Brick'
__version__ = '0.01'
setup(
    name = 'sparqlwrapper-brick',
    version = __version__,
    author = __author__,
    packages = ['sparqlwrapper_brick'],
    package_dir={'': '../'},
    install_requires = ['setuptools', 'requests', 'SPARQLWrapper'],
)
