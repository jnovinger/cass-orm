import os
from setuptools import setup

from cass import __version__

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "cass-orm",
    version = __version__,
    author = "Lusana Ali",
    author_email = "https://github.com/krone",
    description = ("Cassandra ORM layer for Django apps."),
    license = "Unknown",
    keywords = "cassandra pycassa django orm",
    url = "https://github.com/krone/cass-orm",
    packages = [
        'cass',
        'cass.models',
        'cass.fields',
        'cass.query',
        'cass.utils'
    ],
    install_requires=['pycassa', 'thrift', 'django', 'dictshield'],
    long_description=read('README.md'),
    classifiers=[
       "Framework :: Django",
       "Programming Language :: Python",
       "Programming Language :: Python :: 2.7",
       "Topic :: Database",
       "Topic :: Database :: Database Engines/Servers",
       "Topic :: Internet",
       "Topic :: Utilities",
       "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)