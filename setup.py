#coding:utf-8
"""A setuptools based setup module for FDB package.
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path
from fdb import __version__

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name='fdb',
    version=__version__,
    description='The Python driver for Firebird',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    url='https://github.com/FirebirdSQL/fdb',
    author='Pavel Císař',
    author_email='pcisar@users.sourceforge.net',
    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'Intended Audience :: Developers',

        'License :: OSI Approved :: BSD License',

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',

        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS',

        'Topic :: Database',
],
    keywords='Firebird',  # Optional
    packages=find_packages(),  # Required
    install_requires=['future>=0.16.0'],  # Optional
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, <4',
    test_suite='nose.collector',
    project_urls={
        'Documentation': 'http://fdb2.readthedocs.io/en/latest/',
        'Bug Reports': 'http://tracker.firebirdsql.org/browse/PYFB',
        'Funding': 'https://www.firebirdsql.org/en/donate/',
        'Say Thanks!': 'https://saythanks.io/to/pcisar',
        'Source': 'https://github.com/FirebirdSQL/fdb',
    },
)
