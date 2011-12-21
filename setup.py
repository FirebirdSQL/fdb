#!/usr/bin/env python
"""fdb package is a set of Firebird RDBMS bindings for python.
It works on Python 2.6+ (support for Python 3.x is planned)

"""
from setuptools import setup, find_packages
from fdb import __version__

classifiers = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: BSD License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Database',
]

setup(name='fdb', 
        version=__version__,
        description = 'Firebird RDBMS bindings for Python.', 
        url='http://www.firebirdsql.org/en/python-devel-status/',
        classifiers=classifiers,
        keywords=['Firebird'],
        license='BSD',
        author='Pavel Cisar',
        author_email='pcisar@users.sourceforge.net',
        long_description=__doc__,
    install_requires=[],
    setup_requires=[],
    packages=find_packages(exclude=['ez_setup']),
    test_suite='nose.collector',
    #include_package_data=True,
    package_data={'': ['*.txt'],
                  'test':'fbtest.fdb'},
    #message_extractors={'fdb': [
            #('**.py', 'python', None),
            #('public/**', 'ignore', None)]},
    zip_safe=False,
    entry_points="""
    """,
)
