#!/usr/bin/env python
""" imars-etl setup.py """

from setuptools import setup
import io

#import imars_etl
# NOTE: can't do that here before dependencies installed, silly.
VERSION='0.1.0'  # should match __version__ in imars_etl.__init__.py

def read(*filenames, **kwargs):
    encoding = kwargs.get('encoding', 'utf-8')
    sep = kwargs.get('sep', '\n')
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)

long_description = read('README.md') #, 'CHANGES.txt')

setup(name='imars_etl',
    version=imars_etl.__version__,
    description='Interface for IMaRS ETL operations',
    long_description=long_description,
    author='Tylar Murray',
    author_email='code+imars_etl@tylar.info',
    url='https://github.com/usf-imars/imars-etl',
    tests_require=['nose'],
    install_requires=['pymysql', 'parse'],
    # entry_points={  # sets up CLI (eg bash) commands
    #     'console_scripts': [
    #         'imars-etl = imars-etl',
    #     ],
    # },
    #cmdclass={'test': PyTest},  # custom build commands for test/lint/etc
    packages=[  # modules that are added to python when this is installed
        'imars_etl'
    ]
)
