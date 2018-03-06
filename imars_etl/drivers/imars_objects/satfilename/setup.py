#!/usr/bin/env python
""" setup.py for installing USF IMaRS satfilename module """

from setuptools import setup
import io

import satfilename

def read(*filenames, **kwargs):
    encoding = kwargs.get('encoding', 'utf-8')
    sep = kwargs.get('sep', '\n')
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)

long_description = read('README.md') #, 'CHANGES.txt')

setup(name='satfilename',
    version=satfilename.__version__,
    description='module & CLI for generating satellite product filenames',
    long_description=long_description,
    author='Tylar Murray',
    author_email='imars+satfilename@tylar.info',
    url='https://github.com/USF-IMaRS/satfilename',
    tests_require=['nose'],
    install_requires=[  ],
    # cmdclass={'test': PyTest},
    # entry_points={  # sets up CLI (eg bash) commands
    #     'console_scripts': [
    #         'projectname_cmd_name = projectname.import.path.to.my.cmd:method_name_in_that_module',
    #     ],
    # },
    packages=['satfilename']
)
