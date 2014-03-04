#!/usr/bin/env python

import os
import sys

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

__author__ = 'Adam Miskiewicz <adam@bolsterlabs.com>'
__version__ = '0.1.0'

packages = [
    'celery_pipelines',
]

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

setup(
    name='celery-pipelines',
    version=__version__,
    install_requires=[
        'celery>=3.0.24',
    ],
    author='Adam Miskiewicz',
    author_email='adam@bolsterlabs.com',
    license=open('LICENSE').read(),
    url='https://github.com/bolster/celery-pipelines/tree/master',
    keywords='celery chord group pipeline',
    description='Pipelines for Celery',
    long_description=open('README.rst').read() + '\n\n' + open('HISTORY.rst').read(),
    include_package_data=True,
    packages=find_packages(exclude=['tests', 'tests.*']),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet'
    ]
)
