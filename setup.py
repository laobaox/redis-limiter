#!/usr/bin/env python
#-*- coding: utf8 -*-

from setuptools import find_packages, setup

VERSION = '0.0.3'

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='redis-limiter',
    version=VERSION,
    author='bao xie',
    description='A Redis based rate limiter implementation for Python',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/laobaox/redis-limiter',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points = {
        'console_scripts': [
        ],
    },
    python_requires='>=2.7.9',
    install_requires=[
    ],
)
