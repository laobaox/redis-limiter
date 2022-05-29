#!/usr/bin/env python
#-*- coding: utf8 -*-

from setuptools import find_packages, setup

VERSION = '0.0.1'

setup(
    name='redis-limiter',
    version=VERSION,
    author='bao xie',
    url='https://github.com/laobaox/redis-limiter.git',
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
