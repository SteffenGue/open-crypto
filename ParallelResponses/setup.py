# -*- coding: utf-8 -*-

"""
Setup module for installing dependencies of Crypto Data Collector.

"""

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

CONFIG = {
    "name": "Crypto Data Collector",
    "version": "0.9",
    "url": "https://gitlab.informatik.uni-bremen.de/fiwi-crypto/crypto_high_frequency",
    "python_requires": ">=3.7",
    # "test_suite": "discover_tests"
}

setup(**CONFIG, install_requires=['sqlalchemy',
                                  'sqlalchemy_utils',
                                  'PyYAML',
                                  'aiohttp',
                                  'psycopg2-binary',
                                  'aioschedule',
                                  'aiohttp',
                                  'tqdm',
                                  'python-dateutil',
                                  'pandas'])
