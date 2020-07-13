# -*- coding: utf-8 -*-

"""
Setup module for installing dependencies of crypto_apis.

Authors:
    Martin Schorfmann,
    Fabian Peter,
    Steffen Günther

Since:
    24.07.2018

Version:
    13.07.2020
"""

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

CONFIG = {
    "name": "Crypto Data Collector",
    "version": "1.0",
    "url": "https://gitlab.informatik.uni-bremen.de/fiwi-crypto/crypto_high_frequency",
    "python_requires": ">=3.7",
    # "test_suite": "discover_tests"
}

setup(**CONFIG, install_requires=['sqlalchemy',
                                  'PyYAML',
                                  'aioschedule'])
