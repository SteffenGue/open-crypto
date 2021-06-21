#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Setup module for installing the Crypto Data Collector including its dependencies.
"""

from setuptools import setup, find_packages

with open("README.md", "r") as file:
    long_description = file.read()

setup(
    name="crypto-stguen",
    version="0.0.9",
    author="Steffen Guenther",
    author_email="crypto@uni-bremen.de",
    long_description=long_description,
    url="https://gitlab.informatik.uni-bremen.de/fiwi-crypto/crypto_high_frequency",
    long_description_content_type="text/markdown",
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=[
        "aiohttp",
        "aiohttp",
		"certifi",
        "aioschedule",
        "datetime_periods",
        "numpy != 1.19.4",
        "oyaml",
        "pandas",
        "psycopg2-binary",
        "mariadb",
        "pymysql",
        "pytest",
        "python-dateutil",
        "sqlalchemy",
        "sqlalchemy_utils",
        "tqdm"
    ]
)
