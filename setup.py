#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Setup module for installing the Crypto Data Collector including its dependencies.
"""

from setuptools import setup, find_packages

with open("README.md", "r") as file:
    long_description = file.read()

setup(
    name="open-crypto",
    version="3.9.9.1",
    author="Steffen Guenther",
    author_email="crypto@uni-bremen.de",
    long_description=long_description,
    url="https://github.com/SteffenGue/open_crypto",
    long_description_content_type="text/markdown",
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    install_requires=[
        "aiohttp",
        "aioschedule",
        "certifi",
        "datetime_periods",
        "matplotlib",
        "numpy != 1.19.4",
        "oyaml",
        "pandas",
        "pytest",
        "python-dateutil",
        "sqlalchemy >= 1.4.22",
        "sqlalchemy_utils >= 0.37.8",
        "tqdm",
        "validators",
        "nest_asyncio",
        "typeguard >= 2.12.1",
        "colorama",
    ]
)

