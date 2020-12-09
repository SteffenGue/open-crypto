# -*- coding: utf-8 -*-

"""
Setup module for installing dependencies of Crypto Data Collector.

"""

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name="crypto-stguen",
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
      python_requires='>=3.6',
      install_requires=['sqlalchemy',
                        'sqlalchemy_utils',
                        # 'PyYAML',
                        'aiohttp',
                        'psycopg2-binary',
                        'aioschedule',
                        'aiohttp',
                        'tqdm',
                        'numpy < 1.19.4',
                        'python-dateutil',
                        'pandas',
                        'oyaml']
      )
