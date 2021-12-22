# -*- coding: utf-8 -*-
__author__ = 'arkilic'
try:
    from setuptools import setup, find_packages
except ImportError:
    try:
        from setuptools.core import setup
    except ImportError:
        from distutils.core import setup

import versioneer
import os


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='analysisstore',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Prototype analysis results database (for the MX beamlines)",
    long_description=readme,
    install_requires=requirements,
    license="BSD (3-clause)",
    url="https://github.com/NSLS-II/analysisstore.git",
    packages=find_packages(),
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 3',
    ],
)
