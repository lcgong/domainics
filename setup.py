#! /usr/bin/python3
# -*- coding: utf-8 -*- 

from distutils.core import setup
from setuptools import find_packages


setup(
    name='dpillars',
    version='0.1',
    license="BSD",
    description='RESTful Framework with domain object',
    author='Chenggong Lyu',
    author_email='lcgong@gmail.com',
    url='https://github.com/lcgong/dpillars',
    packages=find_packages("src"),
    package_dir = {"": "src"},
    zip_safe = False,
    classifiers = [
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Utilities",
        ],      
    )
