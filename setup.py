#! /usr/bin/python3
# -*- coding: utf-8 -*-

from distutils.core import setup
from setuptools import find_packages

import os
import sys
from setuptools.command.test import test as TestCommand

class PyTest(TestCommand):
    test_package_name = 'MyMainPackage'

    def finalize_options(self):
        TestCommand.finalize_options(self)
        _test_args = [
            '--verbose',
            '--ignore=build',
            '--cov={0}'.format(self.test_package_name),
            '--cov-report=term-missing',
            # '--pep8',
        ]
        extra_args = os.environ.get('PYTEST_EXTRA_ARGS')
        if extra_args is not None:
            _test_args.extend(extra_args.split())
        self.test_args = _test_args
        self.test_suite = True

    def run_tests(self):
        import pytest
        from pkg_resources import normalize_path, _namespace_packages

        errno = pytest.main(self.test_args)
        sys.exit(errno)

setup(
    name='domainics',
    version='0.3.1',
    license="BSD",
    description='Web Framwork with Domain-orieted architecure',
    author='Chenggong Lyu',
    author_email='lcgong@gmail.com',
    url='https://github.com/lcgong/domainics',
    packages=find_packages("src"),
    package_dir = {"": "src"},
    zip_safe = False,
    install_requires = ["tornado>=4.2",
                        "psycopg2>=2.6.1",
                        "arrow>=0.6"],
    classifiers = [
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Utilities",
        ],
    test_suite='tests',
    tests_require=[
        'pytest',
        'pytest-pep8',
        'pytest-cov',
        ],
    cmdclass={'test': PyTest},

    )
