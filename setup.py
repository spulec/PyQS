#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import re

from setuptools import setup, find_packages

with open('pyqs/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')


with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('CHANGELOG.rst') as changelog_file:
    changelog = changelog_file.read()

setup(
    name='pyqs',
    version=version,
    description='A simple task-queue for SQS.',
    long_description=readme + '\n\n' + changelog,
    author='Steve Pulec',
    author_email='spulec@gmail.com',
    url='https://github.com/spulec/pyqs',
    entry_points={
        'console_scripts': [
            'pyqs = pyqs.main:main',
        ],
    },
    install_requires=[
        'boto>=2.32.1'
    ],
    packages=[n for n in find_packages() if not n.startswith('tests')],
    include_package_data=True,
)
