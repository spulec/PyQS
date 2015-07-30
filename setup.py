#!/usr/bin/env python
import re

from setuptools import setup, find_packages

version = ''
with open('pyqs/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')

setup(
    name='pyqs',
    version=version,
    description='A simple task-queue for SQS.',
    author='Steve Pulec',
    author_email='spulec@gmail',
    url='https://github.com/spulec/pyqs',
    entry_points={
        'console_scripts': [
            'pyqs = pyqs.main:main',
        ],
    },
    packages=find_packages(),
    install_requires=[
        "boto>=2.6",
    ],
)
