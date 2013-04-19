#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='pyqs',
    version='0.0.1',
    description='A simple task-queue for SQS.',
    author='Steve Pulec',
    author_email='spulec@gmail',
    url='https://github.com/spulec/pyqs',
    entry_points={
        'console_scripts': [
            'pyqs = pyqs.worker:main',
        ],
    },
    packages=find_packages(),
    install_requires=[
        "boto>=2.6",
    ],
)
