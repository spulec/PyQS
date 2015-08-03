#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os
import re

from setuptools import setup, find_packages

version = ''
with open('pyqs/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')


def parse_requirements():
    """Rudimentary parser for the `requirements.txt` file

    We just want to separate regular packages from links to pass them to the
    `install_requires` and `dependency_links` params of the `setup()`
    function properly.
    """
    try:
        requirements = \
            map(str.strip, local_file('requirements.txt').splitlines())
    except IOError:
        raise RuntimeError("Couldn't find the `requirements.txt' file :(")

    links = []
    pkgs = []
    for req in requirements:
        if not req:
            continue
        if 'http:' in req or 'https:' in req:
            links.append(req)
            name, version = re.findall("\#egg=([^\-]+)-(.+$)", req)[0]
            pkgs.append('{0}=={1}'.format(name, version))
        else:
            pkgs.append(req)

    return pkgs, links


local_file = lambda f: \
    open(os.path.join(os.path.dirname(__file__), f)).read()


install_requires, dependency_links = parse_requirements()

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
    install_requires=install_requires,
    dependency_links=dependency_links,
    packages=filter(lambda n: not n.startswith('tests'), find_packages()),
    include_package_data=True,
)
