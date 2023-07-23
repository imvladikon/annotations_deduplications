#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import importlib
import os

import setuptools

__package_name__ = 'annotations_deduplications'
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


def get_version(package_name: str):
    filename = package_name + "/version.py"
    spec = importlib.util.spec_from_file_location(__package_name__ + ".version", filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.__version__


__version__ = get_version(__package_name__)


def read_requirements():
    """Parses requirements from requirements.txt"""
    reqs_path = os.path.join(__location__, 'requirements.txt')
    with open(reqs_path, encoding='utf8') as f:
        reqs = [line.strip() for line in f if not line.strip().startswith('#')]
    return {'install_requires': reqs}


def read_readme():
    with open(os.path.join(__location__, 'README.md'), 'r') as f:
        long_description = f.read()
    return long_description


setuptools.setup(name=__package_name__,
                 version=__version__,
                 author='Vladimir Gurevich',
                 description='Package to perform simple deduplication',
                 long_description=read_readme(),
                 long_description_content_type='text/markdown',
                 url='https://github.com/imvladikon/annotations_deduplications', # noqa
                 packages=setuptools.find_packages(exclude=(
                     'tests',
                     'tests.*',
                 )),
                 classifiers=[
                     'Programming Language :: Python :: 3',
                     'Topic :: Scientific/Engineering'
                 ],
                 python_requires='>=3.8',
                 package_dir={__package_name__: __package_name__},
                 **read_requirements())
