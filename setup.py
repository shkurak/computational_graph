#!/usr/bin/env python

from setuptools import setup

packages = {
    "compgraph": "compgraph",
}

setup(
    name="compgraph",
    version="1.0",
    description="Library for convenient computations of tables",
    author="Ivan Shkurak",
    author_email="shkurakivan@gmail.com",
    packages=packages,
    package_dir=packages
)
