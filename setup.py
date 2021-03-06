#!/usr/bin/env python
from __future__ import absolute_import
from setuptools import setup

long_description = open("README.md", "r").read()
long_description = long_description.replace(":\n", "::\n")  # lame md -> rst

setup(
  name="s3repo",
  description="A tool to manage simple Debian repositories in S3",
  long_description=long_description,
  license="BSD",
  version="1.3",
  url="http://github.com/mayerathome/s3repo",
  author="Ross Kinder",
  author_email="ross+czNyZXBv@kndr.org",
  package_dir={'': 'src'},
  packages=['s3repo'],
  entry_points={
    "console_scripts": [
      "s3repo = s3repo.command:Main",
    ],
  },
  requires=["six", "boto"],
)
