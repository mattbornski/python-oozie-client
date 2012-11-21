#!/usr/bin/env python

import setuptools

setuptools.setup(name='oozie',
      version='0.1.0',
      packages=setuptools.find_packages(),
      author='Matt Bornski',
      author_email='matt@bornski.com',
      license='GPLv3',
      scripts=['bin/oozie-echo', 'bin/oozie-healthcheck'],
      url='https://github.com/mattbornski/python-oozie-client')