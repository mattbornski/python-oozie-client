#!/usr/bin/env python

import setuptools

setuptools.setup(
    name='oozie',
    version='0.1.0',
    author='Matt Bornski',
    author_email='matt@bornski.com',
    license='GPLv3',
    url='https://github.com/mattbornski/python-oozie-client',
    
    install_requires=[
        'requests',
    ],
    packages=setuptools.find_packages(),
    scripts=[
        'bin/oozie-echo',
        'bin/oozie-healthcheck',
    ],
)