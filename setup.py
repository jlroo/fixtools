# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='fixtools',
    version='2.0' ,
    description='Package to analyze FIX 5.0 SP2 financial data.',
    long_description=readme,
    author='Jose Luis Rodriguez',
    author_email='jrodriguezorjuela@luc.edu',
    url='https://bitbucket.com/jrodriguezorjuela/fixtools',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)
