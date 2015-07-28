#!/usr/bin/env python
import os
import pip
from setuptools import setup, find_packages
from pip.req import parse_requirements
from subprocess import call


install_reqs = parse_requirements("requirements.txt",
                                  session=pip.download.PipSession())
# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]

setup(version='0.1.0',
      name='concord_py_test',
      description='python client testing utilities',
      depencency_links= [
          'https://github.com/concord/concord-py@1c16926cbd842f35f4103ce58b48d19e230cc0cb#egg=concord_py-master'

      ],
      author='concord systems',
      author_email='hi@concord.io',
      packages=find_packages(),
      url='http://concord.io',
      install_requires=reqs,
      test_suite="tests"
)
