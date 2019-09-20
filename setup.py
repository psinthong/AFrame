import setuptools
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='aframe',
      version='0.0.1',
      description='AsterixDB DataFrame',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url='http://github.com/psinthon/AFrame',
      author='Gift Sinthong',
      author_email='psinthon@uci.edu',
      install_requires=[
          'pandas',
          'numpy',
      ],
      license='MIT',
      packages=setuptools.find_packages(),
      zip_safe=False)