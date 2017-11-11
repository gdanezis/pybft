from setuptools import setup


setup(name='pybft',
      version="0.1",
      description='An implementation of the pBFT protocols.',
      author='George Danezis',
      author_email='g.danezis@ucl.ac.uk',
      url=r'https://pypi.python.org/pypi/pybft/',
      packages=['pybft'],
      license="LGPL",
      long_description=" ... ",
      setup_requires=["pytest >= 2.6.4"],
      tests_require = ["pytest >= 2.5.0"],
      install_requires=["pytest >= 2.5.0"],
)