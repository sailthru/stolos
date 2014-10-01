try:
    from distutils.core import setup  # , Extension
    from setuptools import find_packages, findall
except ImportError:
    print ("Please install Distutils and setuptools"
           " before installing this package")
    raise

setup(
    name='scheduler',
    version='1.0.0',
    description=('A DAG-based job scheduler for performing work with complex'
                 ' dependency requirements.'),
    long_description="Check the project homepage for details",
    keywords=['scheduler', 'dag', 'directed acyclic graph', 'graph', 'data',
              'dependency'],

    author='Sailthru Data Science Team',
    author_email='agaudio@sailthru.com',
    url='https://github.com/sailthru/scheduler',

    packages=find_packages(),
    scripts=['./bin/scheduler-submit'],
    data_files=[
        ('conf', findall('conf')),
        ('scheduler/examples', findall('scheduler/examples'))
    ],

    install_requires = [
        'kazoo>=1.3.1',
        'networkx>=1.8.1',
        'ujson>=1.33',
        'argparse>=1.1',
        'pygraphviz>=1.2',
        'argparse_tools>=1.0.0',
    ],

    tests_require=[
        'nose>=1.3.3',
        'colorlog>=2.2.0',
        'pyflakes>=0.8.1',
        'pep8>=1.5.6',
    ],
    test_suite="nose.main",

    extras_require={
        'pyspark': ['pyspark'],
        'redis': ['redis', 'hiredis']
    },

    # include_package_data = True,
    zip_safe = False,

    # Include code that isn't pure python (like c stuff)
    # ext_modules=[Extension('foo', ['foo.c'])]

    entry_points = {
        'console_scripts': [
            'scheduler = scheduler.__main__:go',
        ],
        'setuptools.installation': [
            'eggsecutable = scheduler.__main__:go',
        ],
    },
)
