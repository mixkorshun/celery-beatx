from setuptools import setup, find_packages

setup(
    name='celery-beatx',
    version='0.3.0',
    url='https://github.com/mixkorshun/celery-beatx',
    description='Adds scheduler and cluster-scheduler for celery',
    keywords=['celery', 'celerybeat', 'scheduler'],

    long_description=open('README.rst', 'r').read(),

    author='Vladislav Bakin',
    author_email='mixkorshun@gmail.com',
    maintainer='Vladislav Bakin',
    maintainer_email='mixkorshun@gmail.com',

    license='MIT',
    setup_requires=[
        'pytest-runner',
    ],

    install_requires=[
        'celery',
    ],

    packages=find_packages(exclude=['tests.*', 'tests']),

    test_suite='tests',
    test_requires=['pytest', 'ephem', 'redis'],

    classifiers=[
        # 'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
