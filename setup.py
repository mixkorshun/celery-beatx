from setuptools import setup, find_packages

setup(
    name='celery-beatx',
    version='0.4.1a2',
    url='https://github.com/mixkorshun/celery-beatx',
    description='Modern fail-safe schedule for Celery',
    keywords=['celery', 'celery-beat', 'scheduler'],

    long_description=open('README.rst', 'r').read(),

    author='Vladislav Bakin',
    author_email='mixkorshun@gmail.com',

    license='MIT',

    install_requires=[
        'celery',
    ],

    packages=find_packages(exclude=['beatx.tests.*', 'beatx.tests']),

    classifiers=[
        'Development Status :: 4 - Beta',
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
