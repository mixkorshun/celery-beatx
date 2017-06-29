celery-beatx
============

.. image:: https://travis-ci.org/mixkorshun/celery-beatx.svg?branch=master
   :alt: build status
   :target: https://travis-ci.org/mixkorshun/celery-beatx
.. image:: https://codecov.io/gh/mixkorshun/celery-beatx/branch/master/graph/badge.svg
   :alt: codecov
   :target: https://codecov.io/gh/mixkorshun/celery-beatx
.. image:: https://badge.fury.io/py/celery-beatx.svg
   :alt: pypi
   :target: https://pypi.python.org/pypi/celery-beatx
.. image:: https://img.shields.io/badge/code%20style-pep8-orange.svg
   :alt: pep8
   :target: https://www.python.org/dev/peps/pep-0008/
.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
   :alt: MIT
   :target: https://opensource.org/licenses/MIT

BeatX is celery plugin which improve celery beat functionality.

Features:
  - store scheduler data in various storages
  - support cluster scheduler running

See the documentation_ for more details.

Installation
------------

The package can be installed using::

    pip install celery-beatx

Add following settings to celery configuration::

    CELERY_BEAT_SCHEDULER = 'beatx.schedulers.Scheduler'
    # or
    # CELERY_BEAT_SCHEDULER = 'beatx.schedulers.ClusterScheduler'

    CELERY_BEAT_STORE = "redis://127.0.0.1:6379/0"


Contributing
------------

If you have any valuable contribution, suggestion or idea,
please let us know as well because we will look into it.

Pull requests are welcome too.


.. _documentation: https://celery-beatx.readthedocs.io/