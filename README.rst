.. image:: https://raw.githubusercontent.com/mixkorshun/celery-beatx/master/docs/_static/logo-small.png
   :alt: Celery BeatX

|build-status| |coverage| |version| |pep8| |license|

Celery-BeatX is a modern fail-safe schedule for Celery.

Celery-BeatX allows you to store schedule in different storages and
provides functionality to start celery-beat simultaneously at many nodes.

Features:
 * stores schedule in different storages (currently support: redis, memcached)
 * allows to correctly run several instances of celerybeat simultaneously

See the documentation_ for more details.

Install
-------

The package can be installed using::

    pip install celery-beatx

After package installed you should set celery scheduler::

   celery_app.config_from_object({
      # ...
      'beat_scheduler': 'beatx.schedulers.BeatXScheduler',
      'beatx_store': 'redis://127.0.0.1:6379/',
      # ...
   })


Config using celeryconfig.py obj::

    CELERYBEAT_SCHEDULER = 'beatx.schedulers.BeatXScheduler'
    BEATX_STORE = 'redis://127.0.0.1:6379/

Contributing
------------

If you have any valuable contribution, suggestion or idea,
please let us know as well because we will look into it.

Pull requests are welcome too.


.. _documentation: https://celery-beatx.readthedocs.io/


.. |build-status| image:: https://travis-ci.org/mixkorshun/celery-beatx.svg?branch=master
    :alt: build status
    :target: https://travis-ci.org/mixkorshun/celery-beatx

.. |coverage| image:: https://codecov.io/gh/mixkorshun/celery-beatx/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/mixkorshun/celery-beatx

.. |license| image:: https://img.shields.io/badge/License-MIT-yellow.svg
    :alt: MIT
    :target: https://opensource.org/licenses/MIT

.. |version| image:: https://badge.fury.io/py/celery-beatx.svg
    :alt: pypi
    :target: https://pypi.python.org/pypi/celery-beatx

.. |pep8| image:: https://img.shields.io/badge/code%20style-pep8-orange.svg
    :target: https://www.python.org/dev/peps/pep-0008/
