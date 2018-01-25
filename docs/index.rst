Welcome to celery-beatx's documentation!
========================================

BeatX is modern fail-safe schedule for Celery.

BeatX allows you store schedule in different storages and
provides functionality to start celery-beat simultaneously at many nodes.


Install
-------

The package can be installed using::

    pip install celery-beatx

After package installed you should set celery scheduler::

   celery_app.config_from_object({
      # ...
      'beat_scheduler': 'beatx.schedulers.Scheduler',
      'beat_store': 'redis://127.0.0.1:6379/',
      # ...
   })

It works same as default celery scheduler, but stores schedule in redis storage.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
