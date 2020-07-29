Accern-XYME
===========

*accern\_xyme* is a python library for accessing XYME functionality.

|CircleCI|

.. |CircleCI| image:: https://circleci.com/gh/Accern/accern-xyme.svg?style=svg
   :target: https://circleci.com/gh/Accern/accern-xyme

Usage
-----

For XYME legacy information use the latest version before
`v0.1.0 <https://github.com/Accern/accern-xyme/tree/legacy>`_

You can install *accern\_xyme* with pip:

.. code:: sh

    pip install --user accern-xyme

Import it in python via:

.. code:: python

    import accern_xyme

    xyme = accern_xyme.create_xyme_client("https://xyme.accern.com/", "<USERNAME>", "<PASSWORD>")
    print(xyme.get_pipelines())

:code:`<USERNAME>` and :code:`<PASSWORD>` are the login credentials for XYME.
The values can also be set to :code:`None` in which case the values must
be set in the environment variables :code:`ACCERN_USER`
and :code:`ACCERN_PASSWORD`. A login token can also be provided.

You will need python3.6 or later.
