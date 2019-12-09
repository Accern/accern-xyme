Accern-XYME
===========

*accern\_xyme* is a python library for accessing XYME functionality.

Usage
-----

You can install *accern\_xyme* with pip:

.. code:: sh

    pip install --user accern-xyme

Import it in python via:

.. code:: python

    from accern_xyme import create_xyme_client

    client = accern_xyme.create_xyme_client(
        "https://xyme.accern.com/", "<USERNAME>", "<PASSWORD>")
    print(client.get_user_info())

:code:`<USERNAME>` and :code:`<PASSWORD>` are the login credentials for XYME.
The values can also be set to :code:`None` in which case the values must
be set in the environment variables :code:`ACCERN_USER`
and :code:`ACCERN_PASSWORD`.

You will need python3.6 or later.
