Accern-XYME
===========

*accern\_xyme* is a python/typescript library for accessing XYME functionality.

|CircleCI|

.. |CircleCI| image:: https://circleci.com/gh/Accern/accern-xyme.svg?style=svg
   :target: https://circleci.com/gh/Accern/accern-xyme

Python Usage
------------

You can install *accern\_xyme* with pip:

.. code:: sh

    pip install --user accern-xyme

Import it in python via:

.. code:: python

    import accern_xyme

    xyme = accern_xyme.create_xyme_client(
        "<URL>",
        token="<TOKEN>",
        namespace="default")
    print(xyme.get_dags())

:code:`<URL>` and :code:`<TOKEN>` are the login credentials for XYME.

You will need python3.6 or later.


Typescript Developing
---------------------

You can install dependency with :code:`yarn`. Run :code:`yarn _postinstall`
to configure husky pre-commit hooks for your local environment.
