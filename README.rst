Accern-XYME
===========

*accern\_xyme* is a python/typescript library for accessing XYME functionality.

|GHAction|

.. |GHAction| image:: https://github.com/Accern/accern-xyme/actions/workflows/python-app.yaml/badge.svg
   :target: https://github.com/Accern/accern-xyme/actions/workflows/python-app.yaml/

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
