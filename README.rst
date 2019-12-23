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
and :code:`ACCERN_PASSWORD`. A login token can also be provided.

You will need python3.6 or later.

Exploring Workspaces
--------------------

The workspaces of the user can be retrieved via:

.. code:: python

    for (workspace, count) in client.get_workspaces().items():
        print(f"{workspace} contains {count} jobs"

And jobs in a given workspace can be retrieved via:

.. code:: python

    for job in client.get_jobs(workspace):
        print(f"{job.get_job_id()}: {job.get_name()} - {job.get_status()}"

Or directly by Job ID:

.. code:: python

    job = client.get_job_id("username_example_com/job_id")

Starting Jobs
-------------

A new job can be started via:

.. code:: python

    # creating the job
    job = client.create_job(schema=schema_obj, name="my job")

    with job.update_schema() as cur:
        # updating the schema
        cur["M"]["params"]["hidden_layer_sizes"] = [100, 100, 100]

    # starting the job
    job.start()

    import time
    time.sleep(30)

    print(job.get_status())

Computing Predictions
---------------------

Predictions can be obtained for a finished or running job:

.. code:: python

    # predict_proba is also available
    predictions, stdout = job.predict(df)
    print(stdout)

    print("prediction of first row: ", predictions.iloc[0])
