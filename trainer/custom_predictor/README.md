# Custom Predictors

- - - 

This section describes how to upload custom predictiors models to AI Platform.

First, the scripts have to be packaged in order to be deployed to GCS. To do so, just run:

    python setup.py sdist --formats=gztar

A new package in tar.gz format is built in folder `./dist`.

Using `gsutil`, copy the folder `dist` to a desired GCS bucket.
