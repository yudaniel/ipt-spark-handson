#!/usr/bin/env bash

# Make sure to login with "gcloud auth login"
# Use your own credentials

# Upload your jar to Google Storage
gsutil cp spark-demo.jar gs://dataproc-51de3174-ccbb-43cb-bcb9-c6d7454de224-europe-west6

# Run with Dataproc
# Note: modify class name and jar name
gcloud dataproc jobs submit spark \
--cluster cluster-b7c2 \
--region europe-west6 \
--class ch.ipt.handson.SimpleLocalExample \
--jars gs://dataproc-51de3174-ccbb-43cb-bcb9-c6d7454de224-europe-west6/spark-demo.jar