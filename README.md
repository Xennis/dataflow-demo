# dataflow-demo

## Setup

### Local setup

Requirements
* Python 2.7 is installed
* Google Cloud SDK is installed

Create a virtual environment and install the dependencies
```sh
virtualenv --python python2.7 .venv
. .venv/bin/activate
pip install --requirement requirements.txt
```

Login with the Google Cloud SDK
```sh
gcloud auth login
gcloud auth application-default login
```

### GCP setup

* Create a bucket
* Enable the `dataflow.googleapis.com` API

## Run

```sh
cd customer
python customer.py \
    --runner DataflowRunner \
    --project ${GCP_PROJECT} \
    --region ${GCP_REGION} \
    --temp_location gs://${GCP_BUCKET}/.tmp/ \
    --staging_location gs://${GCP_BUCKET}/.stating \
    --input gs://${GCP_BUCKET}/customer/customer-001.json
```
