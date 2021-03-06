### Overview
The [GCS connector](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage) allows [hail](https://hail.is/docs/0.2/utils/index.html)/spark/hadoop to read/write Google Storage files directly.

It can be [tricky to install](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/INSTALL.md), so this package automates these installation steps:

```
1. downloads the gcs connector .jar file to $SPARK_HOME/jars/ 
2. finds your gcloud .json key file 
   in ~/.config/gcloud/application_default_credentials.json (created by running gcloud auth application-default login) 
   or in ~/.config/gcloud/legacy_credentials/*/adc.json (created by gcloud auth login) 
3. updates your $SPARK_HOME/conf/spark-defaults.conf to add the key file path
```


### Install GCS Connector

Make sure you have `pyspark` or `hail` installed and are are logged in to to gcloud via  

`gcloud auth login`  or  `gcloud auth application-default login`


Then, to add the GCS connector to your pyspark installation, run:
```
curl https://raw.githubusercontent.com/broadinstitute/install-gcs-connector/master/install_gcs_connector.py | python3
```


### Enable access to Requester Pays buckets

Google Bucket owners can enable ['Requester Pays'](https://cloud.google.com/storage/docs/requester-pays) on their buckets so that any person downloading the data, rather than the bucket owners, will pay Google for [network egress costs](https://cloud.google.com/compute/network-pricing).

To use the GCS connector to access requester pays buckets, you should run the installer with the  --gcs-requester-pays-project arg:

```
wget https://raw.githubusercontent.com/broadinstitute/install-gcs-connector/master/install_gcs_connector.py 

python3 install_gcs_connector.py --gcs-requester-pays-project <your-gcloud-project>
```

## Test

You can check if it's working by running hail hadoop_exists:

```
python3 -m pip install --upgrade hail
python3 -c 'import hail as hl; print(hl.hadoop_exists("gs://gcp-public-data-landsat/index.csv.gz"))'
```

This should print `True` at the end.




