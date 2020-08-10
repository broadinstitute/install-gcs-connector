# Copyright (c) 2020 bw2
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import argparse
import glob
import logging
import os
import urllib.request

from pyspark.find_spark_home import _find_spark_home

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO)


GCS_CONNECTOR_URL = 'https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop2-1.9.17/gcs-connector-hadoop2-1.9.17-shaded.jar'

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("-k", "--key-file-path", help="Service account key .json")
    p.add_argument("--gcs-requestor-pays-project", help="If specified, this google cloud project will be charged for access to "
                   "requestor-pays buckets via spark/hadoop. See https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#cloud-storage-requester-pays-feature-configuration")
    args = p.parse_args()
    
    if args.key_file_path and not os.path.isfile(args.key_file_path):
        p.error(f"{args.key_file_path} not found")
        
    if not args.key_file_path:
        # look for existing key files in ~/.config
        key_file_regexps = [
            "~/.config/gcloud/application_default_credentials.json", 
            "~/.config/gcloud/legacy_credentials/*/adc.json",
        ]
        
        # if more than one file matches a glob pattern, select the newest.
        key_file_sort = lambda file_path: -1 * os.path.getctime(file_path)          
        for key_file_regexp in key_file_regexps:
            paths = sorted(glob.glob(os.path.expanduser(key_file_regexp)), key=key_file_sort)
            if paths:            
                args.key_file_path = next(iter(paths))
                logging.info(f"Using key file: {args.key_file_path}")
                break
        else:
            regexps_string = '    '.join(key_file_regexps)
            p.error(f"No json key files found in these locations: \n\n    {regexps_string}\n\n"
                    "Run \n\n  gcloud auth application-default login \n\nThen rerun this script." )
    return args


def is_dataproc_VM():
    """Check if this installation is running on a Dataproc VM"""
    try:
        dataproc_metadata = urllib.request.urlopen("http://metadata.google.internal/0.1/meta-data/attributes/dataproc-bucket").read()
        if dataproc_metadata.decode("UTF-8").startswith("dataproc"):
            return True
    except:
        pass
    
    return False

    
def main():
    if is_dataproc_VM():
        logging.info("This is a Dataproc VM which should already have the GCS cloud connector installed. Exiting...")
        return

    args = parse_args()

    spark_home = _find_spark_home()

    # download GCS connector jar
    local_jar_path = os.path.join(spark_home, "jars", os.path.basename(GCS_CONNECTOR_URL))
    try:
        logging.info(f"Downloading {GCS_CONNECTOR_URL}")
        logging.info(f"   to {local_jar_path}")
        urllib.request.urlretrieve(GCS_CONNECTOR_URL, local_jar_path)
    except Exception as e:
        logging.error(f"Unable to download GCS connector to {local_jar_path}. {e}")
        return

    # update spark-defaults.conf
    spark_config_dir = os.path.join(spark_home, "conf")
    if not os.path.exists(spark_config_dir):
        os.mkdir(spark_config_dir)
    spark_config_file_path = os.path.join(spark_config_dir, "spark-defaults.conf")
    logging.info(f"Updating {spark_config_file_path} json.keyfile")
    logging.info(f"Setting json.keyfile = {args.key_file_path}")
    
    spark_config_lines = [
        "spark.hadoop.google.cloud.auth.service.account.enable true\n",
        f"spark.hadoop.google.cloud.auth.service.account.json.keyfile {args.key_file_path}\n",
    ]
    
    if args.gcs_requestor_pays_project:
        spark_config_lines.extend([
            "spark.hadoop.fs.gs.requester.pays.mode AUTO\n",
            f"spark.hadoop.fs.gs.requester.pays.project.id {args.gcs_requestor_pays_project}\n",
        ])
    
    try:
        # spark hadoop options docs @ https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#cloud-storage-requester-pays-feature-configuration
        if os.path.isfile(spark_config_file_path):
            with open(spark_config_file_path, "rt") as f:
                for line in f:
                    # avoid duplicating options
                    if any([option.split(' ')[0] in line for option in spark_config_lines]):
                        continue

                    spark_config_lines.append(line)

        with open(spark_config_file_path, "wt") as f:
            for line in spark_config_lines:
                f.write(line)

    except Exception as e:
        logging.error(f"Unable to update spark config {spark_config_file_path}. {e}")
        return


if __name__ == "__main__":
    main()
