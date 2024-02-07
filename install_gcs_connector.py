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
from typing import Tuple, Union
import argparse
import glob
import logging
import os
import urllib.request
import xml.etree.ElementTree as ET

from pyspark.find_spark_home import _find_spark_home
from pyspark.version import __version__ as spark_version_string

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO)


def spark_version() -> Tuple[int, int, int]:
    split = spark_version_string.split('.')
    if len(split) != 3:
        raise ValueError(spark_version_string)
    try:
        return tuple(int(x) for x in split)
    except ValueError as err:
        raise ValueError(spark_version_string) from err


THE_SPARK_VERSION = spark_version()


def parse_connector_version(version) -> Tuple[int, int, int, int, Union[int, float]]:
    """Parse a connector version string into a tuple (hadoop version, major version, minor version, patch version, release candidate)."""

    parts = version.split('-')

    try:
        release_candidate = None
        if parts[0].startswith('hadoop'):
            hadoop_version = int(parts[0][6:])
            jar_version = parts[1]
            if len(parts) > 2:
                assert parts[2].startswith('RC')
                release_candidate = int(parts[2][2:])
        elif len(parts) > 1 and parts[1].startswith('hadoop'):
            jar_version = parts[0]
            hadoop_version = int(parts[1][6:])
            if len(parts) > 2:
                assert parts[2].startswith('RC'), version
                release_candidate = int(parts[2][2:])
        else:
            jar_version = parts[0]
            hadoop_version = 3  # starting with version 3.0.0, no hadoop version is present
            if len(parts) > 1:
                assert parts[1].startswith('RC'), version
                release_candidate = int(parts[1][2:])

        major_jar_version, minor_jar_version, patch_jar_version = map(int, jar_version.split('.'))
    except ValueError as err:
        raise ValueError(f'unexpected version string: {version} {jar_version}') from err

    return (hadoop_version, major_jar_version, minor_jar_version, patch_jar_version, release_candidate or float("inf"), version)


def get_gcs_connector_url():
    """Get the URL of the jar file for the latest version of the Hadoop connector."""
    with urllib.request.urlopen("https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/maven-metadata.xml") as f:
        metadata = f.read().decode("utf-8")

    versions = [parse_connector_version(el.text) for el in ET.fromstring(metadata).findall("./versioning/versions/version")]
    if spark_version() < (3, 5, 0):
        hadoop2_versions = [v for v in versions if v[0] == 2]
        latest_version = sorted(hadoop2_versions, key=lambda x: x[1:5])[-1]
    else:
        hadoop3_versions = [v for v in versions if v[0] == 3]
        latest_version = sorted(hadoop3_versions, key=lambda x: x[1:5])[-1]

    latest_version_string = latest_version[-1]
    return f"https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/{latest_version_string}/gcs-connector-{latest_version_string}-shaded.jar"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("-a", "--auth-type",
                   help="How to authenticate. For Spark <3.5.0, this option must be unspecified. For Spark >=3.5.0, use --auth-type APPLICATION_DEFAULT for your laptop. Use --auth-type COMPUTE_ENGINE for GCE VMs. Use SERVICE_ACCOUNT_JSON_KEYFILE with --key-file-path for an explicit key file location. For Spark >=3.5.0, we default to APPLICATION_DEFAULT.")
    p.add_argument("-k", "--key-file-path",
                   help="Required for Spark <3.5.0. Service account key .json path. This path is just added to the spark config file. The .json file itself doesn't need to exist until the GCS connector is first used.")
    p.add_argument("--gcs-requester-pays-project", "--gcs-requestor-pays-project", help="If specified, this google cloud project will be charged for access to "
                   "requester pays buckets via spark/hadoop. See https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#cloud-storage-requester-pays-feature-configuration")
    args = p.parse_args()

    if args.key_file_path and not os.path.isfile(args.key_file_path):
        logging.warning(f"{args.key_file_path} file doesn't exist")


    if THE_SPARK_VERSION >= (3, 5, 0):
        if args.key_file_path is not None and args.auth_type != "SERVICE_ACCOUNT_JSON_KEYFILE":
            raise ValueError(f"In Spark >=3.5.0, when --key-file-path is specified, --auth-type must be SERVICE_ACCOUNT_JSON_KEYFILE")
        if args.auth_type is None:
            args.auth_type = "APPLICATION_DEFAULT"

        valid_auth_types = {
            "ACCESS_TOKEN_PROVIDER",
            "APPLICATION_DEFAULT",
            "COMPUTE_ENGINE",
            "SERVICE_ACCOUNT_JSON_KEYFILE",
            "UNAUTHENTICATED",
            "USER_CREDENTIALS"
        }
        if args.auth_type not in valid_auth_types:
            raise ValueError(f'--auth-type must be one of {" ".join(valid_auth_types)}, found: {args.auth_type}')
    else:
        if args.auth_type is not None:
            raise ValueError(f"--auth-type cannot be used with Spark <3.5.0. We think you have spark {THE_SPARK_VERSION}.")
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
                        "Run \n\n  gcloud auth application-default login \n\nthen rerun this script, "
                        "or use --key-file-path to specify where the key file exists (or will exist later).\n")

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
    try:
        gcs_connector_url = get_gcs_connector_url()
    except Exception as e:
        logging.error(f"get_gcs_connector_url() failed: {e}")
        return

    local_jar_path = os.path.join(spark_home, "jars", os.path.basename(gcs_connector_url))
    logging.info(f"Downloading {gcs_connector_url}")
    logging.info(f"   to {local_jar_path}")

    try:
        urllib.request.urlretrieve(gcs_connector_url, local_jar_path)
    except Exception as e:
        logging.error(f"Unable to download GCS connector to {local_jar_path}. {e}")
        return

    # update spark-defaults.conf
    spark_config_dir = os.path.join(spark_home, "conf")
    if not os.path.exists(spark_config_dir):
        os.mkdir(spark_config_dir)
    spark_config_file_path = os.path.join(spark_config_dir, "spark-defaults.conf")


    if THE_SPARK_VERSION >= (3, 5, 0):
        logging.info(f"Updating {spark_config_file_path} fs.gs.auth.type")
        logging.info(f"Setting fs.gs.auth.type = {args.auth_type}")
        spark_config_lines = [
            f"spark.hadoop.fs.gs.auth.type {args.auth_type}\n",
        ]
        if args.key_file_path:
            spark_config_lines = [
                f"spark.hadoop.fs.gs.auth.service.account.json.keyfile {args.key_file_path}\n",
            ]
    else:
        logging.info(f"Updating {spark_config_file_path} json.keyfile")
        logging.info(f"Setting json.keyfile = {args.key_file_path}")

        spark_config_lines = [
            "spark.hadoop.google.cloud.auth.service.account.enable true\n",
            f"spark.hadoop.google.cloud.auth.service.account.json.keyfile {args.key_file_path}\n",
        ]

    if args.gcs_requester_pays_project:
        spark_config_lines.extend([
            "spark.hadoop.fs.gs.requester.pays.mode AUTO\n",
            f"spark.hadoop.fs.gs.requester.pays.project.id {args.gcs_requester_pays_project}\n",
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
