import logging
import os

from imars_etl.util.config_logger import preconfig_logger
preconfig_logger()

# The airflow import weirdness below is to handle the case where the airflow
# config lives on an NFS share managed by autofs.
# Yet another weird and specific bug:
#     AIRFLOW_HOME lives on an NFS share managed via autofs.
#     Start off with the automount not mounted then run `airflow version`.
#     configuration.py tries (and fails) reading the airflow config because
#     autofs isn't able to mount the NFS for however airflow is looking.
#     Airflow tries to create what it thinks is missing and throws an
#     `airflow.exceptions.AirflowConfigException`:
#     "Error creating $AIRFLOW_HOME: Permission denied."
#     Now `ls $AIRFLOW_HOME` and run `airflow version` again.
#     All works as it should because autofs has now mounted the NFS.
try:
    import airflow
except Exception as err:
    # if err is airflow.exceptions.AirflowConfigException
    #   w/o importing airflow:
    prefix = "AirflowConfigException('Error creating "
    suffix = ": Permission denied'"  # ",)"
    if prefix in repr(err):
        airflow_home = repr(err).split(prefix)[1].split(suffix)[0]
        logging.debug(
            "Permission denied on $AIRFLOW_HOME. Ignore this message if your "
            " $AIRFLOW_HOME is an autofs-managed NFS."
        )
        logging.info("stat-ing {}".format(airflow_home))
        os.system("ls {}".format(airflow_home))  # this tells autofs to mount
        import airflow  # noqa F401
    else:
        raise

from imars_etl.api import *  # noqa F401

__version__ = "0.16.1"  # NOTE: this should match version in setup.py
