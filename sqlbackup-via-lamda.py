#!/usr/local/bin/python
""" Labda function to export mysql data and send via ftp """

import base64
import os
from datetime import datetime
import logging
import boto3
import pymssql

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel('DEBUG')  # Set to DEBUG to see details in lambda logs
DATE_STR = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
AWS_DATA = {'region': os.environ.get('LAMBDA_KMS_REGION')}
ENV_DATA = {
    'LAMBDA_RDS_HOST': os.environ.get('LAMBDA_RDS_HOST'),
    'LAMBDA_RDS_USER': os.environ.get('LAMBDA_RDS_USER'),
    'LAMBDA_RDS_PORT': os.environ.get('LAMBDA_RDS_PORT'),
    'LAMBDA_RDS_S3_DST': os.environ.get('LAMBDA_RDS_S3_DST'),
    'LAMBDA_RDS_DBS': os.environ.get('LAMBDA_RDS_DBS'),
    'LAMBDA_RDS_PASS_PARAM': os.environ.get('LAMBDA_RDS_PASS_PARAM')
}


def get_enc_param_val(param_name):
    """ Returns a decrypted vaule of a given parameter """

    ssm_client = boto3.client('ssm')
    print 'looking up %s' % param_name
    response = ssm_client.get_parameter(
        Name=param_name,
        WithDecryption=True
    )
    return response["Parameter"]["Value"]


def get_param_val(param_name):
    """ Returns a decrypted vaule of a given parameter """

    ssm_client = boto3.client('ssm')
    print 'looking up %s' % param_name
    response = ssm_client.get_parameter(
        Name=param_name
    )
    return response["Parameter"]["Value"]


def rds_backup_database(db_name):
    """ call stored proc for given db_name """

    rds_host = ENV_DATA['LAMBDA_RDS_HOST']
    rds_user = ENV_DATA['LAMBDA_RDS_USER']
    rds_pass = get_enc_param_val(ENV_DATA['LAMBDA_RDS_PASS_PARAM'])

    LOGGER.info('Connecting to %s as %s', rds_host, rds_user)

    with pymssql.connect(rds_host, rds_user, rds_pass, "master") as conn:
        with conn.cursor() as cursor:
            # stored_proc = "exec msdb.dbo.rds_backup_database \
            # @source_db_name='%s', \
            # @s3_arn_to_backup_to='arn:aws:s3:::%s/DR/%s.bak', \
            # @overwrite_S3_backup_file=1;" % (db_name, ENV_DATA['LAMBDA_RDS_S3_DST'], db_name)
            #
            # LOGGER.info('Query(%s)', stored_proc)

            # cursor.execute(stored_proc)
            cursor.callproc('msdb.dbo.rds_backup_database',
                            (db_name,
                             "arn:aws:s3:::%s/DR/%s.bak" % (ENV_DATA['LAMBDA_RDS_S3_DST'], db_name),
                             '1'))
             conn.commit()    
            for row in cursor:
                print "Row=" + row


def lambda_handler(event, context):
    """ lambda entrypoint """

    for db_name in str(ENV_DATA['LAMBDA_RDS_DBS']).split(','):
        rds_backup_database(db_name)


if __name__ == "__main__":
    """ Entrypoint so this can be tested on the cli """

    CONTEXT = {}
    EVENT = {}

    lambda_handler(EVENT, CONTEXT)
