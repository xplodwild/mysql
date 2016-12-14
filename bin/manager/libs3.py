""" Module for S3 client wrapper and related tooling. """
import logging
import os
from manager.utils import debug, env, to_flag

# pylint: disable=import-error,dangerous-default-value,invalid-name
import boto as aws
from boto.s3.key import Key

# Manta client barfing if we log the body of binary data
logging.getLogger('manta').setLevel(logging.INFO)

class S3(object):
    """
    The Manta class wraps access to the Manta object store, where we'll put
    our MySQL backups.
    """
    def __init__(self, envs=os.environ):
        self.access = env('AWS_ACCESS_KEY', None, envs)
        self.secret = env('AWS_SECRET_ACCESS_KEY', None, envs)
        self.region = env('AWS_REGION', 'us-east-1', envs)
        self.bucket = env('AWS_BUCKET', None, envs)

        # we don't want to use `env` here because we have a different
        # de-munging to do
        self.client = aws.s3.connect_to_region(self.region,
            aws_access_key_id=self.access,
            aws_secret_access_key=self.secret)
        self.bucket = self.client.get_bucket(self.bucket)

    @debug
    def get_backup(self, backup_id):
        """ Download file from Manta, allowing exceptions to bubble up """
        try:
            os.mkdir('/tmp/backup', 0770)
        except OSError:
            pass

        # where are we storing the backup locally?
        outfile = '/tmp/backup/{}'.format(backup_id)
        k = Key(self.bucket)
        k.key = backup_id
        k.get_contents_to_filename(outfile)

    def put_backup(self, backup_id, infile):
        """ Upload the backup file to the expected path """
        k = Key(self.bucket)
        k.key = backup_id
        k.set_contents_from_filename(infile)
