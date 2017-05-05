""" Module for SCP (SSH copy) and related tooling. """
import logging
import os
from manager.utils import debug, env, to_flag

from paramiko import SSHClient
from scp import SCPClient

# Manta client barfing if we log the body of binary data
logging.getLogger('manta').setLevel(logging.INFO)

class SCP(object):
    """
    The SCP class wraps access to a remote SSH storage server, where we'll put
    our MySQL backups.
    """
    def __init__(self, envs=os.environ):
        self.host = env('SCP_HOST', None, envs)
        self.path = env('SCP_PATH', '/srv/backups', envs)

        self.ssh = SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.connect(self.host)

    @debug
    def get_backup(self, backup_id):
        """ Download file from server, allowing exceptions to bubble up """
        try:
            os.mkdir('/tmp/backup', 0770)
        except OSError:
            pass

        # where are we storing the backup locally?
        outfile = '/tmp/backup/{}'.format(backup_id)
        remotefile = '{}/{}'.format(self.path, backup_id)
	scp = SCPClient(self.ssh.get_transport())
        scp.get(remotefile, outfile)
        scp.close()

    def put_backup(self, backup_id, infile):
        """ Upload the backup file to the expected path """
        remotefile = '{}/{}'.format(self.path, backup_id)
        scp = SCPClient(self.ssh.get_transport())
        scp.put(infile, remotefile)
        return('{}:{}/{}'.format(self.host, self.path, backup_id))
