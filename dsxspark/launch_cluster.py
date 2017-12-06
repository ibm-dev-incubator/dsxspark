import copy
import os

from dsxspark import runner

PLAYBOOK_PATH = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'playbooks')
LAUNCH_PLAYBOOK = os.path.join(PLAYBOOK_PATH, 'sl_launch.yaml')


class SparkCluster(object):
    def __init__(self, server_count, cluster_name, cpus=4, memory=16,
                 disk_size=25, domain_name=None, ssh_keys=None):
        self.cluster_name = cluster_name
        self.extra_vars = {
            'disk_size': disk_size,
            'memory': memory,
            'cpus': cpus,
            'ssh_keys': ssh_keys,
        }
        if domain_name:
            self.extra_vars['domain'] = domain_name

    def _launch_worker(self, worker_number):
        hostname = self.cluster_name + '%02d' % worker_number
        worker_vars = copy.deepcopy(self.extra_vars)
        worker_vars['hostname'] = hostname
        runner.run_playbook_subprocess(LAUNCH_PLAYBOOK, worker_vars)

    def _write_node_inventory(self):
        pass

    def _write_master_inventory(self):
        pass
