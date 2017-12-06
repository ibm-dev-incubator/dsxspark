import copy
import os
import tempfile

from dsxspark import runner

PLAYBOOK_PATH = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'playbooks')
LAUNCH_PLAYBOOK = os.path.join(PLAYBOOK_PATH, 'sl_launch.yaml')
DESTROY_PLAYBOOK = os.path.join(PLAYBOOK_PATH, 'sl_destroy.yaml')


class SparkCluster(object):
    def __init__(self, server_count, cluster_name, cpus=4, memory=16,
                 disk_size=25, domain_name=None, ssh_keys=None):
        self.cluster_name = cluster_name
        self.server_count = server_count
        self.extra_vars = {
            'disk_size': disk_size,
            'memory': memory,
            'cpus': cpus,
            'ssh_keys': ssh_keys,
            "cluster_name": self.cluster_name,
        }
        if domain_name:
            self.extra_vars['domain'] = domain_name
        self.inventory_file = os.path.join(tempfile.gettempdir(),
                                           'spark_inv.ini')

    def _launch_worker(self, worker_number):
        hostname = self.cluster_name + '%02d' % worker_number
        worker_vars = copy.deepcopy(self.extra_vars)
        worker_vars['hostname'] = hostname
        runner.run_playbook_subprocess(LAUNCH_PLAYBOOK, worker_vars)

    def _write_node_inventory(self, node_num, ip_addr):
        hostname = self.cluster_name + '%02d' % node_num
        with open(self.inventory_file, 'a') as inv_file:
            inv_file.write(
                "%s ansible_host=%s ansible_host_id=%s" % (
                    hostname, ip_addr, node_num))

    def _write_master_inventory(self, hostname, ip_addr):
        with open(self.inventory_file, 'a') as inv_file:
            inv_file.write("""
[master]
%s ansible_host %s ansible_host_id=1

[nodes]
""")

    def deploy_cluster(self):

        with open(self.inventory_file, 'a') as inv_file:
            inv_file.write("""[all:vars]
ansible_connection=ssh
gather_facts=True
gathering=smart
host_key_checking=False
install_java=True
install_temp_dir=/tmp/ansible-install
install_dir=/opt
python_version=2
""")
        self._launch_worker(1)
        self._write_master_inventory()
        for node in range(2, self.server_count):
            self._launch_worker(node)
            self._write_node_inventory()

    def _delete_worker(self, worker_number):
        hostname = self.cluster_name + '%02d' % worker_number
        worker_vars = copy.deepcopy(self.extra_vars)
        worker_vars['hostname'] = hostname
        runner.run_playbook_subprocess(DESTROY_PLAYBOOK, worker_vars)

    def collapse_cluster(self):
        for node in range(self.server_count):
            self._delete_worker(node)
