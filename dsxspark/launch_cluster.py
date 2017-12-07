import copy
import os
import tempfile
import threading

import SoftLayer as sl

from dsxspark import runner

PLAYBOOK_PATH = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'playbooks')
LAUNCH_PLAYBOOK = os.path.join(PLAYBOOK_PATH, 'sl_launch.yml')
DESTROY_PLAYBOOK = os.path.join(PLAYBOOK_PATH, 'sl_destroy.yml')
PREPARE_PLAYBOOK = os.path.join(PLAYBOOK_PATH, 'sl_prepare_node.yml')
SPARK_DEPLOY_DIR = os.path.join(
    os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
    'spark-cluster-install')


class SLSparkCluster(object):
    def __init__(self, server_count, cluster_name, cpus=4, memory=16384,
                 disk_size=25, domain_name='spark.test', datacenter='dal10',
                 ssh_keys=None):
        """Deploy a spark cluster on softlayer

        :param str hostname: The base hostname to use for all nodes. Must not
            contain any special characters, except for '.' and '-'.
        :param str domain_name: The domain name to use for all clusters. Must
            be in the form 'Domain.TLD' and no special characters can be used
            except for '.' and '-'
        :param int cpus: The number of cpus to use for the nodes
        :param int memory: An integer representing the memory size in MB to
            use for the launched VMs. Must be one of: 1024, 2048, 4096,
            6144, 8192, 12288, 16384, 32768, 49152, 65536, 131072, 247808
        :param str datacenter: A string for which data center to launch the
            nodes in. Valid choices are ams01, ams03, che01, dal01, dal05,
            dal06, dal09, dal10, fra02, hkg02, hou02, lon02, mel01, mex01,
            mil01, mon01, osl01, par01, sjc01, sjc03, sao01, sea01, sng01,
            syd01, tok02, tor01, wdc01, wdc04
        :param list ssh_keys: A list of numeric ids for each ssh key to use
            on the server.

        """
        self.cluster_name = cluster_name
        self.server_count = server_count
        self.datacenter = datacenter
        ssh_keys = ssh_keys or []
        self.extra_vars = {
            'disk_size': disk_size,
            'memory': memory,
            'cpus': cpus,
            'ssh_keys': ','.join(ssh_keys),
            "cluster_name": self.cluster_name,
            "domain": domain_name,
            'datacenter': self.datacenter,
        }
        self.inventory_file = os.path.join(
            tempfile.gettempdir(), 'spark_inv_%s.ini' % self.cluster_name)
        self.launcher_treads = []
        self.inventory_lock = threading.Lock()

    def _get_ip_addr(self, hostname):
        sl_client = sl.create_client_from_env()
        vs = sl.VSManager(sl_client)
        server = vs.list_instances(hostname=hostname)[0]
        return server['primaryIpAddress']

    def _launch_worker(self, worker_number, master=None):
        hostname = self.cluster_name + '%02d' % worker_number
        worker_vars = copy.deepcopy(self.extra_vars)
        worker_vars['hostname'] = hostname
        runner.run_playbook_subprocess(LAUNCH_PLAYBOOK, worker_vars)
        ip_addr = self._get_ip_addr(hostname)
        if master:
            self._write_master_inventory(hostname, ip_addr)
        else:
            self._write_node_inventory(hostname, ip_addr, worker_number)

    def _write_node_inventory(self, hostname, ip_addr, node_num):
        with self.inventory_lock:
            with open(self.inventory_file, 'a') as inv_file:
                inv_file.write(
                    "%s ansible_host=%s ansible_host_id=%s\n" % (
                        hostname, ip_addr, node_num))

    def _write_master_inventory(self, hostname, ip_addr):
        with self.inventory_lock:
            self.master_ip = ip_addr
            with open(self.inventory_file, 'a') as inv_file:
                inv_file.write("""
[master]
%s ansible_host=%s ansible_host_id=1

[nodes]
""" % (hostname, ip_addr))

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
        worker = threading.Thread(target=self._launch_worker, args=(1),
                                  kwargs={'master': True})
        for node in range(2, self.server_count + 1):
            worker_thread = threading.Thread(target=self._launch_worker,
                                             args=(node))
            self.launcher_treads.append(worker_thread)
        for worker in self.launcher_treads:
            worker.join()
        self.launcher_treads = []
        runner.run_playbook_subprocess(PREPARE_PLAYBOOK,
                                       inventory=self.inventory_file)
        cwd = os.getcwd()
        os.chdir(SPARK_DEPLOY_DIR)
        runner.run_playbook_subprocess('./setup-spark-standalone.yml',
                                       inventory=self.inventory_file)
        os.chdir(cwd)

    def _delete_worker(self, worker_number):
        hostname = self.cluster_name + '%02d' % worker_number
        worker_vars = copy.deepcopy(self.extra_vars)
        worker_vars['hostname'] = hostname
        runner.run_playbook_subprocess(DESTROY_PLAYBOOK, worker_vars)

    def collapse_cluster(self):
        for node in range(self.server_count + 1):
            self._delete_worker(node)
