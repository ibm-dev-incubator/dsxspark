# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


def write_inventory(output_file, master, nodes):
    output_file.write("""[all:vars]
ansible_connection=ssh
gather_facts=True
gathering=smart
host_key_checking=False
install_java=True
install_temp_dir=/tmp/ansible-install
install_dir=/opt
python_version=2
""")
    output_file.write("""
[master]
%s ansible_host=%s ansible_host_id=1

[nodes]
""" % (master['hostname'], master['ip_addr']))
    for node in nodes:
        output_file.write("%s ansible_host=%s ansible_host_id=%s\n" % (
            node['hostname'], node['ip_addr'], node['node_num']))
