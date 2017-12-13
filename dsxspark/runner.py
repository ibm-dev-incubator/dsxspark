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

import subprocess

from dsxspark import exceptions


def handle_error(stdout, stderr, return_code):
    instance_error_msg = '"msg": "Error in creating instance'
    if instance_error_msg in stdout:
        raise exceptions.InstanceCreateException()


def run_playbook_subprocess(playbook, extra_vars=None, inventory=None):
    cmd = ['ansible-playbook', playbook]
    if inventory:
        cmd.extend(['-i', inventory])
    if extra_vars:
        extra_vars_string = ""
        for var in extra_vars:
            extra_vars_string += "%s='%s' " % (var, extra_vars[var])
        extra_vars_string = extra_vars_string.rstrip()
        cmd.extend(['--extra-vars', extra_vars_string])
    cmd.extend(['--timeout', '25'])
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode > 0:
        handle_error(stdout, stderr, proc.returncode)
        print("ERROR: Playbook %s failed with:\n\tstderr:\n\t\t%s\n"
              "\tstdout:\n\t\t%s" % (playbook, stderr, stdout))
        raise exceptions.PlaybookFailure
