import subprocess

from dsxspark import exceptions


def handle_error(stdout, stderr, return_code):
    instance_error_msg = '"msg": "Error in creating instance'
    if instance_error_msg in stdout:
        raise exceptions.InstanceCreateException()


def run_playbook_subprocess(playbook, extra_vars):
    extra_vars_string = ""
    for var in extra_vars:
        extra_vars_string += "%s='%s' " % (var, extra_vars[var])
    extra_vars_string = extra_vars_string.rstrip()
    cmd = ['ansible-playbook', playbook, '--extra-vars', extra_vars_string]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode > 0:
        handle_error(stdout, stderr, proc.returncode)
        print("ERROR: Playbook %s failed with:\n\tstderr:\n\t\t%s\n"
              "\tstdout:\n\t\t%s" % (playbook, stderr, stdout))
        raise exceptions.PlaybookFailure
