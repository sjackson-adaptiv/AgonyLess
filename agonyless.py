#!/usr/bin/env python
import ssh_drv
import ssh_lib
import yaml
import pdb
from cmn_lib import p_trace

__version__ = '0.1'

"""
A tool to interact with Adaptiv Networks 7.X CPEs.
"""


def main():
    """
    :return:  True or False based on the over all result
    """

    # Kick it!
    overall_result = True

    yaml_file = './config.yml'
    with open(yaml_file, 'r') as agony_yml:
        ne_conf = yaml.load(agony_yml, Loader=yaml.FullLoader)

    for ne in ne_conf['network_entities']:

        # Prompt between each CPE
        prompt = input('Are you ready to continue? y to continue:\n')
        if prompt != 'y':
            p_trace('Quitting')
            return False

        # Break out the variables for initial login
        uname = ne_conf['credentials']['uname']
        mp = ne_conf['credentials']['monitor']
        ap = ne_conf['credentials']['admin']
        port = ne_conf['credentials']['port']
        role = ne_conf['credentials']['role']

        # Establish the ssh session & memo the version
        ssh = ssh_drv.SSH()
        if not ssh.open(ne, role, uname, mp, port=port, role=role, monitor_passwd=mp, admin_passwd=ap):
            p_trace('Unable to log into host - game over!', 'ERROR')
            return False

        # Update the passwords
        for user in ['monitor', 'admin']:
            pw_old = ne_conf['credentials'][user]
            pw_new = ne_conf['new_passwords'][user]

            if not ssh_lib.cli_update_password(ssh, user, pw_old, pw_new):
                overall_result = False
                break

        # Test to verify the login before doing the save config
        ssh2 = ssh_drv.SSH()
        mp_test = ssh.monitor_passwd
        ap_test = ssh.admin_passwd
        p_trace('Confirming new usernames and passwords')
        if ssh2.open(ne, role, uname, mp_test, port=port, role=role, monitor_passwd='agni1234', admin_passwd=ap_test):
            if not ssh2.send('admin'):
                overall_result = False
        else:
            overall_result = False

        if overall_result:
            ssh_lib.cli_save_config(ssh)
        else:
            p_trace(f'Aborting due to failure updating password for  user {user} on CPE {ne}', 'ERROR')

    return overall_result


if __name__ == "__main__":

    result = main()
    if result:
        exit(0)
    else:
        exit(1)
