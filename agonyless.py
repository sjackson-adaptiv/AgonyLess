#!/usr/bin/env python
import ssh_drv
import ssh_lib
import yaml
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
        agony_yml.close()

    for ne in ne_conf['network_entities']:

        # Prompt between each CPE
        prompt = input('Are you ready to continue? y to continue:\n')
        if prompt != 'y':
            p_trace('Quitting')
            return False

        # Break out the variables to login
        uname = ne_conf['credentials']['uname']
        mp = ne_conf['credentials']['monitor_passwd']
        ap = ne_conf['credentials']['admin_passwd']
        port = ne_conf['credentials']['port']
        role = ne_conf['credentials']['role']

        # Establish the ssh session & memo the version
        ssh = ssh_drv.SSH()
        ssh.open(ne, role, uname, mp, port=port, role=role, monitor_passwd=mp, admin_passwd=ap)
        ssh_lib.cli_get_ver(ssh)

        # Update the passwords
        mpn = ne_conf['new_passwords']['monitor_passwd_new']
        apn = ne_conf['new_passwords']['admin_passwd_new']
        ssh_lib.cli_update_password(ssh, 'monitor', mp, mpn)
        ssh_lib.cli_update_password(ssh, 'admin', ap, apn)
        ssh_lib.cli_save_config(ssh)

    return overall_result


if __name__ == "__main__":

    result = main()
    if result:
        exit(0)
    else:
        exit(1)
