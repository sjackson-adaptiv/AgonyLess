import datetime
import time
import re
from cmn_lib import p_trace


"""
This library is meant to be used in conjunction with ssh_drv instances.
The functions contained within are performed on the remote agnonyOS host over
SSH. This is in contrast to methods contained within cmn_lib, which
are performed on the local host.
"""


def cli_nav(fh_ssh, cli_node):
    """
    Navigate to the correct user level and node in the CLI hierarchy to issue a specific command.
    :param fh_ssh: SSH session created via previous call to SSH.open
    :param cli_node: The string that matches which node to navigate to in order to execute
                     a given command.
    :return: True or False if the requested node could be navigated to
    """

    # Exit diag level if required
    if 'root@' in fh_ssh.prompt:
        fh_ssh.send('exit')
    # Enter admin level if required
    if fh_ssh.prompt.endswith('>'):
        fh_ssh.send('admin')

    crnt_node = fh_ssh.prompt.split(f'{fh_ssh.sys_name}-')[1][:-1]
    if cli_node in crnt_node:
        return True
    else:
        # Drop to base cli node
        if 'Admin' not in crnt_node:
            fh_ssh.send('exit')

        if cli_node != 'Admin':
            result = fh_ssh.send(cli_node)
            if not result[0]:
                p_trace(f'Unable to navigate to requested cli node {cli_node} - {result[1]}', 'ERROR')
                return False

    return True


def cli_get_ver(fh_ssh):
    """
    Obtains the system version running on the Adaptiv NE
    :param fh_ssh: SSH session created via previous call to SSH.open
    :return: version - a string matching the vesrion string obtained by a show version
    """
    cli_nav(fh_ssh, 'system')
    output = fh_ssh.send('show version')
    version = output[1][0]
    output = fh_ssh.send('show uptime')
    uptime = output[1][0]
    p_trace(f"{fh_ssh.sys_name} is running:  '{version}'  / System uptime: {uptime}")
    return version


def cli_get_resp(fh_ssh, cli_node, cli_cmd, obj_names):
    """
    A generic helper function that executes a given agni cli show cmd,
    parses the response and returns the data as a dictionary.

    Only a limited subset of commands is supported because the CLI output is not consistent.
    This function relies on parsing based on the object name followed by :

    :param fh_ssh: SSH session created via previous call to SSH.open
    :param cli_node: The node under which the command is located
    :param cli_cmd: The cli command to be executed
    :param obj_names: The full list of object names returned by the command.
                      This list is used to build up the dict, as well as screen scrape delimiter.
    :return: output_parsed - A dict of objects and values based the cmd output,
             or False in case of error
    """
    if not cli_nav(fh_ssh, cli_node):
        p_trace(f'Unable to navigate to requested CLI node - {cli_node}', 'ERROR')
        return False
    output_parsed = {}
    output = fh_ssh.send(cli_cmd, True)

    for line in output[1]:
        if line.startswith('---'):
            continue
        # Remove square brackets and white space
        line = re.sub(r'([\[\]])', ' ', line)
        line = line.strip()
        for obj in obj_names:
            obj_key = obj
            if line.startswith(obj):
                obj_data = []
                update = True
                obj_names.pop(0)
                break
            else:
                update = False
                break

        # Escape brackets () for regexp, otherwise entire line used as data
        obj = re.sub(r'([\(\)])', r'\\\1', obj)
        m = re.search(f'(^{obj} *:)(.*)', line)
        if m:
            # A match implies the start of a new object
            value = m.group(m.lastindex)
        else:
            # No match means data continued to the next line
            value = re.sub(r'(^: )', '', line)

        if len(value) > 0:
            obj_data.append(value.strip())
        if update:
            output_parsed.update({obj_key: obj_data})

    return output_parsed


def cli_get_profile_names(fh_ssh, cli_node):
    """
    Helper function that returns a list of profile names from the cmd "show profile all"
    :param fh_ssh: SSH session created via previous call to SSH.open
    :param cli_node: The node under which the command is located
    :return: output_parsed -A dict of objects and values based the cmd output
    """

    if not cli_nav(fh_ssh, cli_node):
        p_trace(f'Unable to navigate to requested CLI node - {cli_node}', 'ERROR')
        return False

    profiles = fh_ssh.send('show profile all')
    return profiles[1]


def cli_get_ana2_tunnel(fh_ssh):
    """
    Performs a call to get_cli_data, and returns the dict generated by that call
    from the cmd "show profile ANA."
    :param fh_ssh: SSH session created via previous call to SSH.open
    :return: output_parsed -A dict of objects and values based the cmd output
    """
    obj_names = ['Profile Name', 'Status', 'Link1', 'Link2', 'Username', 'Password',
                 'Keepalive', 'Link-detection', 'QoE-check', 'IPDE-QoE', 'Compress', 'VJCOMP', 'Tcpmss',
                 'Frag-seq-sync', 'DNS', 'Mtu', 'Route(s)', 'Interface', 'NAT', 'Failsafe', 'Latency',
                 'Lmtu', 'Lmru', 'Lmrru', 'MDPS', 'QoE-MDPS', 'Weight', 'Bandwidth', 'ANA-Int-Filter',
                 'APD-Bypass', 'IPDE-LJA', 'MINIDEQUEUE', 'FRAG-TIMER', 'RLA-Bandwidth', 'RLA-Reserve',
                 'RLA-Bypass', 'RLA-On-demand', 'IPDE-QUEUE', 'Log(s)']

    profiles = cli_get_profile_names(fh_ssh, 'ana2-client')
    p_name = profiles[0]
    output_parsed = cli_get_resp(fh_ssh, 'ana2-client', f'show profile {p_name}', obj_names)
    return output_parsed


def cli_get_dhcp_prof(fh_ssh, link_id):
    """
    Performs a call to show profile dhcp <link_id>, and returns the dict generated by that call
    :param fh_ssh: SSH session created via previous call to SSH.open
    :param link_id: The dhcp client link identifier. Eg. dhcp-link1 or dhcp-link2
    :return: output_parsed -A dict of objects and values based the cmd output
    """
    cmd = f'show profile {link_id}'
    obj_names = ['Profile Name', 'Status', 'Interface', 'Hostname', 'DNS', 'DDNS',
                 'Default-gateway', 'Loopback', 'Hostname-lookup', 'Routes']

    output_parsed = cli_get_resp(fh_ssh, 'dhcp-client', cmd, obj_names)
    return output_parsed


def cli_get_ana2_server(fh_ssh):
    """
    Performs a call to get_cli_data, and returns the dict generated by that call
    from the cmd "show profile-ana2-server <profile_name>".
    <profile_name> is obtained by the command "show profile all".
    :param fh_ssh: SSH session created via previous call to SSH.open
    :return: output_parsed -A dict of objects and values based the cmd output
    """
    profiles = cli_get_profile_names(fh_ssh, 'ana2-server')
    p_name = profiles[0]

    obj_names = ['Profile', 'Type', 'Bundlesize', 'Version', 'Status', 'Keepalive', 'Authentication',
                 'Pool(s)', 'Link1', 'Link2', 'Link3', 'Link4', 'Link5', 'Link6', 'Link7', 'Link8',
                 'DNS', 'Proxy', 'Protocomp', 'Mtu', 'Mrru', 'Radius', 'Log(s)']

    output_parsed = cli_get_resp(fh_ssh, 'ana2-server', f'show profile {p_name}', obj_names)
    return output_parsed


def cli_get_underlay_info(fh_ssh):
    """
    Helper function to return the number of ISP underlays
    :param fh_ssh: SSH session created via previous call to SSH.open
    :return: An int representing the number of ISP underlays
    """
    if not 'root@' in fh_ssh.prompt:
        fh_ssh.send('/diag')
    response = fh_ssh.send('cat /etc/ana2/ana2.conf | grep self')
    if response[0]:
        underlays = response[1]
        return len(set(underlays))


def cli_update_password(fh_ssh, uname, passwd_old, passwd_new):
    """
    Helper function to update the admin password
    :param fh_ssh: SSH session created via previous call to SSH.open
    :param uname: The user to update
    :param passwd_old: The existing password
    :param passwd_new: The password to be set
    :return: True or False based on success
    """
    cli_nav(fh_ssh, 'system')
    output = fh_ssh.send(f'set password {uname} {passwd_old} {passwd_new}')

    if not output[0] or not output[1]:
        p_trace(f'Something unexpected happened - {output[1]}', 'ERROR')
        return False
    else:
        fh_ssh.uname = passwd_new

    return output[0]


def cli_save_config(fh_ssh):
    cli_nav(fh_ssh, 'Admin')
    fh_ssh.send(f'save config all')
    return True


def calibrate_links(fh_ssh, aux_srv='192.168.110.2'):
    """
    Perform the manual step of calibrating the underlay links based on the
    output of the calibrate debug-qoe cli cmd.
    :param fh_ssh: SSH session created via previous call to SSH.open()
    :param aux_srv: The IP address of the server hosting nuttcp
    :return: True or False based on success

    sh /usr/local/cli/scripts/lib.ana2c.sh set ANA amos link{0} both {1} debug
    """

    cli_nav(fh_ssh, 'ana2-client')
    cmd = f'set profile-ana2-client ANA calibrate {aux_srv} debug-qoe'
    p_trace('Be patient for approx 5 min, performing manual ana2-client link calibration')

    bw_cmds = []
    rla_cmds = []
    link = False
    direction = False

    response = fh_ssh.send(cmd)
    for line in response[1]:
        m_max = re.search(r'(Max )(upload|download)( bandwidth established for )(link\d)( = )(\d+)( Kbps)', line)
        if m_max:
            link = m_max.group(4)
            max_bps = int(m_max.group(6)) * 1000
            direction = 'in'
            if m_max.group(2) == 'upload':
                direction = 'out'
            bw_cmds.append(f'set profile-ana2-client ANA bandwidth {link} {max_bps} {direction}')
        m_rla = re.search(r'(\d+)( Kbps)( +)(95%)( +)(\d+\.\d+%)', line)
        if m_rla:
            rlx_bps = int(m_rla.group(1)) * 1000
            # loss = float(m_rla.group(6))
            # if str(num_links) in link:
            rla_cmds.append(f'set profile-ana2-client ANA ipde-rla bandwidth {link} {rlx_bps} {direction}')

    for cmd in bw_cmds:
        # bw_resp = fh_ssh.send(cmd)
        print(cmd)

    for cmd in rla_cmds:
        # rla_resp = fh_ssh.send(cmd)
        print(cmd)
