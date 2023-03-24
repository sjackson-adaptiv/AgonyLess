import csv
import logging
import os
import pdb
import re
import shutil
import sys
import tarfile
from time import gmtime, strftime
import time
import yaml
from copy import deepcopy
from itertools import product

# Private libs
sys.path.append('../lib')
import cmn_lib
import ssh_drv
import ssh_lib
from iperf3 import iperf3
from rdp import rdp

root_logger = logging.getLogger('root')


def comp_tp(config_file):
    """
    Compiles the test plan by parsing the config.yml file and stripping
    out any sub keys not required for this series of sweeps.
    :param config_file: The yaml based configuration file
    :return: config_db - A dictionary based the stripped config.yml file or False on error

    """
    config_db = cmn_lib.yml_to_dict(config_file)

    if not config_db:
        return False

    # Remove systems that are not required
    for system in list(config_db['resource_config'].keys()):
        if not config_db['resource_config'][system]['required']:
            config_db['resource_config'].pop(system)

    # Remove system commands that are not required
    for execution_block in list(config_db['commands'].keys()):
        for system_key in list(config_db['commands'][execution_block].keys()):
            for tc_key in list(config_db['commands'][execution_block][system_key].keys()):
                if not config_db['commands'][execution_block][system_key][tc_key]['run']:
                    config_db['commands'][execution_block][system_key].pop(tc_key)

    # Remove traffic items that are not required
    for tc in list(config_db['traffic_db'].keys()):
        for tc_key in list(config_db['traffic_db'][tc].keys()):
            if not config_db['traffic_db'][tc][tc_key]['run']:
                config_db['traffic_db'][tc].pop(tc_key)
                if len(config_db['traffic_db'][tc]) == 0:
                    config_db['traffic_db'].pop(tc)

    return config_db


def open_ssh(config_db):
    """
    Establish SSH connections and updates config_db with the ssh instance
    :param config_db: A compiled database comprised of a previous call to comp_tp
    :return: True or False based on success
    """
    result = True
    for system, params in config_db['resource_config'].items():
        root_logger.info(f'Creating SSH instance for {system}')
        ssh = ssh_drv.SSH()

        # Is non default ssh port used for ctrl-plane?
        ctrl_port = 22
        if 'ctrl_port' in params:
            ctrl_port = params['ctrl_port']

        if ssh.open(params['ctrl_ip'], system, params['username'],
                    params['password'], port=ctrl_port, role=params['role']):

            # TO DO - push tools to systems
            if params['tools']:
                root_logger.warning('   TO DO: add code to push updated tools to system')

            # Update config_db with fh
            config_db['resource_config'][system]['fh_ssh'] = ssh

            ssh.data_ip = config_db['resource_config'][system]['data_ip']
            ssh.stats = config_db['resource_config'][system]['stats']

            if 'interfaces' in config_db['resource_config'][system]:
                ssh.interfaces = config_db['resource_config'][system]['interfaces']

            cmds_block = config_db['resource_config'][system]['commands']
            if cmds_block and cmds_block in config_db['commands']['per_tc_execution'].keys():
                ssh.commands = config_db['commands']['per_tc_execution'][cmds_block]
            else:
                ssh.commands = False
        else:
            root_logger.error(f'SSH connection to {system} failed - game over')
            result = False
            break

    root_logger.info('All requested SSH sessions established')

    return result


def get_ssh_fh(config_db):
    """
    Returns lists of file handles corresponding the specific system roles
    :param config_db: A compiled database comprised of a previous call to comp_tp
    :return: clients, servers, duts, netems, cpes, ccs - lists of corresponding file handles
             or False if any of the required ssh sessions are missing
    """
    # Fish out expected file handles
    clients = []
    servers = []
    duts = []
    netems = []
    cpes = []
    ccs = []
    for system, params in config_db['resource_config'].items():
        if params['role'] == 'client':
            clients.append(params['fh_ssh'])
        if params['role'] == 'server':
            servers.append(params['fh_ssh'])
        if params['role'] == 'dut':
            duts.append(params['fh_ssh'])
        if params['role'] == 'netem':
            netems.append(params['fh_ssh'])
        if params['role'] == 'cpe':
            cpes.append(params['fh_ssh'])
        if params['role'] == 'cc':
            ccs.append(params['fh_ssh'])

    # Mandatory sessions
    if len(clients) == 0 or len(servers) == 0 or len(netems) == 0:
        root_logger.error('Mandatory ssh connection missing to either client, server or netem')
        return False

    # Optional sessions
    if len(duts) == 0:
        duts.append(False)
    if len(cpes) == 0:
        cpes.append(False)
    if len(ccs) == 0:
        ccs.append(False)

    return clients, servers, duts, netems, cpes, ccs


def test_init(config_db):
    """
    Performs the initialization and configuration to satisfy the test scripts
    being executed. This includes all the SSH connections, software installs, DUT provisioning,
    and impairments to the secondary netem.
    :param config_db: A compiled database comprised of a previous call to comp_tp
    :return: True or False based on success
    """
    log_file_prefix = root_logger.log_file_prefix

    result = True
    root_logger.info(' +++ Test Infrastructure Initialization Start +++')

    # Establish the SSH sessions
    root_logger.info('Attempting to establish SSH connections')
    if not open_ssh(config_db):
        return False

    # Get lists of file handles (only supporting one of each client and server for now)
    clients, servers, duts, netems, cpes, ccs = get_ssh_fh(config_db)
    # client = clients[0]
    # server = servers[0]
    # dut = duts[0]
    # cpe = cpes[0]
    # cc = ccs[0]
    # pdb.set_trace()
    # ssh_lib.cli_nav(cpe, 'ana2-client')

    # ssh_lib.calibrate_links(cpes[0])
    # ssh_lib.calibrate_links(cpes[1], aux_srv='192.168.120.2')
    # ssh_lib.cli_get_objects(cpe)

    # Sync remote system times to automation servers clock
    root_logger.info('Synchronizing system clocks')
    # [ssh_lib.set_date(fh) for fh in clients + servers + netems]

    # Optional NEs
    if cpes[0]:
        [ssh_lib.set_date(fh) for fh in cpes]
    if ccs[0]:
        [ssh_lib.set_date(fh) for fh in ccs]

    # Ensure client can ping the server
    root_logger.info('Verify data path - DUT pings server')
    srv_data_ip = servers[0].data_ip
    if not ssh_lib.ping(clients[0], srv_data_ip):
        root_logger.error('The DUT is unable to ping the server - game over!')
        return False

    ########################################################
    # Perform pre-test case command execution
    ########################################################
    for system, params in config_db['resource_config'].items():
        cmd_key = params['commands']
        if cmd_key == 'srv_cmds' or cmd_key == 'cli_cmds' or cmd_key == 'cpe_cmds' or cmd_key == 'cc_cmds':
            fh_ssh = config_db['resource_config'][system]['fh_ssh']
            role = fh_ssh.role
            root_logger.info(f'Performing {role} {system} pre-test command execution')
            cmd_result = True
            if cmd_key in config_db['commands']['pre_tc_execution'].keys():
                for k, v in config_db['commands']['pre_tc_execution'][cmd_key].items():
                    if not cmd_result:
                        result = False
                        break
                    for cmd in v['cmds']:
                        ssh_result = fh_ssh.send(cmd)
                        if not ssh_result[0]:
                            cmd_result = False
            if not cmd_result:
                root_logger.error('Unable to complete the requested pre test case execution commands')
                return False

    ########################################
    # Open local csv stats log file(s) when required, used to store per-test case remote stats.
    # Corresponding remote stats will be copied after each test case to the local csv.
    # This is not the same as the global CSV log file opened in main()
    ########################################
    for system, params in config_db['resource_config'].items():
        if params['stats']:
            csv_file_name = f'logs/{log_file_prefix}-{system}_stats.csv'
            fh_csv = open(csv_file_name, 'a')
            csv_writer = csv.writer(fh_csv, delimiter=',')
            root_logger.info(f'csv stats file opened - {csv_file_name}')

            # Update instance with csv writer
            fh_ssh_tmp = config_db['resource_config'][system]['fh_ssh']
            fh_ssh_tmp.fh_csv = fh_csv
            fh_ssh_tmp.csv_writer = csv_writer
            config_db['resource_config'][system]['fh_ssh'] = fh_ssh_tmp

        ########################################################
        # primary netem(s) - Clear existing and prime TC queues
        ########################################################
        if params['role'] == 'netem' and 'primary' in params['impairments']:
            fh_ssh_netem_p = config_db['resource_config'][system]['fh_ssh']
            interfaces = config_db['resource_config'][system]['interfaces']
            root_logger.info('Priming primary netem tc queue disciplines')
            for intf in interfaces:
                fh_ssh_netem_p.send(f'tc -force qdisc del dev {intf} root')
                fh_ssh_netem_p.send(
                 f'tc qdisc add dev {intf} root handle 1: prio bands 2 priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1')
                fh_ssh_netem_p.send(f'tc qdisc add dev {intf} parent 1:2 netem limit 50000')

        ########################################################
        # secondary netem - optional impairments only applied
        # at start of sweeps. (Will apply to all test cases)
        ########################################################
        if params['role'] == 'netem' and 'secondary' in params['impairments']:
            root_logger.info('Applying secondary netem impairments')
            fh_ssh_netem_s = config_db['resource_config'][system]['fh_ssh']
            interfaces = config_db['resource_config'][system]['interfaces']
            imp_key = config_db['resource_config'][system]['impairments']
            impairments = config_db['impairments'][imp_key]

            imp_cmd = False
            for intf in interfaces:
                # Delete pre-existing queue disciplines
                fh_ssh_netem_s.send(f'tc -force qdisc del dev {intf} root')
                fh_ssh_netem_s.send(
                 f'tc qdisc add dev {intf} root handle 1: prio bands 2 priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1')

                for imp in impairments.keys():
                    if imp == 'duplicate' or imp == 'corrupt':
                        imp_cmd = f'tc qdisc add dev {intf} parent 1:2 netem limit 50000 {imp} {impairments[imp]}'

                    if imp == 'reorder':
                        imp_cmd = f'tc qdisc add dev {intf} parent 1:2 netem limit 50000 {imp} {impairments[imp]}'
                        if 'gap' in impairments.keys():
                            imp_cmd = f"tc qdisc add dev {intf} parent 1:2 netem limit 50000 {imp} {impairments['reorder']} gap {impairments['gap']}"
                    fh_ssh_netem_s.send(imp_cmd, True)

    if result:
        root_logger.info(' +++ Test Infrastructure Initialization Complete +++')
    else:
        root_logger.error(' === Test infrastructure initialization failed to complete ===')

    root_logger.info('')
    return result


def compile_impairments(config_db):
    """
    Decompresses the impairments provided in the config yaml, and compiles a single
    dict with all the data pertaining to provisioning the impairments loops.

    :param config_db: A compiled database comprised of a previous call to comp_tp
    :return: impairments_db - A dict with the major key based on the system name
             and the following minor keys:
             fh_ssh - the netem ssh filehandle
             interfaces - the interfaces which impairments are applied
             pri_impairments - a list of the primary impairments, typically (loss, latency)
             jitter - False or int
             repeat - the number of times to repeat the impairment test
    """

    impairments_db = {}
    # Fish out the primary netem impairments
    for system, params in config_db['resource_config'].items():
        if params['role'] == 'netem' and 'primary' in params['impairments']:
            fh_ssh = config_db['resource_config'][system]['fh_ssh']
            interfaces = config_db['resource_config'][system]['interfaces']
            imp_key = config_db['resource_config'][system]['impairments']
            impairments = config_db['impairments'][imp_key]

            # Required impairments
            loss = impairments['loss']
            latency = impairments['latency']

            # Optional keys
            repeat = 1
            jitter = False
            rate = False
            if 'repeat' in impairments:
                repeat = impairments['repeat']
            if 'jitter' in impairments:
                jitter = impairments['jitter']
            if 'rate' in impairments:
                rate = impairments['rate']
                # Build the rate list
                rate_start = int(rate[0].split('-')[0])
                rate_end = int(rate[0].split('-')[1])

                # If needed reverse
                rate_reverse = False
                if rate_start > rate_end:
                    rate_start = int(rate[0].split('-')[1])
                    rate_end = int(rate[0].split('-')[0])
                    rate_reverse = True

                rate_increment = rate[1]
                rate_cnt = rate_start
                rate_itr = 0
                rate_values = []

                while rate_cnt <= rate_end:
                    rate_values.append(round(rate_cnt, 2))
                    rate_itr += rate_increment
                    rate_cnt = round(rate_cnt, 2) + rate_increment

                if rate_reverse:
                    rate_values.reverse()

            # Build the loss list
            try:
                loss_start = float(loss[0].split('-')[0])
                loss_end = float(loss[0].split('-')[1])
                loss_increment = loss[1]
                loss_cnt = loss_start
                loss_itr = 0
                loss_values = []

                while loss_cnt <= loss_end:
                    loss_values.append(round(loss_cnt, 2))
                    loss_itr += loss_increment
                    loss_cnt = round(loss_cnt, 2) + loss_increment

            except AttributeError:
                loss_values = [float(loss[0])]

            # Build the latency list
            try:
                lat_start = int(latency[0].split('-')[0])
                lat_end = int(latency[0].split('-')[1])
                lat_increment = latency[1]
                lat_cnt = lat_start
                lat_itr = 0
                lat_values = []

                while lat_cnt <= lat_end:
                    lat_values.append(round(lat_cnt, 2))
                    lat_itr += lat_increment
                    lat_cnt = round(lat_cnt, 2) + lat_increment

            except AttributeError:
                lat_values = [float(loss[0])]

            pri_impairments = [imp for imp in list(product(loss_values, lat_values))]
            if 'rate' in impairments:
                pri_impairments = [imp for imp in list(product(rate_values, loss_values, lat_values))]

            impairments_db.update({
                system: {
                    'fh_ssh': fh_ssh,
                    'interfaces': interfaces,
                    'pri_impairments': pri_impairments,
                    'jitter': jitter,
                    'rate': rate,
                    'repeat': repeat
                }
            })

    return impairments_db


def generate_report_data(config_db):
    """
    Generates the title and text for the topology diagram and report, as well as
    appends to a report config template with relevant NE details. The NEs are:This data
    (clients, servers, duts, netems, cpes, ccs). This data is saved to a yml which is then
    used in the high level HTML report to populate the 'NETWORK TOPOLOGY DETAILS' table.

    :param config_db: A compiled database comprised of a previous call to comp_tp
    :return: A string meant to be used as the H2 HTML title
    """

    log_file_prefix = root_logger.log_file_prefix
    report_data_out = f'logs/{log_file_prefix}.yml'

    # Pull in the template report data yaml file
    topology_yaml = 'report_templates/as_report_data.yaml'
    try:
        topology_dict_in = cmn_lib.yml_to_dict(topology_yaml)
        topology_diag = topology_dict_in['topology_diag']
        versions = topology_dict_in['versions']
        next_key = len(versions.keys())
    except ValueError as top_error:
        root_logger.error(f'Error reading {topology_yaml}: {top_error}')
        return False

    # This will be the URL to the topology diagram
    top_img_link = False
    sdwan_mode = False

    # Get lists of file handles (only supporting one of each client and server for now)
    clients, servers, duts, netems, cpes, ccs = get_ssh_fh(config_db)

    if cpes[0]:
        cpe_count = 1

        for cpe in cpes:
            # ANA2 Client information
            ana_info = ssh_lib.cli_get_ana2_tunnel(cpe)
            num_uls = ssh_lib.cli_get_underlay_info(cpe)

            # Connection type, SD-I, SD-R or SD-W - weak way to check, relies on ports
            link1 = ana_info['Link1']
            top_img_link = 'UNKNOWN'
            if '6666' in link1[0]:
                sdwan_mode = 'SD-Internet'
                # Num of underlays
                if num_uls == 1:
                    top_img_link = 'http://www.svtqa.xyz/as_images/sd_internet_1_link.png'
                elif num_uls == 2:
                    top_img_link = 'http://www.svtqa.xyz/as_images/sd_internet_2_links.png'
            elif '6747' in link1[0]:
                sdwan_mode = 'SD-WAN'
                # Num of underlays
                if num_uls == 1:
                    top_img_link = 'http://www.svtqa.xyz/as_images/sd_wan_1_link.png'
                elif num_uls == 2:
                    top_img_link = 'http://www.svtqa.xyz/as_images/sd_wan_2_links.png'
            else:
                sdwan_mode = 'UNKNOWN'

            underlay_txt = f'Num of ISP underlays: {num_uls}'

            # Underlay Link bandwidth (links needed to be calibrated)
            bandwidth = ana_info['Bandwidth']
            link_desc = False
            for link in bandwidth:
                m = re.search(r'(Link\d) *(O:)(\d*)(\(bps\) *(I:)(\d*))', link)
                if m:
                    link_id = m.group(1)
                    bw_o_Mbps = int(m.group(3)) / 1000000
                    bw_i_Mbps = int(m.group(6)) / 1000000
                    if bw_o_Mbps < 1:
                        bw_o_Mbps = 'UNCALIBRATED'
                        bw_i_Mbps = 'UNCALIBRATED'
                    else:
                        bw_o_Mbps = f'{bw_o_Mbps} Mbps'
                        bw_i_Mbps = f'{bw_i_Mbps} Mbps'
                else:
                    root_logger.error('Unable to determine link bandwidth')
                    link_desc = 'Link1 and Link2: UNKNOWN Mbps UP/UNKNOWN Mbps DOWN'
                    break
                if link_id == 'Link1':
                    link_desc = f'ANA {link_id}: {bw_o_Mbps} UP / {bw_i_Mbps} DN'
                else:
                    link_desc += f'<BR>ANA {link_id}: {bw_o_Mbps} UP / {bw_i_Mbps} DN'

            cpe_name = cpe.sys_name
            cpe_ver = ssh_lib.cli_get_ver(cpe)
            prof_name = ana_info['Profile Name']
            cpe_ana_ver = prof_name[3]
            weight = ana_info['Weight']
            weight = f'Weight: {weight}'
            rla_bw = ana_info['RLA-Bandwidth']
            rla_bw = f'RLA-Bandwidth: {rla_bw}'
            ipde_queue = ana_info['IPDE-QUEUE']
            ipde_queue = f'IPDE-QUEUE: {ipde_queue}'
            cpe_notes = f'{cpe_ana_ver}<BR>{weight}<BR>{rla_bw}<BR>{ipde_queue}<BR>{link_desc}'

            next_key += 1
            versions.update({next_key: {
                'name': cpe_name,
                'version': cpe_ver,
                'notes': cpe_notes
                }
            })
            cpe_count += 1

    # CCs
    if ccs[0]:
        for cc in ccs:
            ana_srv = ssh_lib.cli_get_ana2_server(cc)
            cc_ver = ssh_lib.cli_get_ver(cc)
            cc_ana_ver = ana_srv['Version']
            cc_notes = f'ANA version {cc_ana_ver[0]}'
            next_key += 1
            versions.update({next_key: {
                'name': cc.sys_name,
                'version': cc_ver,
                'notes': cc_notes
                }
            })

    # Clients & Servers (always present as these the sweep endpoints)
    for fh_ssh in clients + servers:
        ipq_ver = False
        if 'Windows' in fh_ssh.os:
            ver_info = fh_ssh.send('/cygdrive/c/tools/pstools/PsInfo.exe')
            for line in ver_info[1]:
                if 'Kernel version:' in line:
                    os_ver = line.rsplit(':')[1].lstrip()
                if 'Processors:' in line:
                    cpu_num = line.rsplit(':')[1].lstrip()
                if 'Processor speed:' in line:
                    cpu_speed = line.rsplit(':')[1].lstrip()
                if 'Processor type:' in line:
                    cpu_type = line.rsplit(':')[1].lstrip()

            ipq_info = fh_ssh.send('/cygdrive/c/tools/ipqtool.exe -get .info')

        elif 'Linux' in fh_ssh.os:
            os_ver = fh_ssh.send('uname -rv')[1][0]
            cpu_info = fh_ssh.send('cat /proc/cpuinfo')
            cpu_num = 0
            for line in cpu_info[1]:
                if 'processor' in line:
                    cpu_num += 1
                if 'cpu MHz' in line:
                    cpu_speed = line.rsplit(':')[1].lstrip()
                if 'model name' in line:
                    cpu_type = line.rsplit(':')[1].lstrip()

            ipq_info = fh_ssh.send('ipqtool -get .info')

        if ipq_info[0]:
            ipq_ver = ipq_info[1][0].rsplit(':')[1].lstrip()
            notes = f'IPQ {ipq_ver}<BR>{cpu_num} of {cpu_speed} {cpu_type}'
        else:
            notes = f'{cpu_num} of {cpu_speed} {cpu_type}'
            ipq_ver = 'N/A'

        next_key += 1
        versions.update({next_key: {
            'name': fh_ssh.sys_name,
            'version': os_ver,
            'notes': notes
            }
        })

    # If an SD_WAN mode wasn't declared, use the generic IPQ sweep details
    if sdwan_mode:
        tpd_name = f'Mode: {sdwan_mode}'
        tpd_desc = f'This topology best matches the Adaptiv Networks {sdwan_mode} solution'
        # h2_title = f'{underlay_txt}<BR>{link_desc}'
        h2_title = f'{underlay_txt}'
    else:
        tpd_name = 'Generic IPQ autosweep topology'
        tpd_desc = f'This topology best matches the generic IPQ autosweep regression'
        traf_type = [k for k in config_db['traffic_db'].keys()][0].upper()
        if traf_type == 'RDP':
            top_img_link = 'http://www.svtqa.xyz/as_images/rdp_sweep.png'
        else:
            top_img_link = 'http://www.svtqa.xyz/as_images/generic_sweep.png'
        h2_title = f'IPQ: {ipq_ver} - {traf_type} module'

    # Topology Diagram
    topology_diag.update({
        'name': tpd_name,
        'link': top_img_link,
        'description': tpd_desc
    })

    topology_dict_out = {
        'topology_diag': topology_diag,
        'versions': versions
    }

    # Write out yaml file used to populate "network topology details" section
    with open(report_data_out, 'w+') as f:
        yaml.dump(topology_dict_out, f)

    return h2_title


def kick_it(l_args):
    """
    Ensures and configures the following:
     - required systems are available
     - pre-requisites (such as tools, software etc) are met
     - pre-test provisioning
     - network plumbing including netem_2 provisioning (which persists across the sweeps)
    Kicks off the requested automation
    """

    main_csv_writer = l_args['csv_writer']
    gen_final_report = l_args['gen_final_report']
    fh_csv = l_args['fh_csv']
    log_file_prefix = root_logger.log_file_prefix

    # Memo start time
    ts_start = time.time()

    # Parse the config file
    config_db = comp_tp(l_args['config_file'])

    # Initialize the test environment
    if not test_init(config_db):
        return False

    # Compile the impairments
    impairments_db = compile_impairments(config_db)

    # Build list of per test case cmd keys
    cmd_keys = []
    per_tc_execution = config_db['commands']['per_tc_execution']
    for system, cmds in per_tc_execution.items():
        [cmd_keys.append(k) for k, v in cmds.items() if k not in cmd_keys]
    if len(cmd_keys) == 0:
        cmd_keys = [1]
    else:
        cmd_keys.sort()

    # Generate as_final_report.yml used as input to final html report
    h2_title = ''
    if gen_final_report:
        h2_title = generate_report_data(config_db)

    # HACK - both primary netems must use the same impairments (at least for now)
    # So, the impairments in the config file for the last primary netem will be used
    pri_impairments = None
    imp_repeat = 1
    for netem_p, impairments in impairments_db.items():
        pri_impairments = impairments_db[netem_p]['pri_impairments']
        imp_repeat = impairments_db[netem_p]['repeat']

    traffic_cmds = []
    for data_type, key in config_db['traffic_db'].items():
        for k_tc, v_tc in key.items():
            # Did caller specify traffic specific graphs?
            if 'graphs' in v_tc:
                traffic_cmds.append([data_type, v_tc['args'], v_tc['graphs']])
            else:
                traffic_cmds.append([data_type, v_tc['args'], False])

    # Get lists of file handles and break out variables
    clients, servers, duts, netems, cpes, ccs = get_ssh_fh(config_db)

    # Required connections established?
    if len(clients) == 0 or len(servers) == 0 or len(netems) == 0:
        return False

    # HACK - only supporting ONE client and server for now
    # The following will use only the 1st ssh fh if multiple specified in config file
    ssh_client = clients[0]
    ssh_server = servers[0]

    # Variables for csv_data
    cli_sys_name = ssh_client.sys_name
    cli_os = ssh_client.os
    srv_sys_name = ssh_server.sys_name
    srv_os = ssh_server.os

    tc_total = len(pri_impairments) * len(cmd_keys) * len(traffic_cmds) * imp_repeat
    tc_id = 1
    rpt_cnt = 1
    write_header = True

    # Loopy logic!
    while rpt_cnt < imp_repeat + 1:

        # IMPAIRMENTS
        imp_cnt = 1
        for l_l in pri_impairments:
            root_logger.info(f'  +++ Impairment {imp_cnt} of {len(pri_impairments)} +++')
            loss = l_l[0]
            lat = l_l[1]
            rtt = lat * 2

            # Send the same impairments to each primary netem
            for netem_p in impairments_db.keys():
                fh_netem_p = impairments_db[netem_p]['fh_ssh']
                interfaces = impairments_db[netem_p]['interfaces']
                for intf in interfaces:
                    imp_cmd = f'tc qdisc change dev {intf} parent 1:2 netem limit 50000'
                    if loss > 0:
                        imp_cmd = f'{imp_cmd} loss {loss}'
                    if lat > 0:
                        imp_cmd = f'{imp_cmd} delay {lat}ms'
                    fh_netem_p.send(imp_cmd, True)
            imp_cnt += 1

            ########################################
            # PER TEST-CASE SYSTEM PROVISIONING COMMAND SETS
            # (infrastructure knobs and levers before traffic runs)
            ########################################
            for cmd_set in cmd_keys:
                # csv_headers = ['TC_ID', 'ITERs',
                #               'CLIENT_NAME', 'CLIENT_OS', 'SERVER_NAME', 'SERVER_OS', 'CMD_SET',
                #               'CLIENT_CMDs', 'SERVER_CMDs', 'DATA_COMMAND', 'Loss(%) - Latency(RTT)']

                csv_headers = ['TC_ID', 'ITERs',
                               'CLIENT_NAME', 'CLIENT_OS', 'SERVER_NAME', 'SERVER_OS', 'CMD_SET',
                               'CLIENT_CMDs', 'SERVER_CMDs', 'CPE_CMDS', 'CC_CMDS', 'DATA_COMMAND',
                               'Loss(%) - Latency(RTT)']
                cli_cmds = ['None']
                srv_cmds = ['None']
                cpe_cmds = ['None']
                cc_cmds = ['None']
                system_cmds = None

                for fh_ssh in clients + servers + cpes + ccs:
                    if fh_ssh:
                        role = fh_ssh.role
                        cs_cnt = 0

                        if cmd_set in fh_ssh.commands.keys():
                            system_cmds = fh_ssh.commands[cmd_set]['cmds']
                            # loop through list of cmds
                            for system_cmd in system_cmds:
                                cs_cnt += 1
                                root_logger.info(f' {role} cmd set {cmd_set} - {system_cmd}')

                                # CPEs and CCs require the cmd is executed in the correct node
                                if role == 'cpe' or role == 'cc':
                                    cli_node = system_cmd.split('/')[0]
                                    system_cmd = system_cmd.split('/')[1]
                                    ssh_lib.cli_nav(fh_ssh, cli_node)

                                fh_ssh.send(system_cmd)
                        else:
                            system_cmds = ['None']
                            root_logger.info(f' {role} cmd set {cmd_set} - NO COMMANDS')

                    if role == 'client':
                        cli_cmds = ', '.join(system_cmds)
                    if role == 'server':
                        srv_cmds = ', '.join(system_cmds)
                    if role == 'cpe':
                        cpe_cmds = ', '.join(system_cmds)
                    if role == 'cc':
                        cc_cmds = ', '.join(system_cmds)

                ########################################
                # TEST TRAFFIC
                ########################################
                trf_cmd_cnt = 1
                traffic_headers = None
                traffic_data = None

                for trf_cmd in traffic_cmds:

                    csv_data = [f'{tc_id} of {tc_total}']
                    csv_data.extend([f'{rpt_cnt} of {imp_repeat}'])
                    csv_data.extend([cli_sys_name, cli_os, srv_sys_name, srv_os])
                    csv_data.extend([f'{cmd_set}'])
                    csv_data.extend([cli_cmds, srv_cmds, cpe_cmds, cc_cmds])

                    trf_type = trf_cmd[0]
                    trf_args = trf_cmd[1]
                    graphs = trf_cmd[2]

                    csv_data_tmp = deepcopy(csv_data)
                    trf_cmd_string = f'{trf_type} {trf_args}'
                    loss_lat_str = '%.2f - %s' % (loss, rtt)
                    csv_data_tmp.extend([trf_cmd_string, loss_lat_str])

                    # Start the stats collectors
                    # Note about traffic types. Most cmd line traffic tools like iperf3 can be scraped for
                    # bandwidth values, which are not the same as collecting local host stats like network, cpu etc.
                    # The RDP traffic item relies 100% on collecting local windows typeperf metrics. In order to
                    # generate a graph inside the rdp function, it must terminate stats collection there, and not
                    #  below as is the case with most of the autosweep traffic items.
                    for system, params in config_db['resource_config'].items():
                        if params['stats']:
                            if trf_type != 'rdp':
                                ssh_lib.dut_stats(params['fh_ssh'], 'start', trf_cmd[0])

                    root_logger.tcinfo('=== TEST CASE START ===')
                    root_logger.tcinfo(f'    TC {tc_id} of {tc_total} / traffic {trf_cmd_cnt} of {len(traffic_cmds)} / loss:{loss} lat:{rtt}')
                    root_logger.tcinfo(f'       {trf_cmd_string}')

                    # Test duration used to trim the stats on the client and server
                    tc_dur = False

                    if trf_type == 'iperf3':
                        m_time = re.search(r'(-t)(\d+)', trf_args)
                        if m_time:
                            tc_dur = int(m_time.group(2))

                        if graphs:
                            ll_graph_title = 'Loss: %.2f (%%) - Lat: %s (ms)' % (loss, rtt)
                            traffic_headers, traffic_data = iperf3(ssh_client, ssh_server, trf_args, graph=True, tc_id=tc_id, impairments=ll_graph_title)
                        else:
                            traffic_headers, traffic_data = iperf3(ssh_client, ssh_server, trf_args, tc_id=tc_id)

                    if trf_type == 'rdp':
                        tc_dur = trf_args[2]
                        if graphs:
                            ll_graph_title = 'Loss: %.2f (%%) - Lat: %s (ms)' % (loss, rtt)
                            traffic_headers, traffic_data = rdp(ssh_client, ssh_server, trf_args, graph=True, tc_id=tc_id, impairments=ll_graph_title)
                        else:
                            traffic_headers, traffic_data = rdp(ssh_client, ssh_server, trf_args, tc_id=tc_id)

                    # Update traffic headers & data if needed
                    csv_headers.extend(traffic_headers)
                    csv_data_tmp.extend(traffic_data)

                    # Stop the stats collectors (if not rdp) and fetch the stats
                    # pandas_stats_all is some experimentation, and not currently used.
                    pandas_stats_all = {}
                    for system, params in config_db['resource_config'].items():
                        fh_ssh = params['fh_ssh']
                        stats = params['stats']

                        if stats and trf_type != 'rdp':
                            ssh_lib.dut_stats(fh_ssh, 'stop')
                            traffic_headers, traffic_data, pandas_stats = ssh_lib.fetch_stats(
                                 fh_ssh, tc_id=tc_id, tc_dur=tc_dur)
                            pandas_stats_all.update({system: pandas_stats})
                            # Update traffic headers & data
                            csv_headers.extend(traffic_headers)
                            csv_data_tmp.extend(traffic_data)
                        else:
                            continue

                    # Write to main csv file
                    if write_header:
                        main_csv_writer.writerow(csv_headers)
                        write_header = False
                    main_csv_writer.writerow(csv_data_tmp)
                    fh_csv.flush()

                    root_logger.tcinfo('=== TEST CASE END ===')
                    root_logger.info('')
                    tc_id += 1
                    trf_cmd_cnt += 1

                    root_logger.info('Sleeping 30s between tests due to netem buffer type issue')
                    time.sleep(30)
                    if config_db['test_execution']['halt_after_test']:
                        root_logger.info('Halting test execution - press c to continue')
                        pdb.set_trace()

        rpt_cnt += 1

    fh_csv.close()

    # If iperf3 graph dir exists, create tgz file
    ssh_dir = f'logs/{log_file_prefix}_iperf3'
    if os.path.exists(ssh_dir):
        tgz_new = f'{ssh_dir}_graphs.tgz'
        root_logger.info(f'Generating iperf3 graph tar file - {tgz_new}')
        with tarfile.open(tgz_new, "w:gz") as tar:
            tar.add(ssh_dir, arcname=os.path.basename(ssh_dir))
        # Delete iperf3 graph dir
        shutil.rmtree(ssh_dir)

    # Generate high level HTML report
    if gen_final_report:
        imp_str = 'Impairments applied to:'
        for netem in impairments_db.keys():
            imp_str = f'{imp_str} {netem}'

        ts_report = time.strftime('%Y-%m-%d %H:%M:%S')
        run_time_sec = time.time() - ts_start
        run_time = strftime("%H:%M:%S", gmtime(run_time_sec))
        h2_prefix = f'{tc_total} test cases completed in {run_time}'
        h1_title = f'Autosweep Report: {ts_report}'
        h2_title = f'{h2_prefix}<BR><BR>{h2_title}<BR>{imp_str}'
        csvfile = f'logs/{log_file_prefix}.csv'
        yml = f'logs/{log_file_prefix}.yml'
        out = f'logs/{log_file_prefix}.html'
        report_cmd = f"../lib/report_gen.py -c {csvfile} -ty {yml} -mt '{h1_title}' -st '{h2_title}' -o {out}"
        os.system(report_cmd)

    return True
