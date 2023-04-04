import paramiko
import time
import re
import pdb
from cmn_lib import p_trace
from colorama import Fore

"""
This file contains the class that implements the ssh interface to a
remote target system over SSH.

A word to the wise! Paramiko executes each command sent from the initial directory
to which the connection was made. So /home/live for example. To execute a command 
in a different location, chain the commands together. Eg 'cd /new/dir ; ls -al' 
"""


class SSH(object):
    """
    Establishes an SSH session to a remote system, and then handles commands and command output
    """

    def __init__(self):
        """
        """
        self.host_id = False
        self.port = '22'
        self.sys_name = False
        self.fh_ssh = False
        self.os = 'UNKNOWN'
        self.cpu = 'UNKNOWN'
        self.role = False
        self.io_mode = 'standard'
        self.channel = None
        self.prompt = None
        self.monitor_passwd = 'agni123'
        self.admin_passwd = 'agni123'
        self.l3_password = 'c4n4d4DRY'
        self.diag_passwd = 'dp9747ST'

    def open(self, host_id, sys_name, user_name, password, **kwargs):
        """
        Opens the SSH session with the remote system
        :param host_id: Host name or IP address of the remote system
        :param sys_name: A string of convenience, used in the logs to help identify the remote system
        :param user_name: Username to log into remote system
        :param password: Password to log into remote system
        :param kwargs: Optional args are:
                        port - the SSHD port
                        role - When <cpe|cc|rs> implies Adaptiv based host (Agnios is an app and must use raw output)
        :return: True or False based on success of establishing ssh connection
        """
        self.host_id = host_id
        self.sys_name = sys_name
        self.fh_ssh = False
        result = True
        log_string = ''

        # Unpack optional args
        for name, value in list(kwargs.items()):
            if name == 'port':
                self.port = value
            if name == 'role':
                self.role = value
            if name == 'monitor_passwd':
                self.monitor_passwd = value
            if name == 'admin_passwd':
                self.admin_passwd = value

        p_trace(Fore.YELLOW + f'Attempting to establish ssh connection to '
                f'{host_id}:{self.port} as {user_name} / {password}' + Fore.RESET)

        try:
            self.fh_ssh = paramiko.SSHClient()
            self.fh_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.fh_ssh.connect(host_id, port=self.port, username=user_name, password=password, timeout=60)
        except paramiko.AuthenticationException:
            log_string = Fore.RED + f'Authentication failed when connecting to {host_id} as {user_name} / {password}'
            result = False
        except paramiko.BadHostKeyException:
            log_string = Fore.RED + 'The host key given by the SSH server did not match what we were expecting'
            p_trace(log_string + Fore.RESET)
            result = False

        if result:
            if self.role != 'cpe' and self.role != 'cc':
                # Determine host OS and CPU
                uname = self.send('uname -a')[1][0]
                if re.search(r'^Linux', uname):
                    self.os = 'Linux'
                    if re.search('x86_64', uname):
                        self.cpu = '64Bit'
                    else:
                        self.cpu = '32Bit'
                elif re.search('CYGWIN_NT-6.1-WOW64', uname):
                    # Applies to Windows 7 and SRV 2008 R1
                    self.os = 'Windows7'
                    self.cpu = '64Bit'
                elif re.search('CYGWIN_NT-6.3-WOW64', uname):
                    # Applies to SRV 2012 R2
                    self.cpu = '64Bit'
                    self.os = 'Server2012R2'
                elif re.search('CYGWIN_NT-6.2-WOW64', uname):
                    self.os = 'Windows8.0'
                    self.cpu = '64Bit'
                elif re.search('CYGWIN_NT-6.3', uname):
                    self.os = 'Windows8.1'
                    self.cpu = '64Bit'
                elif re.search('CYGWIN_NT-6.4-WOW64', uname):
                    self.os = 'Windows10'
                    self.cpu = '64Bit'
                elif re.search('CYGWIN_NT-10.0', uname):
                    self.os = 'Windows10'
                    self.cpu = '64Bit'
                elif re.search('CYGWIN_NT-6.1', uname):
                    self.os = 'Windows7'
                    self.cpu = '32Bit'
            else:
                # Adaptiv AgniShell
                self.os = 'AgniOS'
                self.cpu = '64Bit'
                self.io_mode = 'raw'
                self.channel = self.fh_ssh.invoke_shell()

                time.sleep(1)
                welcome_msg = self.channel.recv(9999).decode("ascii")
                m_ver = re.search('(Version )(\\d.\\d.\\d-RELEASE)', welcome_msg)
                m_prompt = re.search('(\r\n)(.*)(>)', welcome_msg)
                if m_prompt:
                    self.prompt = f'{m_prompt.group(2)}{m_prompt.group(3)}'
                    # Overwrite callers sysname - used later to fish out the prompt
                    self.sys_name = m_prompt.group(2)
                else:
                    p_trace('Unable to determine prompt, needed to know when commands complete', 'ERROR')
                if m_ver:
                    self.os = f'AgniOS-{m_ver.group(2)}'
                p_trace(welcome_msg)

            log_string = Fore.GREEN + f'SSH connection established with {host_id} ({sys_name}) : {self.os}/{self.cpu}'

        p_trace(log_string + Fore.RESET)
        return result

    def send(self, cmd, suppress_logs=False):
        """
        Send a command and return the output from that command to the caller in a
        usable list format.
        :param cmd: The command to execute on the remote system
        :param suppress_logs: When True, the stdout will be suppressed and not printed to screen or the log file
        :return: ssh_response - a tuple consisting of the cmd result <True|False> and a
                 list consisting of stdout or stderr of the provided command
        """
        cmd_result = True
        p_trace(f"  -->  '{cmd}' to {self.sys_name} ({self.host_id})")

        ssh_response = []
        if self.io_mode == 'standard':
            try:
                stdin, stdout, stderr = self.fh_ssh.exec_command(cmd)
            except ValueError as error:
                root_logger.exception(f'SendSSH command failed - {error}')
                ssh_result = (False, False)
                return ssh_result

            while True:
                standard_out = stdout.readline()
                standard_error = stderr.readline()

                # Format the response data
                if standard_out:
                    line = standard_out
                    # Strip out initial blank space, new lines and carriage returns
                    line = re.sub(r'\r', '', line)
                    line_rgx = re.search(r"(.*)(\n)", line)
                    line2 = line_rgx.group(1)
                    ssh_response.append(line2)
                    if not suppress_logs:
                        p_trace('  <--  %s' % line2)

                if standard_error:
                    line = standard_error
                    line_rgx = re.search(r"(.*)(\r|\n)", line)
                    line = line_rgx.group(1)
                    ssh_response.append(line)
                    # This is odd, but sometimes psutils sends stdout to stderror
                    # Suppress to keep the logs clean
                    if re.search(r'pstools', cmd) and not suppress_logs:
                        p_trace(line)
                    else:
                        p_trace(line, 'ERROR')
                        cmd_result = False

                if not standard_out and not standard_error:
                    p_trace('  <--  previous command returned no output')
                    break

            if suppress_logs:
                p_trace('  <--  output of last cmd intentionally suppressed')

        else:
            # RAW CHANNEL MODE required for Adaptiv AgniOS
            self.channel.setblocking(1)
            self.channel.send(f'{cmd}\n')
            lines = []
            line = ''
            skip_fst_line = True

            while True:
                #line += self.channel.recv(1).decode('ascii')
                t = 1
                if 'set password' in cmd:
                    t = 10
                line += self.channel.recv(t).decode('utf-8')

                line = line.replace('\t', '    ')
                password = 'unknown'

                if line.startswith('Password:') or 'Admin Password' in line:
                    if cmd == 'admin' or 'set password' in cmd:
                        password = self.admin_passwd
                    elif cmd == 'level3':
                        password = self.l3_password
                    elif cmd == 'diag' or cmd == '/diag':
                        password = self.diag_passwd

                    p_trace(f'  <--  {line}')
                    p_trace(f"  -->  '{password}' to {self.sys_name} ({self.host_id})")
                    self.channel.send(f'{password}\n')
                    line = ''

                elif line.endswith('(Yes/No) ?'):
                    self.channel.send(f'y\n')
                    line = ''

                elif 'saveconfig' in line:
                    self.channel.send(f'yes\n')
                    line = ''

                elif line.endswith('\r\n'):
                    if len(line) > 2:
                        line = line.replace('\r\n', '')
                        if skip_fst_line:
                            skip_fst_line = False
                        else:
                            lines += [line]
                        # Look for errors
                        if line.startswith('% '):
                            cmd_result = False
                        line = ''

                elif line.endswith('# ') or line.endswith('> ') or line.endswith('#    '):
                    line = line.replace('\r\n', '')
                    line = line.replace(' ', '')
                    # non diag prompts
                    m = re.search(f'(.*)({self.sys_name}.*)', line)
                    if m:
                        self.prompt = m.group(2)
                    # diag (bsd shell) prompt
                    m2 = re.search(f'(root@)', line)
                    if m2:
                        self.prompt = line
                    break

            if not suppress_logs:
                for line in lines:
                    ssh_response.append(line)
                    p_trace('  <--  %s' % line)
            else:
                for line in lines:
                    ssh_response.append(line)
                p_trace('  <--  output of last cmd intentionally suppressed')


        ssh_result = (cmd_result, ssh_response)
        return ssh_result
