import time
from colorama import Fore


def p_trace(string, level='INFO'):
    """
    Helper function to standardize look of local script prints
    :param string: The string to be printed
    :param level: The trace level
    :return: natta
    """
    out_string = ""

    if level == 'ERROR' or level == 'FAIL':
        out_string = Fore.RED + string + Fore.RESET
    if level == 'WARNING' or level == 'DEBUG' or level == 'SKIPPED':
        out_string = Fore.YELLOW + string + Fore.RESET
    if level == 'DEBUG2':
        out_string = Fore.BLUE + string + Fore.RESET
    if level == 'TEST_CASE' or level == 'PASS':
        out_string = Fore.GREEN + string + Fore.RESET
    if level == 'INFO':
        out_string = string


    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f'{"=====> ":>15} {level} {ts} {out_string}')
