import time


def p_trace(string, level='INFO '):
    """
    Helper function to standardize look of local script prints
    :param string: The string to be printed
    :param level: The trace level
    :return: natta
    """
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f'{"=====> ":>15} {level} {ts} {string}')
