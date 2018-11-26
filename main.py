import argparse
import datetime
import logging
import os

import gevent.monkey as monkey
monkey.patch_all(thread=False, select=False)

from harvesters.gazetteer_harvester import GazetteerHarvester


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(format='%(asctime)s-%(levelname)s-%(name)s - %(message)s')


def validate_date(s: str):
    try:
        return datetime.date.fromisoformat(s)
    except ValueError:
        msg = f"Not a valid date: '{s}', expected pattern: YYYY-MM-DD"
        raise argparse.ArgumentTypeError(msg)


def is_positive_number(i: str):
    value = int(i)
    if value <= 0:
        msg = f"Please provide a positive date offset."
        raise argparse.ArgumentTypeError(msg)
    else:
        return value


def is_writable_directory(path: str):
    if os.path.exists(path) and (not os.path.isdir(path) or not os.access(path, os.W_OK)):
        msg = f"Please provide writable directory."
        raise argparse.ArgumentTypeError(msg)
    elif not os.path.exists(path):
        os.makedirs(path)
        return path
    else:
        return path


def create_default_output_directory():
    path = f'./output/{datetime.date.today().isoformat()}/'
    if not os.path.exists(path):
        os.makedirs(path)

    return path


parser = argparse.ArgumentParser(description='Harvest MARC authority data from various data providers.')
parser.add_argument('-f', '--format', type=str, nargs='?', default='marc', choices=['marc', 'marcxml'],
                    help='The desired output format.')
parser.add_argument('-s', '--sources', type=str, nargs='?', default='all', choices=['all', 'gazetteer', 'loc'],
                    help='The desired data providers.')
parser.add_argument('-t', '--target', type=is_writable_directory, nargs='?', default=create_default_output_directory(),
                    help='Specificy output directory.')

group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('-c', '--continue', action='store_true',
                   help='Continue from last time the script was run.')
group.add_argument('-d', '--date', type=validate_date, nargs='?',
                   help='Harvest everything from a given date onwards, pattern: YYYY-MM-DD.')
group.add_argument('-o', '--offset', type=is_positive_number, nargs='?',
                   help='Use an offset of days to specify the starting date.')


if __name__ == '__main__':
    parsed = vars(parser.parse_args())

    date_log_path = f"{parsed['target']}/last_run_date.log"
    print(parsed)

    if parsed['continue']:
        with open(date_log_path, 'r') as log:
            start_date = datetime.date.fromisoformat(log.readline())
    elif parsed['date']:
        start_date = parsed['date']
    else:
        start_date = datetime.date.today() - datetime.timedelta(days=parsed['offset'])

    logger.info(f"Harvesting all data changes since {start_date.isoformat()}.")

    if parsed['sources'] == "gazetteer":
        gazetteer = GazetteerHarvester(
            start_date=start_date,
            output_directory=parsed['target'],
            output_format=parsed['format']
        )
        gazetteer.start()
    elif parsed['sources'] == "loc":
        print("Todo: Harvest LoC")
    else:
        gazetteer = GazetteerHarvester(
            start_date=start_date,
            output_directory=parsed['target'],
            output_format=parsed['format']
        )
        gazetteer.start()
        print("Todo: Harvest LoC")

    with open(date_log_path, 'w') as log:
        log.write(datetime.date.today().isoformat())
