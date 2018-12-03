import argparse
import datetime
import logging
import os

import gevent.monkey as monkey
monkey.patch_all(thread=False, select=False)

from harvesters.gazetteer_harvester import GazetteerHarvester
from harvesters.loc_harvester import LocHarvester

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


def create_default_output_directory(output_path):
    path = f'{output_path}/{datetime.date.today().isoformat()}/'
    if not os.path.exists(path):
        os.makedirs(path)

    return path


parser = argparse.ArgumentParser(description='Harvest MARC authority data from various data providers.')
parser.add_argument('-f', '--format', type=str, nargs='?', default='marc', choices=['marc', 'marcxml'],
                    help="The desired output format.")
parser.add_argument('-s', '--sources', type=str, nargs='?', default='all', choices=['all', 'gazetteer', 'loc'],
                    help="The desired data providers.")
parser.add_argument('-t', '--target', type=is_writable_directory, nargs='?', default='./output',
                    help="Specificy output directory.")

group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('-c', '--continue', action='store_true',
                   help="Continue from last time the script was run.")
group.add_argument('-d', '--date', type=validate_date, nargs='?',
                   help="Harvest everything since a given date, date ISO pattern: YYYY-MM-DD.")
group.add_argument('-o', '--offset', type=is_positive_number, nargs='?',
                   help=f"Use a day offset from the current date to specify the starting date.")


if __name__ == '__main__':
    options = vars(parser.parse_args())
    final_output_path = create_default_output_directory(options['target'])
    date_log_path = f"{options['target']}/last_run_date.log"

    if options['continue']:
        with open(date_log_path, 'r') as log:
            start_date = datetime.date.fromisoformat(log.readline().rstrip('\n'))
    elif options['date']:
        start_date = options['date']
    else:
        start_date = datetime.date.today() - datetime.timedelta(days=options['offset'])

    logger.info(f"Harvesting all data changes since {start_date.isoformat()}.")

    if options['sources'] == "gazetteer":
        gazetteer = GazetteerHarvester(
            start_date=start_date,
            output_directory=final_output_path,
            output_format=options['format']
        )
        gazetteer.start()
    elif options['sources'] == "loc":
        loc = LocHarvester(
            start_date=start_date,
            output_directory=final_output_path,
            output_format=options['format']
        )
        loc.start()
    else:
        gazetteer = GazetteerHarvester(
            start_date=start_date,
            output_directory=final_output_path,
            output_format=options['format']
        )
        gazetteer.start()
        loc = LocHarvester(
            start_date=start_date,
            output_directory=final_output_path,
            output_format=options['format']
        )
        loc.start()

    with open(date_log_path, 'w') as log:
        log.write(datetime.date.today().isoformat())
