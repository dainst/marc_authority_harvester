import argparse
import datetime
import logging
import os
import dateutil.parser

import gevent.monkey as monkey
monkey.patch_all(thread=False, select=False)

from harvesters.gazetteer_harvester import GazetteerHarvester
from harvesters.loc_harvester import LocHarvester
from harvesters.thesauri_harvester import ThesauriHarvester

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(format='%(asctime)s-%(levelname)s-%(name)s - %(message)s')


def validate_date(s: str):
    try:
        return dateutil.parser.parse(s)
    except ValueError:
        msg = "Not a valid date: '{0}', expected pattern: YYYY-MM-DD".format(s)
        raise argparse.ArgumentTypeError(msg)


def is_positive_number(i: str):
    value = int(i)
    if value <= 0:
        msg = "Please provide a positive date offset."
        raise argparse.ArgumentTypeError(msg)
    else:
        return value


def is_writable_directory(path: str):
    if os.path.exists(path) and (not os.path.isdir(path) or not os.access(path, os.W_OK)):
        msg = "Please provide writable directory."
        raise argparse.ArgumentTypeError(msg)
    elif not os.path.exists(path):
        os.makedirs(path)
        return path
    else:
        return path


def create_default_output_directory(output_path):
    path = "{0}/{1}/".format(output_path, datetime.date.today().isoformat())
    if not os.path.exists(path):
        os.makedirs(path)

    return path


parser = argparse.ArgumentParser(description='Harvest MARC authority data from various data providers.')
parser.add_argument('-f', '--format', type=str, nargs='?', default='marc', choices=['marc', 'marcxml'],
                    help="The desired output format.")
parser.add_argument('-s', '--sources', type=str, nargs='?', default='all', choices=['all', 'gazetteer', 'loc', 'ths'],
                    help="The desired data providers.")
parser.add_argument('-t', '--target', type=is_writable_directory, nargs='?', default='./output',
                    help="Specificy output directory.")

group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('-n', '--no-limit', action='store_true',
                   help="Get all data available without a date limit.")
group.add_argument('-c', '--continue', action='store_true',
                   help="Continue from last time the script was run.")
group.add_argument('-d', '--date', type=validate_date, nargs='?',
                   help="Harvest everything since a given date, date ISO pattern: YYYY-MM-DD.")
group.add_argument('-o', '--offset', type=is_positive_number, nargs='?',
                   help="Use a day offset from the current date to specify the starting date.")


def run_harvests(options):

    final_output_path = create_default_output_directory(options['target'])

    if options['continue']:
        date_log_path = "{0}/last_run_date.log".format(options['target'])
        if not os.path.exists(date_log_path):
            logger.warning("Unable to continue harvest, because no file exists"
                           "at {0}.".format(date_log_path))
            return
        with open(date_log_path, 'r') as log:
            start_date = dateutil.parser.parse(log.readline().rstrip('\n'))

    elif options['date']:
        start_date = options['date']

    elif options['offset']:
        start_date = datetime.date.today() - datetime.timedelta(days=options['offset'])

    else:
        start_date = None

    if start_date is not None:
        logger.info("Harvesting all data changes since {0}.".format(start_date.isoformat()))
    else:
        logger.info("Running complete harvest.")

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
    elif options['sources'] == 'ths':
        thesaurus = ThesauriHarvester(
            start_date=start_date,
            output_directory=final_output_path,
            output_format=options['format']
        )
        thesaurus.start()

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

        thesaurus = ThesauriHarvester(
            start_date=start_date,
            output_directory=final_output_path,
            output_format=options['format']
        )
        thesaurus.start()


if __name__ == '__main__':
    options = vars(parser.parse_args())

    run_harvests(options)

    with open("{0}/last_run_date.log".format(options['target']), 'w') as log:
        log.write(datetime.date.today().isoformat())
