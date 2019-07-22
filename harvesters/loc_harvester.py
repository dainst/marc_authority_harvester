import requests
import grequests  # used for asynchronous/parallel queries
from pymarc import record_to_xml, marcxml
import datetime
import logging
import dateutil.parser
import pytz

from lxml import etree
from io import BytesIO, StringIO


class LocHarvester:

    _NS = {'default': "http://www.w3.org/2005/Atom"}

    _subscribed_feeds = [
        "http://id.loc.gov/authorities/names/feed/",
        "http://id.loc.gov/authorities/subjects/feed/"
    ]

    _output_file_handlers = {}

    _start_date = None
    _file_suffix = None
    _batch_size = 300

    def _collect_entries_since_start_date(self, feed, start_date):

        result = []

        feed_page_index = 1
        feed_items = self._read_feed("{0}{1}".format(feed, feed_page_index), start_date)
        feed_page_index += 1

        result.extend(feed_items)

        while feed_items:
            feed_items = self._read_feed("{0}{1}".format(feed, feed_page_index), start_date)
            feed_page_index += 1
            result.extend(feed_items)

        # If an entry was edited twice or more within the harvested timespan, it will show up multiple times in the
        # result list.
        self.logger.debug("Filtering duplicate results, current:  {0}".format(len(result)))
        result = list(set(result))
        self.logger.debug("                             filtered: {0}".format(len(result)))

        return result

    def _collect_entry_data(self, link_list):

        records = []

        try:
            rs = [grequests.get(url) for (url, _date) in link_list]
            responses = grequests.map(rs)
            for response in responses:
                if response is None:
                    continue

                response.raise_for_status()
                record = marcxml.parse_xml_to_array(StringIO(BytesIO(response.content).read().decode('UTF-8')))[0]
                records.append(record)
            return records
        except Exception as e:
            self._handle_query_exception(e, 5)

    def _retry_query(self, url, retries_left):
        self.logger.info("  Retrying {0}...".format(url))
        try:
            if retries_left == 0:
                self.logger.info("  No retries left for {0}.".format(url))
                return None
            else:
                response = requests.get(url=url)
                response.raise_for_status()
                self.logger.info("  Retry successful.")
                return response.json()
        except Exception as e:
            self._handle_query_exception(e, retries_left - 1)

    def _handle_query_exception(self, e, retries_left):

        if retries_left > 5:
            return self._retry_query(e.request.url, retries_left)
        else:
            self.logger.error("Maximum number of retries reached, aborting.")
            self.logger.error("Unhandled error: ")
            self.logger.error("Request: {0}".format(e.request))
            self.logger.error("Response: {0}".format(e.response))

    def _read_feed(self, url, min_date):
        res = requests.get(url, headers={"Accept": "application/xml"}, cookies={"Cookie": "?"})

        xml_element_tree = etree.parse(BytesIO(res.content))

        entries = xml_element_tree.xpath(
            "//default:entry", namespaces=self._NS
        )

        result = []
        for entry in entries:
            link = entry.xpath(
                './default:link[@rel="alternate" and @type="application/marc+xml"]/@href', namespaces=self._NS
            )[0]
            timestamp = entry.xpath(
                './default:updated/text()', namespaces=self._NS
            )[0]

            date = dateutil.parser.parse(timestamp)

            if date < pytz.UTC.localize(min_date):
                continue

            result.append((link, date))

        return result

    def _write_records(self, records, file_handler_mapping):
        for record in records:
            if record.get_fields('100'):
                if self._format == 'marc':
                    file_handler_mapping['100'].write(record.as_marc())
                elif self._format == 'marcxml':
                    file_handler_mapping['100'].write(record_to_xml(record))
            elif record.get_fields('110'):
                if self._format == 'marc':
                    file_handler_mapping['110'].write(record.as_marc())
                elif self._format == 'marcxml':
                    file_handler_mapping['110'].write(record_to_xml(record))
            elif record.get_fields('111'):
                if self._format == 'marc':
                    file_handler_mapping['111'].write(record.as_marc())
                elif self._format == 'marcxml':
                    file_handler_mapping['111'].write(record_to_xml(record))
            elif record.get_fields('130'):
                if self._format == 'marc':
                    file_handler_mapping['130'].write(record.as_marc())
                elif self._format == 'marcxml':
                    file_handler_mapping['130'].write(record_to_xml(record))

    def start(self):

        if self._start_date is None:
            self.logger.warning("Harvesting without start date is not supported, aborting.")
            return

        with open("{0}loc_personal_names{1}".format(self._output_directory, self._suffix), 'wb') as personal_names_fh, \
             open("{0}loc_corporate_names{1}".format(self._output_directory, self._suffix), 'wb') as corporate_names_fh, \
             open("{0}loc_meeting_names{1}".format(self._output_directory, self._suffix), 'wb') as meeting_names_fh, \
             open("{0}loc_uniform_titles{1}".format(self._output_directory, self._suffix), 'wb') as uniform_titles_fh:

            heading_to_file_handler = \
                {
                    '100': personal_names_fh,
                    '110': corporate_names_fh,
                    '111': meeting_names_fh,
                    '130': uniform_titles_fh
                }

            for feed in self._subscribed_feeds:
                self.logger.info("Reading feed: {0}.".format(feed))
                entry_links = self._collect_entries_since_start_date(feed, self._start_date)

                batched_list = []
                for i in range(0, len(entry_links), self._batch_size):
                    batched_list.append(entry_links[i:i + self._batch_size])

                self.logger.info("Collecting entry data batches and writing results to file. "
                                 "({1} entries in {1} batches)".format(len(entry_links), len(batched_list)))
                counter = 1
                for batch in batched_list:
                    self.logger.info("  Processing batch #{0} of {1}.".format(counter, len(batched_list)))
                    self._write_records(self._collect_entry_data(batch), heading_to_file_handler)
                    counter += 1

    def __init__(self, start_date, output_directory, output_format):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        if start_date is None:
            self.logger.warning("Harvesting without start date is not supported.")

        # LoC applies changes at 5:00 EST, which would be 11:00 local time in Berlin.
        # We won't be running the script in the daytime, so we force the script to look for timestamps actually one day
        # earlier than actually requested. TODO: Maybe find generalized solution (without expecting UTC+1 timezone)
        if datetime.datetime.now().time().hour < 12:
            new_date = start_date - datetime.timedelta(days=1)
            self.logger.warning("Script running before LoC applies changes to their update feed, "
                                "also harvesting changes from {0}.".format(new_date.isoformat()))
            self._start_date = new_date
        else:
            self._start_date = start_date
        self._output_directory = output_directory

        if output_format == 'marc':
            self._suffix = '.mrc'
        elif output_format == 'marcxml':
            self._suffix = '.marcxml'

        self._format = output_format
