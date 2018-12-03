import requests
import grequests  # used for asynchronous/parallel queries
from pymarc import record_to_xml, marcxml
import datetime
import logging

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
        feed_items = self._read_feed(f"{feed}{feed_page_index}", start_date)
        feed_page_index += 1

        result.extend(feed_items)

        while feed_items:
            feed_items = self._read_feed(f"{feed}{feed_page_index}", start_date)
            feed_page_index += 1
            result.extend(feed_items)

        # If an entry was edited twice or more within the harvested timespan, it will show up multiple times in the
        # result list.
        self.logger.debug(f"Filtering duplicate results, current:  {len(result)}")
        result = list(set(result))
        self.logger.debug(f"                             filtered: {len(result)}")

        return result

    def _collect_entry_data(self, link_list):

        records = []

        def exception_handler(request, exception):
            self.logger.error(exception)
            self.logger.error(request)
            pass

        try:
            rs = [grequests.get(url) for (url, _date) in link_list]
            responses = grequests.map(rs, exception_handler=exception_handler)
            for response in responses:
                response.raise_for_status()
                record = marcxml.parse_xml_to_array(StringIO(BytesIO(response.content).read().decode('UTF-8')))[0]
                records.append(record)
            return records
        except Exception as e:
            self._handle_query_exception(e)

    def _handle_query_exception(self, e):
        self.logger.error(e)
        if type(e) is ValueError:
            self.logger.error('JSON decoding fails!')
            self.logger.error(e)
        elif type(e) is requests.exceptions.RequestException:
            self.logger.error(f'Gazetteer service request fails!')
            self.logger.error(f'Request: {e.request}')
            self.logger.error(f'Response: {e.response}')

    def _read_feed(self, url, min_date):
        self.logger.debug(url)
        res = requests.get(url, headers={"Accept": "application/xml"}, cookies={"Cookie": "?"})

        xml_element_tree: etree.ElementTree = etree.parse(BytesIO(res.content))

        entries = xml_element_tree.xpath(
            f"//default:entry", namespaces=self._NS
        )

        result = []
        for entry in entries:
            link = entry.xpath(
                f'./default:link[@rel="alternate" and @type="application/marc+xml"]/@href', namespaces=self._NS
            )[0]
            timestamp = entry.xpath(
                f'./default:updated/text()', namespaces=self._NS
            )[0]

            date = datetime.datetime.fromisoformat(timestamp).date()

            if date < min_date:
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

        with open(f"{self._output_directory}loc_personal_names{self._suffix}", 'wb') as personal_names_fh, \
             open(f"{self._output_directory}loc_corporate_names{self._suffix}", 'wb') as corporate_names_fh, \
             open(f"{self._output_directory}loc_meeting_names{self._suffix}", 'wb') as meeting_names_fh, \
             open(f"{self._output_directory}loc_uniform_titles{self._suffix}", 'wb') as uniform_titles_fh:

            heading_to_file_handler = \
                {
                    '100': personal_names_fh,
                    '110': corporate_names_fh,
                    '111': meeting_names_fh,
                    '130': uniform_titles_fh
                }

            for feed in self._subscribed_feeds:
                self.logger.info(f"Reading feed: {feed}.")
                entry_links = self._collect_entries_since_start_date(feed, self._start_date)

                batched_list = []
                for i in range(0, len(entry_links), self._batch_size):
                    batched_list.append(entry_links[i:i + self._batch_size])

                self.logger.info(f"Collecting entry data batches and writing results to file. "
                                 f"({len(entry_links)} entries in {len(batched_list)} batches)")
                counter = 1
                for batch in batched_list:
                    self.logger.info(f"  Processing batch #{counter} of {len(batched_list)}.")
                    self._write_records(self._collect_entry_data(batch), heading_to_file_handler)
                    counter += 1

    def __init__(self, start_date, output_directory, output_format):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        self._start_date = start_date
        self._output_directory = output_directory

        if output_format == 'marc':
            self._suffix = '.mrc'
        elif output_format == 'marcxml':
            self._suffix = '.marcxml'

        self._format = output_format
