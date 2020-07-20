import requests

import grequests  # used for asynchronous/parallel queries
from pymarc import Record, Field, record_to_xml
import datetime
import logging
import re
import math

from harvesters.helper import MARCXML_OPENING_ELEMENTS, MARCXML_CLOSING_ELEMENTS


class GazetteerHarvester:
    _base_url = 'https://gazetteer.dainst.org'
    _output_file = None
    _file_writer = None
    _cached_places = dict()
    _batch_size = 250
    _processed_batches_counter = 0
    _gazId_pattern = re.compile('.*/place/(\d+)$')

    def _scrub_coordinates_and_polygons(self, place):
        if 'prefLocation' in place:
            del place['prefLocation']
        if 'locations' in place:
            del place['locations']

        return place

    def _extract_gaz_id_from_url(self, url):
        match = self._gazId_pattern.match(url)
        return match.group(1)

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
        self.logger.error(e)
        if type(e) is ValueError:
            self.logger.error("JSON decoding fails!")
        elif type(e) is requests.exceptions.RequestException:
            self.logger.error("Gazetteer service request fails!")
            self.logger.error("Request: {0}".format(e.request))
            self.logger.error("Response: {0}".format(e.response))
        elif type(e) is requests.exceptions.HTTPError and e.response.status_code == 500:
            return self._retry_query(e.request.url, retries_left)
        elif type(e) is requests.exceptions.ConnectionError:
            return self._retry_query(e.request.url, retries_left)

    def _create_marc_record(self, place):
        def create_x51_heading_subfield(data):
            if 'language' not in data or data['language'] == '':
                return ['a', data['title']]
            else:
                return ['a', data['title'], 'l', data['language']]

        field_001 = Field(tag='001', data="iDAI.gazetteer-{0}".format(place['gazId']))
        field_003 = Field(tag='003', data="DE-2553")

        fixed_length_data_elements = datetime.date.today().isoformat().replace('-', '')
        fixed_length_data_elements += '|'           # index 6
        fixed_length_data_elements += '|'
        fixed_length_data_elements += '|'
        fixed_length_data_elements += '|'
        fixed_length_data_elements += 'z'
        fixed_length_data_elements += 'z'           # 11
        fixed_length_data_elements += 'z'
        fixed_length_data_elements += '|'
        fixed_length_data_elements += '|'
        fixed_length_data_elements += '|'
        fixed_length_data_elements += '|'
        fixed_length_data_elements += 'd'           # 17
        fixed_length_data_elements += '          '  # 27
        fixed_length_data_elements += '|'
        fixed_length_data_elements += '|'
        fixed_length_data_elements += ' '
        fixed_length_data_elements += 'b'
        fixed_length_data_elements += 'n'
        fixed_length_data_elements += '|'
        fixed_length_data_elements += '    '        # 37
        fixed_length_data_elements += ' '
        fixed_length_data_elements += ' '
        field_008 = Field(tag='008', data=fixed_length_data_elements)

        field_024 = Field(
            tag=24, indicators=(7, ' '), subfields=[
                'a', place['gazId'],
                '2', "iDAI.gazetteer",
                '9', "iDAI.gazetteer-{0}".format(place['gazId'])
            ]
        )

        field_040 = Field(
            tag=40, indicators=(' ', ' '), subfields=[
                'a', 'Deutsches Archäologisches Institut'
            ]
        )

        if 'prefName' in place:
            field_151 = Field(
                tag=151, indicators=(' ', ' '), subfields=create_x51_heading_subfield(place['prefName']) + [
                    '1', "{0}/doc/{1}".format(self._base_url, place['gazId'])
                ]
            )
        else:
            self.logger.warning("No 'prefName' for place:")
            self.logger.warning(place)
            return None

        fields_451 = []
        if 'names' in place:
            for variant_name in place['names']:
                fields_451.append(Field(
                    tag=451, indicators=(' ', ' '), subfields=create_x51_heading_subfield(variant_name)
                ))

        order = 1
        fields_551 = []
        added_parents = []
        if 'parent' in place:
            parent_uri = place['parent']
            while parent_uri is not None:

                if parent_uri not in self._cached_places:
                    self.logger.debug("Parent:{0}/doc/{1}.json not in cached places!"
                                      .format(self._base_url, self._extract_gaz_id_from_url(parent_uri)))
                    self.logger.debug("Child: {0}/doc/{1}.json"
                                      .format(self._base_url, self._extract_gaz_id_from_url(place['@id'])))
                    self.logger.debug("...running additional query.")

                    url = "{0}/doc/{1}.json".format(self._base_url, self._extract_gaz_id_from_url(parent_uri))

                    response = requests.get(url)
                    parent = response.json()
                    self._cached_places[parent_uri] = self._scrub_coordinates_and_polygons(parent)

                current = self._cached_places[parent_uri]

                if 'prefName' in current:
                    fields_551.append(Field(
                        tag=551, indicators=(' ', ' '), subfields=create_x51_heading_subfield(current['prefName']) + [
                            'x', "part of", 'i', "{0}".format(order), '0', "iDAI.gazetteer-{0}".format(current['gazId'])
                        ]
                    ))
                elif 'accessDenied' in current and current['accessDenied'] is True:
                    break
                else:
                    self.logger.warning("No prefName for: {0}/doc/{1}.json".format(self._base_url, current['gazId']))

                order += 1
                added_parents += [parent_uri]

                if 'parent' in current:
                    if current['parent'] in added_parents:
                        self.logger.error("Tried adding {0} as a parent a second time. This should not happen.".format(parent_uri))
                        self.logger.error("Gazetteer ID: {0}".format(place['gazId']))
                        parent_uri = None
                    else:
                        parent_uri = current['parent']
                else:
                    parent_uri = None

        record = Record(force_utf8=True)
        record.leader = record.leader[0:6] + 'z' + record.leader[7:]
        record.add_field(field_001)
        record.add_field(field_003)
        record.add_field(field_008)
        record.add_field(field_024)
        record.add_field(field_040)

        record.add_field(field_151)

        if fields_451:
            for field in fields_451:
                record.add_field(field)

        if fields_551:
            for field in fields_551:
                record.add_field(field)

        return record

    def _write_place(self, place):

        record = self._create_marc_record(place)

        if record is None:
            self.logger.warning("Skipping place:")
            self.logger.warning(place)
        elif self._format == 'marc':
            self._output_file.write(record.as_marc())
        elif self._format == 'marcxml':
            self._output_file.write(record_to_xml(record))

    def _collect_places_data(self, batch):
        self.logger.info("Retrieving place data for batch #{0}...".format(self._processed_batches_counter + 1))
        url_list = []
        for item in batch:
            if item['@id'] in self._cached_places:
                continue
            url_list.append("{0}/doc/{1}.json".format(self._base_url, item["gazId"]))

        places = []

        try:
            rs = [grequests.get(url) for url in url_list]
            responses = grequests.map(rs)
            for response in responses:
                if response is None:
                    continue

                response.raise_for_status()
                place = response.json()

                self._cached_places[place['@id']] = self._scrub_coordinates_and_polygons(place)

                places.append(place)
        except Exception as e:
            self._handle_query_exception(e, 5)

        # Also load parent and ancestor places of the current batch (in case they are not already cached)        url_list = []
        for place in places:
            if 'parent' in place and place['parent'] not in self._cached_places:
                url_list.append(
                    "{0}/doc/{1}.json".format(self._base_url, self._extract_gaz_id_from_url(place["parent"]))
                )
            if 'ancestors' in place:
                for ancestor in place['ancestors']:
                    if ancestor not in self._cached_places:
                        url_list.append(
                            "{0}/doc/{1}.json".format(self._base_url, self._extract_gaz_id_from_url(ancestor))
                        )

        url_list = list(set(url_list))

        try:
            rs = [grequests.get(url) for url in url_list]
            responses = grequests.map(rs)
            for response in responses:
                response.raise_for_status()
                place = response.json()

                places.append(place)

                self._cached_places[place['@id']] = self._scrub_coordinates_and_polygons(place)
        except Exception as e:
            self._handle_query_exception(e, 5)

        self._processed_batches_counter += 1
        return places

    def _get_batch(self, scroll_id=None):
        if scroll_id is None:
            url = "{0}/search.json?limit={1}&scroll=true&q={2}"\
                .format(self._base_url, self._batch_size, self.timeframe_query)
        else:
            url = "{0}/search.json?limit={1}&scrollId={2}&q={3}"\
                .format(self._base_url, self._batch_size, scroll_id, self.timeframe_query)

        try:
            response = requests.get(url=url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self._handle_query_exception(e, 5)

    def start(self):
        with open(self._output_path, 'wb') as output_file:
            self._output_file = output_file
            if self._format == 'marcxml':
                self._output_file.write(MARCXML_OPENING_ELEMENTS)

            batch = self._get_batch()
            total = batch['total']
            scroll_id = batch['scrollId']

            self.logger.info("{0} places in query total.".format(total))
            self.logger.info("Number of batches: {0}".format(math.ceil(total / self._batch_size)))
            places = self._collect_places_data(batch['result'])

            for place in places:
                self._write_place(place)

            next_batch = self._get_batch(scroll_id)
            while next_batch['result']:
                places = self._collect_places_data(next_batch['result'])

                for place in places:
                    self._write_place(place)

                next_batch = self._get_batch(scroll_id)

            if self._format == 'marcxml':
                self._output_file.write(MARCXML_CLOSING_ELEMENTS)

    def __init__(self, start_date, output_directory, output_format):

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        if start_date is None:
            self.timeframe_query = ''
        else:
            self.timeframe_query = \
                "lastChangeDate:[{0}%20TO%20{1}]".format(start_date.isoformat(), datetime.date.today().isoformat())

        if output_format == 'marc':
            suffix = '.mrc'
        elif output_format == 'marcxml':
            suffix = '.marcxml'
        else:
            self.logger.error("Unknown format: {0}, aborting.".format(output_format))
            return

        self._output_path = "{0}gazetteer_authority{1}".format(output_directory, suffix)
        self._format = output_format
