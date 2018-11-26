import requests

import grequests  # used for asynchronous/parallel queries
from pymarc import Record, Field, record_to_xml
import datetime
import logging
import re
import math


class GazetteerHarvester:
    _base_url: str = 'https://gazetteer.dainst.org'
    _output_file = None
    _file_writer = None
    _cached_places = dict()
    _batch_size: int = 250
    _processed_batches_counter: int = 0
    _gazId_pattern = re.compile('.*/place/(\d+)$')

    def _extract_gaz_id_from_url(self, url):
        match = self._gazId_pattern.match(url)
        return match.group(1)

    def _handle_query_exception(self, e):
        self.logger.error(e)
        if type(e) is ValueError:
            self.logger.error('JSON decoding fails!')
            self.logger.error(e)
        elif type(e) is requests.exceptions.RequestException:
            self.logger.error(f'Gazetteer service request fails!')
            self.logger.error(f'Request: {e.request}')
            self.logger.error(f'Response: {e.response}')

    def _create_marc_record(self, place):
        def create_x51_heading_subfield(data):
            if 'language' not in data or data['language'] == '':
                return ['a', data['title']]
            else:
                return ['a', data['title'], 'l', data['language']]

        field_024 = Field(
            tag=24, indicators=(' ', 7), subfields=[
                'a', place['gazId'],
                '2', "iDAI.gazetteer"
            ]
        )

        field_040 = Field(
            tag=40, indicators=(' ', ' '), subfields=[
                'a', "iDAI.gazetteer"
            ]
        )

        field_151 = None
        if 'prefName' in place:
            field_151 = Field(
                tag=151, indicators=(' ', ' '), subfields=create_x51_heading_subfield(place['prefName'])
            )

        fields_451 = []
        if 'names' in place:
            for variant_name in place['names']:
                fields_451.append(Field(
                    tag=451, indicators=(' ', ' '), subfields=create_x51_heading_subfield(variant_name)
                ))

        order = 1
        fields_551 = []
        if 'parent' in place:
            parent_uri = place['parent']
            while parent_uri is not None:

                if parent_uri not in self._cached_places:
                    self.logger.debug(f"Parent:{self._base_url}/doc/{self._extract_gaz_id_from_url(parent_uri)}.json "
                                      f"not in cached places!")
                    self.logger.debug(f"Child: {self._base_url}/doc/{self._extract_gaz_id_from_url(place['@id'])}.json")
                    self.logger.debug(f"...running additional query.")

                    url = f'{self._base_url}/doc/{self._extract_gaz_id_from_url(parent_uri)}.json'

                    response = requests.get(url)
                    parent = response.json()

                    self._cached_places[parent['@id']] = parent

                current = self._cached_places[parent_uri]

                if 'prefName' in current and 'accessDenied':
                    fields_551.append(Field(
                        tag=551, indicators=(' ', ' '), subfields=create_x51_heading_subfield(current['prefName']) + [
                            'x', "part of", 'i', f"ancestor of order {order}"
                        ]
                    ))
                elif 'accessDenied' in current and current['accessDenied'] is True:
                    break
                else:
                    self.logger.warning(f"No prefName for: {self._base_url}/doc/{current['gazId']}.json")

                order += 1
                if 'parent' in current:
                    parent_uri = current['parent']
                else:
                    parent_uri = None

        record = Record(force_utf8=True, to_unicode=True)
        record.add_field(field_024)
        record.add_field(field_040)

        if field_151 is not None:
            record.add_field(field_151)

        if fields_451 is not []:
            for field in fields_451:
                record.add_field(field)

        if fields_551 is not []:
            for field in fields_551:
                record.add_field(field)

        return record

    def _write_place(self, place):

        record = self._create_marc_record(place)

        if self._format == 'marc':
            self._output_file.write(record.as_marc())
        elif self._format == 'marcxml':
            self._output_file.write(record_to_xml(record))

    def _fetch_places_data(self, batch):
        self.logger.info(f'Retrieving place data for batch #{self._processed_batches_counter + 1}...')
        url_list = []
        for item in batch:
            if item['@id'] in self._cached_places:
                continue
            url_list.append(f'{self._base_url}/doc/{item["gazId"]}.json')

        places = []
        try:
            rs = [grequests.get(url) for url in url_list]
            responses = grequests.map(rs, exception_handler=self._handle_query_exception)
            for response in responses:
                response.raise_for_status()
                place = response.json()

                self._cached_places[place['@id']] = place

                places.append(place)
        except Exception as e:
            self._handle_query_exception(e)

        # Also load parent and ancestor places of the current batch (in case they are not already cached)
        url_list = []
        for place in places:
            if 'parent' in place and place['parent'] not in self._cached_places:
                url_list.append(f'{self._base_url}/doc/{self._extract_gaz_id_from_url(place["parent"])}.json')
            if 'ancestors' in place:
                for ancestor in place['ancestors']:
                    if ancestor not in self._cached_places:
                        url_list.append(f'{self._base_url}/doc/{self._extract_gaz_id_from_url(ancestor)}.json')

        url_list = list(set(url_list))

        try:
            rs = [grequests.get(url) for url in url_list]
            responses = grequests.map(rs, exception_handler=self._handle_query_exception)
            for response in responses:
                response.raise_for_status()
                place = response.json()

                places.append(place)

                self._cached_places[place['@id']] = place
        except Exception as e:
            self._handle_query_exception(e)

        self._processed_batches_counter += 1
        return places

    def _fetch_batch(self, offset):
        url = f'{self._base_url}/search.json?limit={self._batch_size}&offset={offset}&q={self.q}'
        self.logger.debug(url)
        try:
            response = requests.get(url=url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self._handle_query_exception(e)

    def start(self):
        with open(self._output_path, 'wb') as output_file:
            self._output_file = output_file

            batch = self._fetch_batch(0)
            total = batch['total']

            self.logger.info(f"{total} places in query total.")
            self.logger.info(f"Harvesting {math.ceil(total / self._batch_size)} batches.")
            places = self._fetch_places_data(batch['result'])

            for place in places:
                self._write_place(place)

            if total > self._batch_size:
                offset = self._batch_size
                while offset < total:
                    batch = self._fetch_batch(offset)
                    places = self._fetch_places_data(batch['result'])

                    for place in places:
                        self._write_place(place)

                    offset += self._batch_size

    def __init__(self, start_date, output_directory, output_format):

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.info('Initializing, loading data and creating mapping...')

        self.q = f'lastChangeDate:[{start_date.isoformat()}%20TO%20{datetime.date.today().isoformat()}]'

        if output_format == 'marc':
            suffix = '.mrc'
        elif output_format == 'marcxml':
            suffix = '.marcxml'
        else:
            self.logger.error(f"Unknown format: {output_format}, aborting.")
            return

        self._output_path = f"{output_directory}gazetteer_authority{suffix}"
        self._format = output_format