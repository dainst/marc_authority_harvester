import logging
import requests

from lxml import etree
from io import BytesIO
from lxml.etree import ElementTree, Element
from pymarc import Record, Field, record_to_xml

import dateutil.parser
import datetime

from harvesters.helper import MARCXML_OPENING_ELEMENTS, MARCXML_CLOSING_ELEMENTS


class ThesauriHarvester:

    _IDAI_WORLD_THESAURI_SERVICE_ENDPOINT = 'http://thesauri.dainst.org/'

    _NS = {
        'default': _IDAI_WORLD_THESAURI_SERVICE_ENDPOINT,
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
        'owl': 'http://www.w3.org/2002/07/owl#',
        'skos': 'http://www.w3.org/2004/02/skos/core#',
        'dct': 'http://purl.org/dc/terms/',
        'foaf': 'http://xmlns.com/foaf/spec/',
        'void': 'http://rdfs.org/ns/void#',
        'iqvoc': 'http://try.iqvoc.net/schema#',
        'skosxl': 'http://www.w3.org/2008/05/skos-xl#',
        'schema': 'http://thesauri.dainst.org/schema#'
    }

    _root_concept = "{0}_fe65f286".format(
        _IDAI_WORLD_THESAURI_SERVICE_ENDPOINT,
    )
    _oldest_date = None
    _output_file = None
    _file_writer = None
    _cached_pref_labels = dict()

    def _harvest_concept(self, uri):
        try:
            response = requests.get(url='{0}.rdf'.format(uri))
            response.raise_for_status()

            root = etree.parse(BytesIO(response.content))

            pref_label = root.xpath(
                './rdf:Description[@rdf:about="{0}"]/skos:prefLabel/text()',
                namespaces=self._NS
            )

            is_absolute_root = root.find(
                './/skos:topConceptOf',
                namespaces=self._NS
            )
            change_dates = root.xpath(
                './rdf:Description/skos:changeNote/rdf:Description/dct:modified/text()',
                namespaces=self._NS
            )

            # If there have been no changes, check if creation date falls within timeframe instead.
            if change_dates == []:
                change_dates = root.xpath(
                    './rdf:Description/skos:changeNote/rdf:Description/dct:created/text()',
                    namespaces=self._NS
                )

            is_within_timeframe = False

            if self._oldest_date is None:
                is_within_timeframe = True
            else:
                for timestamp in change_dates:
                    date = datetime.datetime.combine(
                        dateutil.parser.parse(timestamp, ignoretz=True),
                        datetime.datetime.min.time()
                    )

                    if date < self._oldest_date:
                        continue
                    else:
                        is_within_timeframe = True

            if is_absolute_root is not None:
                self.logger.info('Skipping root concept {0}.'.format(uri))
            else:
                self._cached_pref_labels[uri] = pref_label
                if not is_within_timeframe:
                    self.logger.debug('No changes to {0} within timeframe.'. format(uri))
                else:
                    record = self._create_marc_record(root, uri)
                    if self._format == 'marc':
                        self._output_file.write(record.as_marc())
                    elif self._format == 'marcxml':
                        self._output_file.write(record_to_xml(record))

            narrower_concept_uris = root.xpath(
                '//skos:narrower/@rdf:resource', namespaces=self._NS
            )

            for uri in narrower_concept_uris:
                self._harvest_concept(uri)
        except requests.exceptions.HTTPError as e:
            self.logger.error(e)

    def _create_marc_record(self, root, uri):
        source = 'iDAI.thesauri'
        thesaurus_id = uri.rsplit('/', 1)[1]

        field_001 = Field(tag='001', data=source + thesaurus_id)
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
            tag=24,
            indicators=(7, ' '),
            subfields=[
                'a', thesaurus_id,
                '2', source,
                '9', source + thesaurus_id
            ]
        )

        field_040 = Field(
            tag=40,
            indicators=(' ', ' '),
            subfields=['a', 'Deutsches Archäologisches Institut']
        )

        main_description_element = root.xpath(
            './rdf:Description[@rdf:about="{0}"]'.format(uri),
            namespaces=self._NS
        )[0]

        pref_label_value = main_description_element.xpath(
            './skos:prefLabel[@xml:lang="de"]/text()',
            namespaces=self._NS
        )

        if not pref_label_value:
            self.logger.warning('No german pref label for {0}.'.format(uri))

        field_150 = Field(
            tag=150,
            indicators=(' ', ' '),
            subfields=[
                'a', str(pref_label_value[0]),
                'l', 'de'
            ]
        )

        fields_450 = []
        alt_language_pref_elements = main_description_element.xpath(
            './skos:prefLabel[not(@xml:lang="de")]',
            namespaces=self._NS
        )

        for element in alt_language_pref_elements:
            label = element.xpath(
                './text()', namespaces=self._NS
            )[0]

            language = element.xpath(
                './@xml:lang', namespaces=self._NS
            )[0]

            field = Field(
                tag=450,
                indicators=(' ', ' '),
                subfields=[
                    'a', str(label),
                    'l', language,
                    'i', 'pref label'
                ]
            )

            fields_450.append(field)

        alt_label_elements = main_description_element.xpath(
            './skos:altLabel',
            namespaces=self._NS
        )

        for element in alt_label_elements:
            label = element.xpath(
                './text()', namespaces=self._NS
            )[0]

            language = element.xpath(
                './@xml:lang', namespaces=self._NS
            )[0]

            field = Field(
                tag=450,
                indicators=(' ', ' '),
                subfields=[
                    'a', str(label),
                    'l', language,
                    'i', 'alt label'
                ]
            )

            fields_450.append(field)

        fields_550 = []

        broader_uri = main_description_element.xpath(
            './skos:broader/@rdf:resource',
            namespaces=self._NS
        )[0]

        broader_label = root.xpath(
            './rdf:Description[@rdf:about="{0}"]/skos:prefLabel/text()'.format(broader_uri),
            namespaces=self._NS
        )[0]

        broader_language = root.xpath(
            './rdf:Description[@rdf:about="{0}"]/skos:prefLabel/@xml:lang'.format(broader_uri),
            namespaces=self._NS
        )[0]

        broader_field = Field(
            tag=550,
            indicators=(' ', ' '),
            subfields=[
                'a', str(broader_label),
                'l', str(broader_language),
                '0', source + broader_uri.rsplit('/', 1)[1],
                '1', broader_uri,
                'i', 'broader concept'
            ]
        )
        fields_550.append(broader_field)

        definition_elements = main_description_element.xpath(
            './skos:definition',
            namespaces=self._NS
        )

        fields_677 = []

        for element in definition_elements:
            definition_text = element.xpath(
                './text()',
                namespaces=self._NS
            )[0]
            definition_lang = element.xpath(
                './@xml:lang',
                namespaces=self._NS
            )[0]
            field = Field(
                tag=677,
                indicators=(' ', ' '),
                subfields=[
                    'a', str(definition_text),
                    'l', str(definition_lang),
                    'v', source
                ]
            )

            fields_677.append(field)

        record = Record(force_utf8=True)
        record.leader = record.leader[0:6] + 'z' + record.leader[7:]
        record.add_field(field_001)
        record.add_field(field_003)
        record.add_field(field_008)
        record.add_field(field_024)
        record.add_field(field_040)
        record.add_field(field_150)

        for field in fields_450:
            record.add_field(field)

        for field in fields_550:
            record.add_field(field)

        for field in fields_677:
            record.add_field(field)

        return record

    def start(self):
        self.logger.info("Harvesting iDAI.thesauri, starting with root {0}.".format(self._root_concept))
        with open(self._output_path, 'wb') as output_file:
            self._output_file = output_file

            if self._format == 'marcxml':
                self._output_file.write(MARCXML_OPENING_ELEMENTS)
            self._harvest_concept(self._root_concept)
            if self._format == 'marcxml':
                self._output_file.write(MARCXML_CLOSING_ELEMENTS)

    def __init__(self, start_date, output_directory, output_format):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        if start_date is not None:
            self._oldest_date = start_date

        if output_format == 'marc':
            suffix = '.mrc'
        elif output_format == 'marcxml':
            suffix = '.marcxml'
        else:
            self.logger.error("Unknown format: {0}, aborting.".format(output_format))
            return

        self._output_path = "{0}thesauri_authority{1}".format(output_directory, suffix)
        self._format = output_format
