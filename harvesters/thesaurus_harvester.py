import inspect
import logging
import os
import requests
import sys

from datetime import date
from io import BytesIO
from lxml import etree
from lxml.etree import ElementTree, Element
from model.Concept import Concept
from pymarc import Record, Field
from requests import Response, Timeout, HTTPError
from typing import Dict, List, Tuple

module_name = inspect.stack()[0][1].rsplit('/', 1)[1].split('.')[0]
module_logger = logging.getLogger("%s.%s" % (module_name, __name__))


class ThesaurusHarvester:
    IDAI_WORLD_THESAURI_SERVICE_ENDPOINT: str = 'http://thesauri.dainst.org/'
    IDAI_WORLD_THESAURI_SERVICE_TYPE: str = '.rdf'
    IDAI_WORLD_THESAURI_SERVICE_TIMEOUT: int = 60
    IDAI_WORLD_THESAURI_SERVICE_TIMEOUT_MAX: int = 300
    DEFAULT_OUTPUT_PATH: str = '../output/'
    DEFAULT_MARC_AUTHORITY_FILENAME: str = 'thesaurus_authority.mrc'
    DEFAULT_START_DATE_ISO_FORMATTED = ''
    DEFAULT_OFFSET: int = 0
    NS_XML: str = '{http://www.w3.org/XML/1998/namespace}'
    NS_RDF: str = '{http://www.w3.org/1999/02/22-rdf-syntax-ns#}'
    NS_SKOS: str = '{http://www.w3.org/2004/02/skos/core#}'
    NS_DAI: str = IDAI_WORLD_THESAURI_SERVICE_ENDPOINT
    broader_element_dict: Dict[str, Dict[str, str]] = {}

    def load_rdf_thesaurus_concepts(self, start_date: date, offset: int) -> List[Concept]:
        result_list: List[Concept] = []
        service_timeout: int = self.IDAI_WORLD_THESAURI_SERVICE_TIMEOUT
        search_service_url: str = self.IDAI_WORLD_THESAURI_SERVICE_ENDPOINT + 'de/koha_search.html'
        start_date_in_iso_format: str = self.DEFAULT_START_DATE_ISO_FORMATTED
        file_count: int = 0
        concept_count: int = 0
        has_elements: bool = True

        if type(start_date) is date:
            start_date_in_iso_format = start_date.isoformat()
        elif start_date is not None:
            self.logger.warning(
                f"Starting date ({start_date}) is not of type date! No starting date set for complete export.")

        if type(offset) is not int:
            self.logger.warning(f"Offset (value = {offset}) is not a integer value (type = {type(offset)})!")
            if type(offset) is str:
                offset = int(offset)
                self.logger.warning(f"Cast Offset to integer value {offset}")
            else:
                offset = self.DEFAULT_OFFSET
                self.logger.warning(f"Set Offset to default value = {offset}")

        while has_elements:
            try:
                self.logger.info('Calling iDAI Thesaurus service (' +
                                 f'start date={start_date_in_iso_format}, ' +
                                 f'offset={offset}, timeout={service_timeout}) ...')
                search_service_parameters: List[Tuple] = [('date', {start_date_in_iso_format}), ('offset', {offset})]
                response: Response = requests.get(
                    search_service_url,
                    params=search_service_parameters,
                    timeout=service_timeout)
                self.logger.info('Request url: ' + response.request.url)
                self.logger.debug('Request body: ' + str(response.request.body))
                self.logger.debug('Request headers: ' + str(response.request.headers))

                response.raise_for_status()

            except Timeout as timeout:
                self.logger.warning(f'Timeout error occurred for timeout: {service_timeout}s')
                self.logger.warning(f'Timeout exception: {timeout}')

                service_timeout += self.IDAI_WORLD_THESAURI_SERVICE_TIMEOUT
                self.logger.info(f'Increase service timeout to {service_timeout}s')

                if service_timeout > self.IDAI_WORLD_THESAURI_SERVICE_TIMEOUT_MAX:
                    self.logger.error(f'Service timeout maximum reached, further requests canceled!')
                    has_elements = False

            except HTTPError as http_err:
                self.logger.error(f'HTTP error occurred: {http_err}')

            except Exception as err:
                self.logger.error(f'Other error occurred: {err}')

            else:
                response_code: int = response.status_code
                self.logger.info(f'Response code: {response_code}')
                if response.status_code == 200:
                    response_content: bytes = response.content
                    root: ElementTree = etree.parse(BytesIO(response_content))

                    concept_description_element_list: List[Element] = root.findall(
                        f'.//{self.NS_RDF}Description/[@{self.NS_RDF}about]')

                    concept_description_element_no: int = len(concept_description_element_list)
                    if concept_description_element_no > 0:
                        file_count += 1
                        # offset += 1000
                        offset += concept_description_element_no
                        concept_list: List[Concept] = self._extract_rdf_concepts(concept_description_element_list)
                        concept_count += len(concept_list)

                        self.logger.info(
                            f'Total Result: {file_count} Service call(s), ' +
                            f'{offset} Description(s), {concept_count} Concept(s)')

                        if self.is_output_chunked:
                            self._write_marc_authority_file(concept_list, file_count, True)

                        result_list += concept_list
                    else:
                        has_elements = False

                    self.logger.info(f'More Records: {has_elements}\n')

        return result_list

    def load_rdf_broader_concept_by_reference(self, concept_ref: str) -> Element:
        concept_description_element: Element = None
        concept_ref_token: List[str] = concept_ref.rsplit('/', 1)
        self.logger.debug(f"Broader concept reference: {concept_ref}")

        concept_namespace: str = concept_ref_token[0] + '/'
        self.logger.debug(f"Broader concept namespace: {concept_namespace}")

        concept_id: str = concept_ref_token[1]
        self.logger.debug(f"Broader concept id: {concept_id}")

        if concept_namespace != self.NS_DAI:
            self.logger.error(f"Invalid broader concept reference: {concept_ref}")
        else:
            is_service_timeout: bool = True
            service_url = concept_ref + self.IDAI_WORLD_THESAURI_SERVICE_TYPE
            service_timeout: int = self.IDAI_WORLD_THESAURI_SERVICE_TIMEOUT

            self.logger.debug(
                f'Resolving concept broader resource (reference={concept_ref}, timeout={service_timeout}s) ...')
            while is_service_timeout:
                try:
                    response: Response = requests.get(url=service_url, timeout=service_timeout)
                    self.logger.debug('Request url: ' + response.request.url)
                    self.logger.debug('Request body: ' + str(response.request.body))
                    self.logger.debug('Request headers: ' + str(response.request.headers))

                    response.raise_for_status()

                except Timeout as timeout_err:
                    self.logger.warning(
                        f'Resolving concept broader resource ({concept_ref}) FAILED! {timeout_err}')

                    service_timeout += self.IDAI_WORLD_THESAURI_SERVICE_TIMEOUT
                    self.logger.info(f'Increase service timeout to {service_timeout}s')

                    if service_timeout > self.IDAI_WORLD_THESAURI_SERVICE_TIMEOUT_MAX:
                        is_service_timeout = False
                        self.logger.error(
                            f'Resolving concept broader resource ({concept_ref}) FAILED!' +
                            f'Service timeout maximum ({self.IDAI_WORLD_THESAURI_SERVICE_TIMEOUT_MAX}s) reached, ' +
                            'further requests canceled!')

                except HTTPError as http_err:
                    is_service_timeout = False
                    self.logger.error(
                        f'Resolving concept broader resource FAILED! {http_err}')

                except Exception as err:
                    is_service_timeout = False
                    self.logger.error(f'Resolving concept broader resource ({concept_ref}) FAILED! {err}')

                else:
                    is_service_timeout = False
                    response_code: int = response.status_code
                    self.logger.debug(f'Response code: {response_code}')

                    if response_code == 200:
                        response_content: bytes = response.content
                        root: ElementTree = etree.parse(BytesIO(response_content))
                        concept_description_element = root.find(
                            f'.//{self.NS_RDF}Description/[@{self.NS_RDF}about="{concept_ref}"]')

        return concept_description_element

    def _extract_rdf_concepts(self, concept_description_element_list: List[Element]) -> List[Concept]:
        concept_list: List[Concept] = []

        for concept_description_element in concept_description_element_list:
            skos_prefix: str = self.NS_SKOS.strip('{}')
            if concept_description_element.find(
                    f"{self.NS_RDF}type/[@{self.NS_RDF}resource='{skos_prefix}Concept']") is not None:
                # etree.dump(concept_description_element, pretty_print=True, with_tail=True)
                concept_id: str = concept_description_element.attrib.get(f'{self.NS_RDF}about')
                concept: Concept = self._extract_rdf_thesaurus_concept_data(concept_id, concept_description_element)
                self.logger.debug(str(concept))
                concept_list.append(concept)

        return concept_list

    def _extract_rdf_thesaurus_concept_data(self, concept_id: str, concept_description_element: Element) -> Concept:
        concept_pref_label_list: List[Element] = concept_description_element.findall(f'{self.NS_SKOS}prefLabel')
        concept_alt_label_list: List[Element] = concept_description_element.findall(f'{self.NS_SKOS}altLabel')
        concept_broader: Element = concept_description_element.find(f'{self.NS_SKOS}broader')
        concept_definition_list: List[Element] = concept_description_element.findall(f'{self.NS_SKOS}definition')

        return Concept(concept_id,
                       self._map_l10n_dependent_rdf_elements(concept_pref_label_list),
                       self._map_l10n_dependent_rdf_elements(concept_alt_label_list),
                       self._extract_rdf_broader_element_data(concept_broader),
                       self._map_l10n_dependent_rdf_elements(concept_definition_list))

    def _map_l10n_dependent_rdf_elements(self, elements: List[Element]) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for element in elements:
            result[element.get(f'{self.NS_XML}lang')] = element.text

        return result

    def _extract_rdf_broader_element_data(self, rdf_broader_element: Element) -> Dict[str, Dict[str, str]]:
        result: Dict[str, Dict[str, str]] = {}

        if rdf_broader_element is not None:
            parent_ref: str = rdf_broader_element.get(f'{self.NS_RDF}resource')
            self.logger.debug(f"Parent ref: {parent_ref}")

            if parent_ref is not None:
                parent_id: str = parent_ref.rsplit('/', 1)[1]
                self.logger.debug(f"Parent id: {parent_id}")

                if parent_id is not None and parent_id not in result.keys():
                    cached_result: Dict[str, str] = self.broader_element_dict.get(parent_id)
                    if cached_result is not None:
                        result[parent_id] = cached_result
                    else:
                        concept_description_element: Element = self.load_rdf_broader_concept_by_reference(parent_ref)
                        if concept_description_element is not None:
                            concept_pref_label_list: List[Element] = concept_description_element.findall(
                                f'{self.NS_SKOS}prefLabel')

                            result[parent_id] = self._map_l10n_dependent_rdf_elements(concept_pref_label_list)
                            self.broader_element_dict[parent_id] = self._map_l10n_dependent_rdf_elements(
                                concept_pref_label_list)

            else:
                self.logger.warning("Broader without resource attribute!")
                self.logger.warning(
                    f"Broader element: {etree.dump(rdf_broader_element, pretty_print=True, with_tail=True)}")

        return result

    def _create_marc_authority_record(self, concept: Concept) -> Record:
        source: str = 'iDAI.thesauri'
        thesaurus_id: str = concept.id.rsplit('/', 1)[1]

        field_001 = Field(tag='001', data=source + thesaurus_id)
        field_003 = Field(tag='003', data="DE-2553")

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

        fields_150 = []
        for pref_label_key, pref_label_value in concept.pref_label_list.items():
            field_150 = Field(
                tag=150,
                indicators=(' ', ' '),
                subfields=[
                    'a', pref_label_value,
                    'l', pref_label_key
                ]
            )
            fields_150.append(field_150)

        fields_450 = []
        for alt_label_key, alt_label_value in concept.alt_label_list.items():
            field_450 = Field(
                tag=450,
                indicators=(' ', ' '),
                subfields=[
                    'a', alt_label_value,
                    'l', alt_label_key
                ]
            )
            fields_450.append(field_450)

        fields_550 = []
        for broader_key, broader_value in concept.broader_list.items():
            for broader_value_key, broader_value_value in broader_value.items():
                field550 = Field(
                    tag=550,
                    indicators=(' ', ' '),
                    subfields=[
                        'a', broader_value_value,
                        'l', broader_value_key,
                        '0', source + broader_key,
                        '1', self.NS_DAI + broader_key
                    ]
                )
                fields_550.append(field550)

        fields_677 = []
        for definition_key, definition_value in concept.definition_list.items():
            field_677 = Field(
                tag=677,
                indicators=(' ', ' '),
                subfields=[
                    'a', definition_value,
                    '9', definition_key
                ]
            )
            fields_677.append(field_677)

        record = Record(force_utf8=True)
        record.leader = record.leader[0:6] + 'z' + record.leader[7:]
        record.add_field(field_001)
        record.add_field(field_003)
        record.add_field(field_024)
        record.add_field(field_040)

        for field in fields_150:
            record.add_field(field)

        for field in fields_450:
            record.add_field(field)

        for field in fields_550:
            record.add_field(field)

        for field in fields_677:
            record.add_field(field)

        return record

    def _write_marc_authority_file(self, concept_list, file_count=0, as_chunks=False):
        if as_chunks:
            filename: str = self.output_filename.rsplit('.')[0]
            filetype: str = self.output_filename.rsplit('.')[1]
            file: str = self.output_path + filename + '_' + str(file_count) + '.' + filetype
        else:
            file: str = self.output_path + self.output_filename

        self.logger.debug(f'Prepare Output file: {file}')
        with open(file, 'wb') as output_file:
            self.logger.debug(f'Concepts: {len(concept_list)}')
            for concept in concept_list:
                self.logger.debug(str(concept))
                record = self._create_marc_authority_record(concept)

                if record is None:
                    self.logger.warning("Skipping concept:")
                    self.logger.warning(str(concept))
                else:
                    self.logger.debug(record)
                    raw_marc_record: bytes = record.as_marc()
                    self.logger.debug('Raw Marc record created.')
                    output_file.write(raw_marc_record)
                    self.logger.debug('Raw Marc record written.')

        self.logger.info(f'Output file written: {file}')

    def _check_output_path(self, output_path: str) -> str:
        if not output_path.endswith('/'):
            output_path += '/'
        if not os.path.exists(output_path):
            os.makedirs(output_path)
            self.logger.info(f"Export path doesn't exist. New directory {output_path} created.")
        elif not os.path.isdir(output_path):
            self.logger.warning(f"Export path is not a directory. Using default path: {self.DEFAULT_OUTPUT_PATH}")
            output_path = self.DEFAULT_OUTPUT_PATH
        elif not os.access(output_path, os.W_OK):
            self.logger.warning(f"Export path has no write access. Using default path: {self.DEFAULT_OUTPUT_PATH}")
            output_path = self.DEFAULT_OUTPUT_PATH

        return output_path

    def _check_offset(self, offset_str: str) -> int:
        offset: int = self.DEFAULT_OFFSET

        try:
            offset = int(offset_str)
        except ValueError as ve:
            self.logger.warning(f"Exception: {ve}")
            self.logger.warning(f"Offset ({offset_str}) is not a number. Using default offset: {self.DEFAULT_OFFSET}")

        return offset

    def _check_start_date(self, start_date_in_iso_format: str) -> date:
        start_date: date = None

        if start_date_in_iso_format != 'None' and start_date_in_iso_format != 'none':
            try:
                start_date = date.fromisoformat(start_date_in_iso_format)
            except ValueError as ve:
                self.logger.warning(f"Exception: {ve}")
                self.logger.warning(f"Starting date is not in ISO format! Using no start date for complete export.")

        return start_date

    def _check_output_filename(self, output_filename: str) -> str:
        if output_filename is None or '':
            output_filename = self.DEFAULT_MARC_AUTHORITY_FILENAME
            self.logger.warning(f"No filename given, using default name: {self.DEFAULT_MARC_AUTHORITY_FILENAME}")
        else:
            filename_tokens: List[str] = output_filename.rsplit('.', 1)
            if len(filename_tokens) <= 1:
                self.logger.warning(f"Export filename '{output_filename}' has no filetype. Using 'mrc' filetype.")
            elif filename_tokens[1] != 'mrc' and filename_tokens[1] != 'mrk':
                self.logger.warning(f"Export filename '{output_filename}' has a wrong filetype. Using 'mrc' filetype.")

            output_filename += '.mrc'

        return output_filename

    def _check_is_output_chunked(self, is_output_chunked: str) -> bool:
        if is_output_chunked == 'True' or is_output_chunked == 'true':
            is_output_chunked = True
        else:
            is_output_chunked = False

        return is_output_chunked

    def run(self, start_date: str, offset: str):
        concepts = self.load_rdf_thesaurus_concepts(self._check_start_date(start_date), self._check_offset(offset))

        if not self.is_output_chunked:
            self._write_marc_authority_file(concepts)

    def __init__(self, output_path: str, output_filename: str, is_output_chunked: str):
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

        self.output_path: str = self._check_output_path(output_path)
        self.output_filename: str = self._check_output_filename(output_filename)
        self.is_output_chunked: bool = self._check_is_output_chunked(is_output_chunked)


def main(arguments: List[str]):
    harvester: ThesaurusHarvester = ThesaurusHarvester(arguments[1], arguments[2], arguments[3])
    harvester.run(arguments[4], arguments[5])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    module_logger.info(f'sys.argv: {sys.argv}')

    if len(sys.argv) != 6:
        print("Please provide as argument:")
        print("1. Export path to marc authority data (directory).")
        print("2. Export file name (e.g. 'thesaurus_authority.mrc').")
        print("3. 'True' or 'False' for chunked (depending on number off service calls) or not chunked export files.")
        print("4. Export start date in iso-format (or 'None' for complete export)")
        print("5. Offset Number (or '0' for no offset)")
        sys.exit()

    main(sys.argv)
