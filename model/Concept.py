from typing import Dict


class Concept:

    def __init__(self, concept_id: str,
                 pref_label_list: Dict[str, str],
                 alt_label_list: Dict[str, str],
                 broader_list: Dict[str, Dict[str, str]],
                 definition_list: Dict[str, str]):

        self.id: str = concept_id
        self.pref_label_list: Dict[str, str] = pref_label_list
        self.alt_label_list: Dict[str, str] = alt_label_list
        self.broader_list: Dict[str, Dict[str, str]] = broader_list
        self.definition_list: Dict[str, str] = definition_list

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)
