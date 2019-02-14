# encoding: utf-8
from __future__ import unicode_literals, print_function
from collections import OrderedDict, defaultdict

import attr
from clldutils.misc import slug
from clldutils.path import Path
from clldutils.text import split_text, strip_brackets
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.dataset import Concept, Language

import csv
import lingpy
from sinopy.segments import get_structure

class Dataset(BaseDataset):
    id = 'yanglalo'
    dir = Path(__file__).parent
    concept_class = Concept
    language_class = Language

    def cmd_download(self, **kw):
        # nothing to do, as the raw data is in the repository
        pass

    def cmd_install(self, **kw):
        # cache languages

        # cache concepts

        # read raw data for later addition
        filename = self.dir.joinpath('raw', 'raw_data.tsv').as_posix()
        with open(filename) as csvfile:
            reader = csv.DictReader(csvfile, delimiter='\t')
            raw_entries = [row for row in reader]

        # add information to dataset
        with self.cldf as ds:
            ds.add_sources(*self.raw.read_bib())

            # add languages to dataset
            languages = []
            for language in self.languages:
                # add to dataset
                ds.add_language(
                    ID=language['ID'],
                    Glottocode=language['GLOTTOLOG'],
                    Name=language['NAME']
                )

                # cache for later selecting the columns from the raw data
                languages.append(language['ID'])

            # add concepts to dataset
            for concept in self.concepts:
                ds.add_concept(
                    ID=concept['NUMBER_IN_SOURCE'],
                    Name=concept['ENGLISH'],
                    Concepticon_ID=concept['CONCEPTICON_ID'],
                    Concepticon_Gloss=concept['CONCEPTICON_GLOSS'],
                )

            # add lexemes
            for cogid, entry in enumerate(raw_entries):
                # get the parameter frm the number in source, skipping over
                # non-published data
                parameter = entry['Parameter'].split('.')[0]
                if parameter:
                    for language in languages:
                        value = entry[language]
                        value = value.replace(';', ',')
                        value = value.replace('烂饭', '')
                        forms = [form.strip() for form in value.split(',')]
                        forms = [form for form in forms if form]
                        for form in forms:
                            # do replacements fixing BIPA
                            # TODO: move to orthographic profile later
                            replacements = [
                                ('H', '¹'), # superscript H is for aspiration
                                ('ᶫ', '³'),
                                ('ɤ̪', 'ɤ'),
                                ('(', ''), # keep everything in parentheses
                                (')', ''),
                            ]

                            for repl in replacements:
                                form = form.replace(repl[0], repl[1])

                            # perform segmentation with sinopy: extract
                            # syllabic information and then extract the
                            # segmented tokens (with the morpheme boundary)
                            syllables = [
                                syl[0] for syl in
                                get_structure(form, zipped=True)]
                            segments = [
                                [segment[1] for segment in syllable] + ['+']
                                for syllable in syllables
                            ]

                            # flatten the list of segments, as requested by
                            # lexibank, removing the final morpheme mark
                            segments = [seg for syl in segments for seg in syl]
                            segments = segments[:-1]

                            for row in ds.add_lexemes(
                                Language_ID=language,
                                Parameter_ID=parameter,
                                Form=form,
                                Value=value,
                                Segments=segments,
                                Source=['Yang2011Lalo'],
                                Cognacy=cogid,
                                ):
                                ds.add_cognate(
                                    lexeme = row,
                                    Cognateset_ID=cogid,
                                    Source=['Yang2011Lalo'],
                                    Alignment_Source='Yang2011Lalo',
                                )
