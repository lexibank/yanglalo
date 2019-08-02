import csv

from clldutils.path import Path
from clldutils.text import split_text
from pylexibank.dataset import NonSplittingDataset, Language, Concept
from pylexibank.util import pb
import attr

@attr.s
class HLanguage(Language):
    Location = attr.ib(default=None)
    Source_ID = attr.ib(default=None)

@attr.s
class HConcept(Concept):
    Chinese_Gloss = attr.ib(default=None)



class Dataset(NonSplittingDataset):
    id = "yanglalo"
    dir = Path(__file__).parent
    concept_class = HConcept
    language_class = HLanguage

    def cmd_download(self, **kw):
        # nothing to do, as the raw data is in the repository
        pass

    def clean_form(self, item, form):
        if form:
            form = split_text(form, separators=",")[0]
            return form

    def cmd_install(self, **kw):

        # read raw data for later addition
        filename = self.dir.joinpath("raw", "raw_data.tsv").as_posix()
        with open(filename) as csvfile:
            reader = csv.DictReader(csvfile, delimiter="\t")
            raw_entries = [row for row in reader]

        # add information to dataset
        with self.cldf as ds:
            ds.add_sources(*self.raw.read_bib())

            # add languages to dataset

            languages = {k['Source_ID']: k['ID'] for k in self.languages}
            ds.add_languages()
            
            concepts = {c.attributes['number_in_source']: c.id for c in
                    self.conceptlist.concepts.values()}
            for concept in self.conceptlist.concepts.values():
                ds.add_concept(
                        ID=concept.id,
                        Name=concept.english,
                        Concepticon_ID=concept.concepticon_id,
                        Concepticon_Gloss=concept.concepticon_gloss,
                        Chinese_Gloss = concept.attributes['chinese']
                        )
            # add lexemes
            for cogid, entry in pb(enumerate(raw_entries), desc="cldfify",
                    total=len(raw_entries)):
                # get the parameter frm the number in source, skipping over
                # non-published data
                parameter = entry["Parameter"].split('.')[0]
                if parameter:
                    for language in languages:
                        for row in ds.add_lexemes(
                            Language_ID=languages[language],
                            Parameter_ID=concepts[parameter],
                            Value=entry[language],
                            Source=["Yang2011Lalo"],
                            Cognacy=cogid,
                        ):
                            ds.add_cognate(
                                lexeme=row,
                                Cognateset_ID=cogid,
                                Source=["Yang2011Lalo"],
                                Alignment_Source="",
                            )
