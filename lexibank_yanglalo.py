from pathlib import Path

import attr
import pylexibank
from clldutils.misc import slug


@attr.s
class CustomLanguage(pylexibank.Language):
    Location = attr.ib(default=None)
    Source_ID = attr.ib(default=None)
    Family = attr.ib(default="Sino-Tibetan")
    SubGroup = attr.ib(default="Lalo")


@attr.s
class CustomConcept(pylexibank.Concept):
    Chinese_Gloss = attr.ib(default=None)


class Dataset(pylexibank.Dataset):
    id = "yanglalo"
    dir = Path(__file__).parent
    writer_options = dict(keep_languages=False, keep_parameters=False)

    concept_class = CustomConcept
    language_class = CustomLanguage
    form_spec = pylexibank.FormSpec(missing_data=("烂饭", "-"), first_form_only=True)

    def cmd_download(self, args):
        supp_material_url = "https://opal.latrobe.edu.au/ndownloader/articles/21844209/versions/1"
        supp_material_sheet = "33403_SOURCE4_2_A.xlsx"
        english_supplement = "33403_SOURCE4_2_A.AppendixEwordlist.csv"

        self.raw_dir.download_and_unpack(supp_material_url)
        self.raw_dir.xlsx2csv(supp_material_sheet)

        # We do some very rough cleanup of the converted Excel sheets here.
        cleanup = [
            ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,",
            "Appendix E: Sample Lalo language data and Proto-Lalo reconstructions",
            "A. Nature",
            "B. Animals",
            "C. Plants",
            "D. Body Parts",
            "E. Society",
            "F. Material Culture",
            "G. Verbs",
            "H. Adjectives",
            "I. Miscellaneous",
        ]

        with open(self.raw_dir / english_supplement, "r+") as f:
            lines = f.readlines()
            f.seek(0)
            for line in lines:
                if not any(c in line for c in cleanup):
                    f.write(line)
            f.truncate()

    def cmd_makecldf(self, args):
        wordl_english = self.raw_dir.read_csv("33403_SOURCE4_2_A.AppendixEwordlist.csv", dicts=True)

        args.writer.add_sources()
        language_lookup = args.writer.add_languages(lookup_factory="Source_ID")

        concept_lookup = {}
        for concept in self.conceptlists[0].concepts.values():
            idx = concept.id.split("-")[-1] + "_" + slug(concept.english)
            args.writer.add_concept(
                ID=idx,
                Name=concept.english,
                Concepticon_ID=concept.concepticon_id,
                Concepticon_Gloss=concept.concepticon_gloss,
                Chinese_Gloss=concept.attributes["chinese"],
            )
            concept_lookup[concept.attributes["number_in_source"]] = idx

        for cogid, entry in pylexibank.progressbar(
            enumerate(wordl_english), desc="cldfify", total=len(wordl_english)
        ):
            parameter = entry["No."]
            if parameter in concept_lookup:
                for language in language_lookup:
                    for row in args.writer.add_forms_from_value(
                        Language_ID=language_lookup[language],
                        Parameter_ID=concept_lookup[parameter],
                        Value=entry[language],
                        Source=["Yang2011Lalo"],
                        Cognacy=cogid,
                    ):
                        args.writer.add_cognate(
                            lexeme=row, Cognateset_ID=cogid + 1, Source=["Yang2011Lalo"]
                        )
