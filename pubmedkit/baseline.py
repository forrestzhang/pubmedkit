import pubmed_parser as pp
import pandas as pd
from os.path import basename

def baseline_to_dict(xmlfile: str) -> dict:
    data = dict()
    path_xml = pp.parse_medline_xml(xmlfile)
    filename = basename(xmlfile)
    fileinfor = filename.split('.')
    baselineversion = fileinfor[0]

    for entry in path_xml:
        data[entry['pmid']] = {
            'pmid': entry['pmid'],
            'title': entry['title'],
            'abstract': entry['abstract'],
            'journal': entry['journal'],
            'pubdate': entry['pubdate'],
            'publication_types': entry['publication_types'],
            'authors': entry['authors'],
            'doi': entry['doi'],
            'version': baselineversion
        }
    return data

def baseline_to_pd(xmlfile: str) -> pd.DataFrame:
    data = baseline_to_dict(xmlfile)
    return pd.DataFrame.from_dict(data).T