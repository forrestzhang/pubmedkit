import sys
sys.path.append("..")
from pubmedkit.db_utils import create_pubmed_table, create_journal_table, insert_pubmed_data, insert_journal_data
from sqlalchemy import create_engine
from pubmedkit.baseline import load_baseline
from glob import glob
from tqdm import tqdm


def main():
    engine = create_engine('sqlite:///../testdata/crm.db')


    # create_pubmed_table(engine)
    # create_journal_table(engine)

    files = glob('../testdata/updatefiles/*.xml.gz')
    files = sorted(files)
    # print(files)
    keywords = [
        "promoter",
        "cis-regulatory",
        "cis-element",
        "enhancer",
        "silencer",
        "operator",
    ]

    for xmlfile in tqdm(files):
        try:
            data = load_baseline(xmlfile, keywords=keywords, impact_factor=6, log=True)

            insert_pubmed_data(engine=engine, data=data)
        except Exception as e:
            print(f"Error in file:{xmlfile}")
            print(e)
    # ifdf = pd.read_excel('../testdata/2023if.xlsx')

    # print(ifdf.describe())

    # ifdf['IF2023'] = ifdf['IF2023'].replace('<0.1', 0.1).astype(float)
    # ifdf['IF5year'] = ifdf['IF5year'].replace('<0.1', 0.1).astype(float)

    # insert_journal_data(engine, df=ifdf)

if __name__ == "__main__":
    main()