import sys
sys.path.append("..")
from pubmedkit.db_utils import create_pubmed_table, insert_pubmed_data
from sqlalchemy import create_engine
from pubmedkit.baseline import load_baseline
from glob import glob
from tqdm import tqdm
from multiprocessing import Pool

def main(threads_number = 16):
    engine = create_engine('sqlite:///../testdata/crm_v2.db')


    create_pubmed_table(engine)
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

    params_list = []

    for xmlfile in files:
        params_list.append({
            'xmlfile': xmlfile,
            'keywords': keywords,
            'impact_factor': 6
        })
  

    with Pool(threads_number) as p:
        # 使用 tqdm 显示进度条
        results = p.imap_unordered(load_baseline_worker, params_list)
        
        # 使用 tqdm 显示进度条
        for data in tqdm(results, total=len(params_list)):
            if data:
                insert_pubmed_data(engine=engine, data=data)

def load_baseline_worker(params:dict):
    xmlfile = params['xmlfile']
    keywords = params['keywords']
    impact_factor = params['impact_factor']
    data = []
    try:

        data = load_baseline(xmlfile, keywords=keywords, impact_factor=impact_factor, log=True)
        
        
    except Exception as e:
        print(f"Error in file:{xmlfile}")
        print(e)
    
    return data

if __name__ == "__main__":
    main()