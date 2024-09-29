import sys
sys.path.append("..")
import pickle
from pubmedkit.baseline import load_baseline
from glob import glob
from tqdm import tqdm
from multiprocessing import Pool

def main(keywords:list, fileslist:list, outfile:str, threads_number = 32):

    params_list = []

    for xmlfile in fileslist:
        params_list.append({
            'xmlfile': xmlfile,
            'keywords': keywords,
            'impact_factor': 6
        })

    cache_data = []

    with Pool(threads_number) as p:
        # 使用 tqdm 显示进度条
        results = p.imap_unordered(load_baseline_worker, params_list)
        
        # 使用 tqdm 显示进度条
        for datalist in tqdm(results, total=len(params_list)):
            if datalist:
                for data in datalist:
                    cache_data.append(data)
    
    with open(outfile, 'wb') as f:
        pickle.dump(cache_data, f)

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

    keywords = [
        "promoter",
        "cis-regulatory",
        "cis-element",
        "enhancer",
        "silencer",
        "operator",
    ]
    
    files = glob('../testdata/updatefiles/*.xml.gz')
    files = sorted(files)

    main(keywords=keywords, fileslist=files, outfile='../testdata/test_baseline_filter.pkl')