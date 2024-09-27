from Bio import Entrez
from Bio import Medline
import pandas as pd

def query_pmid(query: str, email: str="your_email@example.com", retmax: int=9999) -> list:
    Entrez.email = email 
    search_handle = Entrez.esearch(db="pubmed", term=query, retmax=retmax)
    record = Entrez.read(search_handle)
    idlist = record["IdList"]
    return idlist


def load_pubmed_file(filename: str,  *args, **kwargs):

    # 获取输出类型，默认为'list'
    output_type = kwargs.get('output_type', 'list')

    # 验证输出类型的有效性
    if output_type not in ['list', 'dict', 'pd']:
        raise ValueError('output_type must be "pd", "list" or "dict"')

    records_dict = {}

    with open(filename, 'r') as handle:

        for record in Medline.parse(handle):
            pmid = record['PMID']
            records_dict[pmid] = {
                'pmid': pmid,
                'title': record.get('TI', 'N/A'),
                'abstract': record.get('AB', 'No abstract available'),
                'journal': record.get('JT', 'N/A'),
                'pubdate': record.get('DP', 'N/A'),
                'publication_types': record.get('PT', []),
                'authors': record.get('AU', [])
            }
    # records = []


    # with open(filename) as handle:
    #     records = Medline.parse(handle)
    
    # # return records

    # records_dict = {}

    # for record in records:
    #     pmid = record['PMID']
    #     records_dict[pmid] = {
    #         'pmid': pmid,
    #         'title': record['TI'],
    #         'abstract':  record['AB'],
    #         'journal': record['JT'],
    #         'pubdate': record['DP'],    
    #         'publication_types': record['PT'],
    #         'authors': record['AU'],

    #     }
    


    # 根据输出类型返回数据
    if output_type == 'pd':
        # 如果是'pd'，则返回DataFrame格式的数据
        return pd.DataFrame.from_dict(records_dict).T
    elif output_type == 'dict':
        # 如果是'dict'，则返回字典格式的数据
        return records_dict
    elif output_type == 'list':
        # 如果是'list'，则返回列表格式的数据
        return list(records_dict.values())
    else:
        # output_type无效，抛出异常
        raise ValueError('Invalid output_type')
    

if __name__ == '__main__':
    records = load_pubmed_file("../data/pubmed_Arabidopsis_ChIP.txt")