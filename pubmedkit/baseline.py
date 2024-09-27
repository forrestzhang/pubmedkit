import pubmed_parser as pp
import pandas as pd
from os.path import basename, isfile
import pickle
import logging

def load_baseline(xmlfile: str, *args, **kwargs):
    """
    从给定的XML文件中加载基线数据，并根据output_type参数返回不同格式的数据。

    参数:
    xmlfile (str): XML文件路径。
    *args: 未使用的额外位置参数。
    **kwargs: 未使用的额外关键字参数，目前支持output_type和log。

    返回:
    DataFrame, dict或list: 根据output_type参数的值，返回相应格式的基线数据。
    """
    # 日志配置
    log = kwargs.get('log', False)
    if log:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if not isfile(xmlfile):
        raise FileNotFoundError(f"The specified file {xmlfile} does not exist.")

    # 获取输出类型，默认为'list'
    output_type = kwargs.get('output_type', 'list')

    # 验证输出类型的有效性
    if output_type not in ['list', 'dict', 'pd']:
        raise ValueError('output_type must be "pd", "list" or "dict"')
    
    # 从kwargs获取关键词过滤相关参数
    keywords = kwargs.get('keywords', [])
    kw_filter = kwargs.get('kw_filter', 'abstract')

    # 从kwargs获取影响因子过滤相关参数
    impact_factor = kwargs.get('impact_factor', 0)

    # 打印关键词和过滤类型
    if log:
        logging.info(f"keywords: {keywords}")
        logging.info(f"kw_filter: {kw_filter}")

    # 初始化过滤标志和变量
    keywords_keep = False
    perform_keyword_filtering = True
    impact_factor_keep = False
    perform_impact_factor_filtering = True

    # 根据关键词列表决定是否进行关键词过滤
    if keywords:
        if kw_filter:
            if log:
                logging.info("perform keyword filter")
    else:
        if log:
            logging.info("do not perform keyword filter")
        perform_keyword_filtering = False
        keywords_keep = True

    # 根据影响因子决定是否进行影响因子过滤
    if impact_factor <= 0:
        if log:
            logging.info("do not perform impact_factor filter")
        perform_impact_factor_filtering = False
        impact_factor_keep = True
    else:
        if log:
            logging.info(f"keep impact_factor > {impact_factor}")
    
    # 加载影响因子字典（如果需要进行影响因子过滤）
    impact_factor_dict = {}
    if perform_impact_factor_filtering:
        impact_factor_dict = load_dict_from_pickle('../data/if2024.pickle')

    # 初始化数据字典
    data_dict = dict()

    # 解析Medline XML文件
    try:
        if log:
            logging.info(f"parse xml file {xmlfile}")
        path_xml = pp.parse_medline_xml(xmlfile)
    except Exception as e:
        raise RuntimeError("Error parsing XML file") from e

    # 获取XML文件名（不带路径）
    filename = basename(xmlfile)
    # 分割文件名，获取基线版本信息
    fileinfor = filename.split('.')
    baselineversion = fileinfor[0]

    # 遍历XML条目，构建数据字典
    for entry in path_xml:

        # 根据条件进行关键词过滤
        if perform_keyword_filtering:
            if kw_filter == 'both':
                keywords_keep = (keywords_filter(entry['abstract'], keywords) or keywords_filter(entry['title'], keywords))
            elif kw_filter == 'abstract':
                keywords_keep = keywords_filter(entry['abstract'], keywords)
            elif kw_filter == 'title':
                keywords_keep = keywords_filter(entry['title'], keywords)
            else:
                raise ValueError("Invalid kw_filter value")

        # 根据条件进行影响因子过滤
        if perform_impact_factor_filtering:
            journal_name = entry['journal'].rstrip().lower()
            if journal_name in impact_factor_dict:
                if impact_factor_dict[journal_name] >= impact_factor:
                    impact_factor_keep = True
                    # if log:
                    #     logging.info(f"journal name: {journal_name}, impact factor: {impact_factor_dict[journal_name]}, keep!!")
                else:
                    impact_factor_keep = False
                    # if log:
                    #     logging.info(f"journal name: {journal_name}, impact factor: {impact_factor_dict[journal_name]}, drop!!")
            else:
                impact_factor_keep = False
                # if log:
                #     logging.info(f"journal name: {journal_name}, not found in impact factor dict, drop!!")
                
        # 如果通过关键词和影响因子过滤，则将条目添加到数据字典中
        if keywords_keep and impact_factor_keep:
            data_dict[int(entry['pmid'])] = {
                'pmid': int(entry['pmid']),
                'title': entry['title'],
                'abstract': entry['abstract'],
                'journal': entry['journal'],
                'pubdate': entry['pubdate'],
                'publication_types': entry['publication_types'],
                'authors': entry['authors'],
                'doi': entry['doi'],
                'version': baselineversion
            }

    if log:
        logging.info(f"output_type: {output_type}")
        logging.info(f"{len(data_dict)} entries loaded")
        

    # 根据输出类型返回数据
    if output_type == 'pd':
        # 如果是'pd'，则返回DataFrame格式的数据
        return pd.DataFrame.from_dict(data_dict).T
    elif output_type == 'dict':
        # 如果是'dict'，则返回字典格式的数据
        return data_dict
    elif output_type == 'list':
        # 如果是'list'，则返回列表格式的数据
        return list(data_dict.values())
    else:
        # output_type无效，抛出异常
        raise ValueError('Invalid output_type')

def baseline_to_dict(xmlfile: str) -> dict:
    data = dict()
    path_xml = pp.parse_medline_xml(xmlfile)
    filename = basename(xmlfile)
    fileinfor = filename.split('.')
    baselineversion = fileinfor[0]

    for entry in path_xml:
        data[int(entry['pmid'])] = {
            'pmid': int(entry['pmid']),
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

def baseline_to_list(xmlfile: str) -> list:
    data = []
    path_xml = pp.parse_medline_xml(xmlfile)
    filename = basename(xmlfile)
    fileinfor = filename.split('.')
    baselineversion = fileinfor[0]

    for entry in path_xml:
        data.append( {
            'pmid': int(entry['pmid']),
            'title': entry['title'],
            'abstract': entry['abstract'],
            'journal': entry['journal'],
            'pubdate': entry['pubdate'],
            'publication_types': entry['publication_types'],
            'authors': entry['authors'],
            'doi': entry['doi'],
            'version': baselineversion
        })
    return data

def baseline_to_list_keywords_filter(xmlfile: str, keywords: list, kw_filter: str='abstract') -> list:
    data = []
    path_xml = pp.parse_medline_xml(xmlfile)
    filename = basename(xmlfile)
    fileinfor = filename.split('.')
    baselineversion = fileinfor[0]

    if filter not in ['abstract', 'title', 'both']:
        raise ValueError('filter must be abstract or title')

    for entry in path_xml:
        keep = False
        if kw_filter == 'both':
            keep = keywords_filter(entry['abstract'], keywords) or keywords_filter(entry['title'], keywords)
        elif kw_filter == 'abstract':
            keep = keywords_filter(entry['abstract'], keywords)
        elif kw_filter == 'title':
            keep = keywords_filter(entry['title'], keywords)
        if keep:
            data.append( {
                'pmid': int(entry['pmid']),
                'title': entry['title'],
                'abstract': entry['abstract'],
                'journal': entry['journal'],
                'pubdate': entry['pubdate'],
                'publication_types': entry['publication_types'],
                'authors': entry['authors'],
                'doi': entry['doi'],
                'version': baselineversion
            })
    return data

def baseline_to_list_filter(xmlfile: str, *args, **kwargs) -> list:
    """
    将XML文件转换为包含条目的列表，并根据关键词过滤。
    
    参数:
    - xmlfile: XML文件路径
    - keywords: 关键词列表
    - kw_filter: 过滤字段 ('abstract', 'title', 'both')
    """
    keywords = kwargs.get('keywords', [])
    kw_filter = kwargs.get('kw_filter', 'abstract')

    if kw_filter not in ['abstract', 'title', 'both']:
        raise ValueError('kw_filter must be "abstract", "title", or "both"')

    data = []
    path_xml = pp.parse_medline_xml(xmlfile)
    filename = basename(xmlfile)
    fileinfo = filename.split('.')
    baselineversion = fileinfo[0]

    for entry in path_xml:
        keep = False
        if kw_filter == 'both':
            keep = keywords_filter(entry['abstract'], keywords) or keywords_filter(entry['title'], keywords)
        elif kw_filter == 'abstract':
            keep = keywords_filter(entry['abstract'], keywords)
        elif kw_filter == 'title':
            keep = keywords_filter(entry['title'], keywords)
        
        if keep:
            data.append({
                'pmid': int(entry['pmid']),
                'title': entry['title'],
                'abstract': entry['abstract'],
                'journal': entry['journal'],
                'pubdate': entry['pubdate'],
                'publication_types': entry['publication_types'],
                'authors': entry['authors'],
                'doi': entry['doi'],
                'version': baselineversion
            })

    return data


def keywords_filter(sentence:str, keywords:list):
    # List of keywords to look for in the sentence
    # keywords = [
    #     "promoter",
    #     "cis-regulatory",
    #     "cis-element",
    #     "enhancer",
    #     "silencer",
    #     "operator",
    # ]
    sentence = sentence.lower()
    keep = False
    # Use a regular expression to remove any words that don't contain any of the keywords
    filtered_sentence = " ".join(
        word
        for word in sentence.split()
        if any(keyword.lower() in word for keyword in keywords)
    )

    if filtered_sentence:
        keep = True

    return keep

def baseline_to_pd(xmlfile: str) -> pd.DataFrame:
    data = baseline_to_dict(xmlfile)
    return pd.DataFrame.from_dict(data).T


def load_dict_from_pickle(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)