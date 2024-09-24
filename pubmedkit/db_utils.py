from sqlalchemy import create_engine, Table, Column, String, MetaData
from sqlalchemy.dialects.sqlite import insert

def create_db_engine(db_url: str):
    return create_engine(db_url)

def create_pubmed_table(engine):
    metadata = MetaData()
    example_table = Table('pubmed_table', metadata,
                          Column('pmid', String, primary_key=True),
                          Column('title', String),
                          Column('abstract', String),
                          Column('journal', String),
                          Column('pubdate', String),
                          Column('publication_types', String),
                          Column('authors', String),
                          Column('doi', String),
                          Column('version', String))
    metadata.create_all(engine)
    return example_table

def insert_data(engine, table, data):
    with engine.connect() as conn:
        for pmid, record in data.items():
            stmt = insert(table).values(
                pmid=record['pmid'],
                title=record['title'],
                abstract=record['abstract'],
                journal=record['journal'],
                pubdate=record['pubdate'],
                publication_types=record['publication_types'],
                authors=record['authors'],
                doi=record['doi'],
                version=record['version']
            ).on_conflict_do_update(
                index_elements=['pmid'],
                set_={
                    'title': record['title'],
                    'abstract': record['abstract'],
                    'journal': record['journal'],
                    'pubdate': record['pubdate'],
                    'publication_types': record['publication_types'],
                    'authors': record['authors'],
                    'doi': record['doi'],
                    'version': record['version']
                },
                where=(table.c.version < record['version'])
            )
            conn.execute(stmt)