import logging
import pandas as pd
from sqlalchemy import (create_engine, Table, Column, String, Integer, MetaData, Text, Float, BigInteger,
                        select, and_, or_, not_, func, insert, text, exc)
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_db_engine(db_url: str):
    return create_engine(db_url)

def create_pubmed_table(engine):
    metadata = MetaData()
    pubmed_table = Table('pubmed_table', metadata,
                         Column('id', Integer, primary_key=True, autoincrement=True),
                         Column('pmid', Integer),
                         Column('title', Text),
                         Column('abstract', Text),
                         Column('journal', Text),
                         Column('pubdate', Text),
                         Column('publication_types', Text),
                         Column('authors', Text),
                         Column('doi', Text),
                         Column('version', Text))
                         
    if engine.dialect.name == 'mysql':
        pubmed_table.append_column(Column('index', Integer, nullable=True))
    elif engine.dialect.name == 'postgresql':
        pubmed_table.append_column(Column('index', Integer, nullable=True, server_default='0'))
                         
    metadata.create_all(engine)
    return pubmed_table

def create_journal_table(engine):
    metadata = MetaData()
    journal_table = Table('journal_table', metadata,
                          Column('id', Integer, primary_key=True, autoincrement=True),
                          Column('journal', String),
                          Column('IF2023', Float),
                          Column('IF5year', Float))
    metadata.create_all(engine)
    return journal_table

def insert_pubmed_data(engine, data):
    # 确保 data 是list类型
    if not isinstance(data, list):
        raise ValueError("Data must be a list")

    metadata = MetaData()
    metadata.reflect(bind=engine)
    pubmed_table = metadata.tables['pubmed_table']

    try:
        with engine.begin() as conn:
            # 构建插入数据
            records_to_insert = [
                {
                    'pmid': record['pmid'],
                    'title': record['title'],
                    'abstract': record['abstract'],
                    'journal': record['journal'],
                    'pubdate': record['pubdate'],
                    'publication_types': record['publication_types'],
                    'authors': record['authors'],
                    'doi': record['doi'],
                    'version': record['version']
                }
                for record in data
            ]
            
            # 批量执行插入
            conn.execute(insert(pubmed_table), records_to_insert)
            
            logger.info(f"Inserted {len(data)} records successfully.")
    
    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")




def search_pubmed_table_simple(engine, table, search_term, search_field='journal'):
    with engine.connect() as conn:
        stmt = select([table]).where(func.lower(table.c[search_field]).like(f'%{search_term.lower()}%'))
        result = conn.execute(stmt)
        return result.fetchall()

def search_pubmed_table(engine, table, search_terms, search_fields, operator='AND'):
    with engine.connect() as conn:
        conditions = []
        for term, field in zip(search_terms, search_fields):
            conditions.append(func.lower(table.c[field]).like(f'%{term.lower()}%'))

        if operator == 'AND':
            stmt = select([table]).where(and_(*conditions))
        elif operator == 'OR':
            stmt = select([table]).where(or_(*conditions))
        elif operator == 'NOT':
            stmt = select([table]).where(not_(or_(*conditions)))
        else:
            raise ValueError("Operator must be 'AND', 'OR', or 'NOT'")

        result = conn.execute(stmt)
        return result.fetchall()

def insert_journal_data(engine, df):
    # Preprocess the DataFrame to handle '<0.1' values
    try:
        df['IF2023'] = df['IF2023'].replace('<0.1', '0.1').astype(float)
        df['IF5year'] = df['IF5year'].replace('<0.1', '0.1').astype(float)
    except ValueError as e:
        logger.error(f"Failed to convert data types: {e}")
        return

    # Log the DataFrame contents (in debug mode only)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"DataFrame contents:\n{df.head()}")

    metadata = MetaData()
    metadata.reflect(bind=engine)
    journal_table = Table('journal_table', metadata, autoload_with=engine)

    with engine.begin() as conn:
        try:
            # Batch insert for better performance
            conn.execute(
                insert(journal_table),
                [
                    {
                        "journal": row['journal'],
                        "IF2023": row['IF2023'],
                        "IF5year": row['IF5year']
                    }
                    for index, row in df.iterrows()
                ]
            )
        except IntegrityError as e:
            logger.error(f"Database integrity error: {e}")
        except Exception as e:
            logger.error(f"Error executing SQL: {e}")

def join_pubmed_and_journal(engine, pubmed_table, journal_table):
    with engine.connect() as conn:
        stmt = select([pubmed_table, journal_table]).where(
            func.lower(pubmed_table.c.journal) == func.lower(journal_table.c.journal)
        )
        result = conn.execute(stmt)
        return result.fetchall()

