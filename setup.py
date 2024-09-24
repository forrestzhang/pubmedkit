from setuptools import setup, find_packages

setup(
    name='pubmedkit',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'biopython>=1.70',
        'pubmed_parser>=0.5.0'
    ],
    description='A Python package to convert PubMed Baseline files to Pandas DataFrames and search PubMed entries.',
    author='Tao Zhang',
    author_email='forrest_zhang@163.com',
    url='https://github.com/forrestzhang/pubmedkit',
    license='MIT',
)