import os
import csv
import lucene
from concurrent.futures import ThreadPoolExecutor
from java.nio.file import Paths
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.analysis.en import EnglishAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, TextField, Field
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser, QueryParserBase
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.search.similarities import ClassicSimilarity
import time

lucene.initVM()

def create_index_writer(index_dir):
    """
    Initializes the index writer for the specified directory.

    :param index_dir: Directory path where the index will be stored.
    :return: IndexWriter instance.
    """
    if not os.path.exists(index_dir):
        os.makedirs(index_dir)
    directory = FSDirectory.open(Paths.get(index_dir))
    analyzer = StandardAnalyzer()
    index_writer_config = IndexWriterConfig(analyzer)
    return IndexWriter(directory, index_writer_config)


def index_documents(index_writer, docs_dir):
    """
    Indexes all documents from a given directory.

    :param index_writer: IndexWriter instance to write documents to the index.
    :param docs_dir: Directory containing documents to be indexed.
    """
    files = [f for f in os.listdir(docs_dir) if os.path.isfile(os.path.join(docs_dir, f)) and f.startswith('output_')]
    total_files = len(files)
    
    for count, filename in enumerate(files, start=1):
        file_path = os.path.join(docs_dir, filename)
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            doc_id = filename.split('_')[1].split('.')[0]
            document = Document()
            document.add(TextField("content", content, Field.Store.YES))
            document.add(TextField("doc_id", doc_id, Field.Store.YES))
            index_writer.addDocument(document)

    index_writer.commit()



def process_queries(index_dir, csv_input_file, csv_output_file, k = 10):
    """
    Processes queries from a CSV file, performs a search, and writes results to another CSV file.

    :param index_dir: Directory where the index is stored.
    :param csv_input_file: CSV file containing queries.
    :param csv_output_file: CSV file to write search results.
    :return: Dictionary with query_id as keys and list of doc_ids as values.
    """
    reader = DirectoryReader.open(FSDirectory.open(Paths.get(index_dir)))
    searcher = IndexSearcher(reader)
    #searcher.setSimilarity(ClassicSimilarity())
    analyzer = StandardAnalyzer()
    query_parser = QueryParser("content", analyzer)
    
    results = {}

    with open(csv_output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['query_id', 'doc_id'])

        with open(csv_input_file, 'r', encoding='utf-8') as infile:
            csv_reader = csv.reader(infile)
            next(csv_reader)
            
            for count, row in enumerate(csv_reader, start=1):
                query_id = row[0]
                query_str = QueryParserBase.escape(row[1])
                query = query_parser.parse(query_str)
                top_docs = searcher.search(query, k).scoreDocs

                results[query_id] = []
                for hit in top_docs:
                    doc = searcher.doc(hit.doc)
                    doc_id = doc.get("doc_id")
                    writer.writerow([query_id, doc_id])
                    results[query_id].append(doc_id)

    reader.close()
    return results
