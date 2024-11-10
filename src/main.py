import os
from pylucene import create_index_writer, index_documents, process_queries
from evaluator import load_correct_documents, load_retrieved_documents, precision_and_recall, calculate_map_and_mar


FOLDER = "results/big/"
INDEX_DIR = FOLDER + "index_standard"
DOCS_DIR = FOLDER + "docs"
QUERY_FILE = FOLDER + "queries.csv"
RESULTS_FILE = FOLDER + "result.csv"
CORRECT_FILE = FOLDER + "correct.csv"


def main():
    print("index")
    #index_writer = create_index_writer(INDEX_DIR)
    #index_documents(index_writer, DOCS_DIR)
    #index_writer.close()
    print("queries")
    process_queries(INDEX_DIR, QUERY_FILE, RESULTS_FILE, 10)
    print("__________________")
    generated = load_retrieved_documents(RESULTS_FILE)
    relevant = load_correct_documents(CORRECT_FILE)
    for k in [1, 3, 5, 10]:
        results = precision_and_recall(relevant, generated, k)
        print(calculate_map_and_mar(results))
if __name__ == "__main__":
    main()
