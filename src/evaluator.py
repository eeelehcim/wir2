import csv

def load_correct_documents(file_path):
    correct_docs = {}
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            query_id = row[0]
            document_id = row[1]
            if query_id not in correct_docs:
                correct_docs[query_id] = set()
            correct_docs[query_id].add(document_id)
    return correct_docs

def load_retrieved_documents(file_path):
    retrieved_docs = {}
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            query_id = row[0]
            document_id = row[1]
            if query_id not in retrieved_docs:
                retrieved_docs[query_id] = []
            retrieved_docs[query_id].append(document_id)
    return retrieved_docs

def precision_and_recall(correct_docs, retrieved_docs, k):
    results = {}
    
    for query_id in retrieved_docs:
        retrieved = set(retrieved_docs[query_id][:k])
        correct = correct_docs.get(query_id, set())

        retrieved_correct = retrieved.intersection(correct)
        precision = len(retrieved_correct) / len(retrieved) if retrieved else 0
        recall = len(retrieved_correct) / len(correct) if correct else 0
        
        results[query_id] = (precision, recall)
    
    return results

def calculate_map_and_mar(results):
    total_precision = 0
    total_recall = 0
    query_count = len(results)
    
    for precision, recall in results.values():
        total_precision += precision
        total_recall += recall
    
    map_score = total_precision / query_count if query_count else 0
    mar_score = total_recall / query_count if query_count else 0
    
    return map_score, mar_score