import json
import os
import re
import math
from nltk.stem import PorterStemmer

# I use PorterStemmer to reduce words to their base/root form (e.g., "running" becomes "run")
ps = PorterStemmer()

# This is the folder where all my saved index files live
INDEX_DIR = "partial_indexes"

# This function breaks text into lowercase words, removing punctuation
def tokenize(text):
    return re.findall(r'\b\w+\b', text.lower())

# This function finds where a term appears (which docs it's in and extra info like tf and importance)
def find_postings(term):
    postings = []  # list of (doc_id, tf, importance)
    doc_ids = set()  # set of document IDs where the term appears
    for file in os.listdir(INDEX_DIR):
        if file.startswith("index_partial_") and file.endswith(".json"):
            try:
                with open(os.path.join(INDEX_DIR, file), 'r') as f:
                    part = json.load(f)
                    if term in part:
                        postings.extend(part[term])
                        for doc_id, _, _ in part[term]:
                            doc_ids.add(doc_id)
            except:
                # If a file can't be read, just skip it
                continue
    return postings, doc_ids

# This function ranks documents using TF-IDF scoring based on the query terms
def tfidf_ranking(query_terms, total_docs):
    scores = {}
    for term in query_terms:
        postings, doc_ids = find_postings(term)
        df = len(doc_ids)  # document frequency: how many docs contain this term
        if df == 0:
            continue
        idf = math.log(total_docs / (1 + df))  # inverse document frequency
        for doc_id, tf, importance in postings:
            # TF-IDF formula with a weight multiplier
            score = tf * idf * importance
            scores[doc_id] = scores.get(doc_id, 0) + score
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)

# This counts how many unique documents we have in all the index files
def count_total_documents():
    seen = set()
    for file in os.listdir(INDEX_DIR):
        if file.startswith("index_partial_") and file.endswith(".json"):
            try:
                with open(os.path.join(INDEX_DIR, file), 'r') as f:
                    part = json.load(f)
                    for postings in part.values():
                        for doc_id, _, _ in postings:
                            seen.add(doc_id)
            except:
                continue
    return len(seen)

# Main function that runs the search engine in the terminal
def main():
    # First count all the docs (used for IDF)
    total_docs = count_total_documents()

    # Load mapping from document ID to the URL
    with open('doc_id_map.json', 'r') as f:
        doc_id_map = json.load(f)

    # Interactive search loop
    while True:
        query = input("Search> ")
        if not query.strip():
            break  # If input is empty, stop the program
        # Tokenize and stem the query
        terms = [ps.stem(w) for w in tokenize(query)]
        results = tfidf_ranking(terms, total_docs)

        if not results:
            print("No results found.")
            continue

        # Show top 10 results
        for doc_id, score in results[:10]:
            url = doc_id_map.get(str(doc_id), "Unknown Document")
            print(f"{url} — Score: {score:.2f}")

# Entry point
if __name__ == '__main__':
    main()

