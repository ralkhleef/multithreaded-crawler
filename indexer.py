import os
import json
import re
from collections import defaultdict
from nltk.stem import PorterStemmer
from bs4 import BeautifulSoup

# I use this stemmer to reduce words to their root form
ps = PorterStemmer()

# Turns text into a list of lowercase words, removing punctuation
def tokenize(text):
    return re.findall(r'\b\w+\b', text.lower())

# Extracts all visible words and "important" ones from HTML tags like title, h1, etc.
def extract_important_words(html):
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text()  # full visible text from HTML
    important = []

    for tag in ['title', 'h1', 'h2', 'h3', 'strong', 'b']:
        for elem in soup.find_all(tag):
            important += tokenize(elem.get_text())  # grab emphasized words

    return tokenize(text), important

# This function walks through the downloaded data, builds index, and saves it in parts
def index_corpus(corpus_root, partial_limit=10):
    inverted_index = defaultdict(list)  # word → list of (doc_id, frequency, importance)
    doc_id_map = {}  # maps doc_id → URL
    doc_id = 0
    partial_count = 0

    print(f"Starting indexing in: {corpus_root}")

    for root, _, files in os.walk(corpus_root):
        for file in files:
            if not file.endswith('.json'):
                continue

            file_path = os.path.join(root, file)
            try:
                with open(file_path, 'r', encoding='utf8') as f:
                    data = json.load(f)
                    url = data.get('url')
                    html = data.get('content', '')
                    if not html:
                        continue
            except Exception as e:
                print(f"Skipped {file_path}: {e}")
                continue

            # Get all words and the "important" ones
            words, important_words = extract_important_words(html)
            term_freq = defaultdict(int)

            # Count term frequencies using stemming
            for word in words:
                stemmed = ps.stem(word)
                term_freq[stemmed] += 1

            # Add word info to the inverted index
            for word, freq in term_freq.items():
                importance = 2 if word in important_words else 1
                inverted_index[word].append((doc_id, freq, importance))

            doc_id_map[doc_id] = url
            doc_id += 1

            # Save partial index every N documents to reduce memory usage
            if doc_id % partial_limit == 0:
                print(f"Saving partial index #{partial_count}")
                with open(f'index_partial_{partial_count}.json', 'w') as out:
                    json.dump(inverted_index, out)
                inverted_index = defaultdict(list)
                partial_count += 1

    # Save anything that’s left after the loop
    if inverted_index:
        print(f"Saving final partial index #{partial_count}")
        with open(f'index_partial_{partial_count}.json', 'w') as out:
            json.dump(inverted_index, out)

    # Save the mapping from document ID to URL
    with open('doc_id_map.json', 'w') as out:
        json.dump(doc_id_map, out)

    print("Indexing complete.")
    print(f"Total documents indexed: {doc_id}")

# Run the indexer on the developer dataset directory
if __name__ == '__main__':
    index_corpus('/home/ralkhlee/ics_data')
