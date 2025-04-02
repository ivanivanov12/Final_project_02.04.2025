def split_words(text):
    return text.lower().split()


def remove_stop_words(words, stop_words):
    return [word for word in words if word not in stop_words]


def parse_query(query, stop_words):
    words = split_words(query)
    cleaned_words = remove_stop_words(words, stop_words)
    return set(cleaned_words)


def match_document(document_words, query_words):
    return len(set(document_words) & query_words)


def find_documents(documents, query, stop_words):
    query_words = parse_query(query, stop_words)
    results = []
    for document_id, document_content in documents:
        document_words = split_words(document_content)
        relevance = match_document(document_words, query_words)
        if relevance > 0:
            results.append((document_id, relevance))
    results.sort(key=lambda x: x[1], reverse=True)
    return results
