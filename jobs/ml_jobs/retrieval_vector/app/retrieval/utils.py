from docarray import DocumentArray


def load_documents(path: str) -> DocumentArray:
    return DocumentArray.load(path)
