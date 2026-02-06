from urllib.parse import urlparse


def get_filename_from_url(url):
    """
    Extract the filename from a URL.
    """
    if url is None:
        return None
    else:
        return urlparse(url).path.split("/")[-1]
