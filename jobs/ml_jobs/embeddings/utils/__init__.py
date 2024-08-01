from urllib.parse import urlparse


def get_filename_from_url(url):
    """
    Extract the filename from a URL.
    """
    return urlparse(url).path.split("/")[-1]
