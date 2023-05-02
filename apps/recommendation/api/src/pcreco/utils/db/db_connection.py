from flask import g


def get_session():
    return __get_session()


def __get_session():
    return g.connection
