from flask import g
from psycopg2 import sql
import psycopg2.extensions


def as_string(composable, encoding="utf-8"):
    if isinstance(composable, sql.Composed):
        return "".join([as_string(x, encoding) for x in composable])
    elif isinstance(composable, sql.SQL):
        return composable.string
    else:
        rv = sql.ext.adapt(composable._wrapped)
        if isinstance(rv, psycopg2.extensions.QuotedString):
            rv.encoding = encoding
        rv = rv.getquoted()
        return rv.decode(encoding) if isinstance(rv, bytes) else rv


def get_session():
    return __get_session()


def __get_session():
    return g.connection
