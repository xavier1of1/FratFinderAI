from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg.rows import dict_row

from fratfinder_crawler.config import Settings


@contextmanager
def get_connection(settings: Settings) -> Iterator[psycopg.Connection]:
    connection = psycopg.connect(settings.database_url, row_factory=dict_row)
    try:
        yield connection
        if not connection.closed:
            connection.commit()
    except Exception:
        if not connection.closed:
            connection.rollback()
        raise
    finally:
        connection.close()
