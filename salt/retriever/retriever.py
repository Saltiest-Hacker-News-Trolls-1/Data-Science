#!/usr/bin/env python

import logging
from psycopg2.extensions import connection

from salt.retriever.apifuncs import get_item
from salt.retriever.dbfuncs import add_item

RETRIEVER_LOG = logging.getLogger('root')


def retrieve_and_add_item(
		conn: connection,
		id: int,
		score_func: callable = None,
		cleaner_func: callable = None
) -> None:
	item = get_item(id, required_keys={'id', 'by'})
	if score_func is not None:
		if cleaner_func is not None:
			item['text'] = cleaner_func(item['text'])
		scores = score_func(item['text'])
		item = {**item, **scores}
	add_item(conn, item)


