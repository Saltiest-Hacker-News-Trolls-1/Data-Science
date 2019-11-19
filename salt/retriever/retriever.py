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
	try:
		item = get_item(id, required_keys={'id', 'by', 'type'})
	except Exception as e:
		RETRIEVER_LOG.info(f'Skipping item with id {id} due to exception: {str(e)}')
		RETRIEVER_LOG.warning(e)
		item = None
	if item is not None:
		if score_func is not None and 'text' in item:
			if cleaner_func is not None:
				item['text'] = cleaner_func(item['text'])
			scores = score_func(item['text'])
			item = {**item, **scores}
		RETRIEVER_LOG.info(f'Adding item with id {id}.')
		add_item(conn, item)
	else:
		RETRIEVER_LOG.info(f'Not adding item with id {id}, as it is not a comment.')


