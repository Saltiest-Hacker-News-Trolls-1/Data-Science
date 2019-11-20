#!/usr/bin/env python

import aiohttp
import logging
import psycopg2

from salt.retriever.log import startLog, getLogFile
from salt.retriever.dbfuncs import create_tables, get_max_id_retrieved
from salt.retriever.apifuncs import get_max_item, cleaner_func, fetch_batch, ENDPOINT
from salt.retriever.retriever import add_items_from_batch_pooled
from salt.models.ScoringFunctions import score_func


def main():
	RUN_LOG.info('Connecting to database...')
	with psycopg2.connect(database='hn_salt') as psql_conn:
		RUN_LOG.info('Database connection established.')
		create_tables(psql_conn)

		max_id = get_max_id_retrieved(psql_conn)
		if max_id is None:
			max_id = 0
		start = max_id + 1
		stop = get_max_item()
		step = 10000
		if stop - start < step:
			step = stop - start - 1

		for i in range(start, stop, step):
			urls = []
			for id in range(i, i + step):
				urls.append(f'{ENDPOINT}/item/{id}.json')
			RUN_LOG.info(f'Fetching batch of {len(urls)} urls in range {urls[0]} - {urls[-1]}')
			batch = fetch_batch(urls, required_keys={'id', 'by', 'type'})
			add_items_from_batch_pooled(psql_conn, batch, score_func=score_func, cleaner_func=cleaner_func,
				required_keys={'id', 'by', 'text', 'time', 'parent', 'neg', 'neu', 'pos', 'compound'})


if __name__ == '__main__':
	startLog(getLogFile(__file__))
	RUN_LOG = logging.getLogger('root')
	eCount = 0
	while eCount < 20:
		try:
			main()
		except Exception as e:
			e += 1
			RUN_LOG.warning(f'Exception in run, current exception count: {e}')
			RUN_LOG.exception(e)

