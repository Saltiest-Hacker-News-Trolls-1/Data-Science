#!/usr/bin/env python

import aiohttp
import logging
import psycopg2

from salt.retriever.log import startLog, getLogFile
from salt.retriever.dbfuncs import create_tables, get_max_id_retrieved
from salt.retriever.apifuncs import get_max_item, cleaner_func, fetch_batch, ENDPOINT
from salt.retriever.retriever import add_items_from_batch
from salt.models.ScoringFunctions import score_func


def main():
	startLog(getLogFile(__file__))
	RUN_LOG = logging.getLogger('root')

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
			add_items_from_batch(psql_conn, batch, score_func=score_func, cleaner_func=cleaner_func,
				required_keys={'id', 'by', 'text', 'time', 'parent'})


if __name__ == '__main__':
	while True:
		try:
			main()
		except aiohttp.client_exceptions.ClientConnectorError as e:
			pass
