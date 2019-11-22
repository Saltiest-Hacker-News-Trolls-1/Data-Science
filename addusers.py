#!/usr/bin/env python

import logging
import psycopg2

from salt.retriever.log import startLog, getLogFile
from salt.retriever.dbfuncs import create_tables, get_max_id_retrieved, get_missing_users, add_users
from salt.retriever.apifuncs import get_max_item, cleaner_func, fetch_batch, ENDPOINT
from salt.retriever.retriever import add_items_from_batch
from salt.models.ScoringFunctions import score_func


def main():
	ADD_LOG.info('Connecting to database...')
	with psycopg2.connect(database='hn_salt') as psql_conn:
		ADD_LOG.info('Database connection established.')
		create_tables(psql_conn)

		users_list = get_missing_users(psql_conn)

		chunksize = 1000

		for i in range(0, len(users_list) + 1, chunksize):
			users_chunk = users_list[i:i + chunksize]
			urls = []
			for username in users_chunk:
				urls.append(f'{ENDPOINT}/user/{username}.json')
			if len(urls) > 0:
				ADD_LOG.info(f'Fetching batch of {len(urls)} urls in range {urls[0]} [{i}] - {urls[-1]} [{i + len(users_chunk)}]')
				batch = fetch_batch(urls, required_keys={'id', 'karma'}, comments_only=False)
				add_users(psql_conn, batch.values())

	ADD_LOG.info('Done getting users.')
	return True


if __name__ == '__main__':
	startLog(getLogFile(__file__))
	ADD_LOG = logging.getLogger('root')
	eCount = 0
	while eCount < 20:
		eCount += 1
		try:
			if main() is True:
				break
		except Exception as e:
			ADD_LOG.warning(f'Exception in addusers, current exception count: {eCount}')
			ADD_LOG.exception(e)

	ADD_LOG.critical('App finished, program closing...')
	ADD_LOG.critical('********************************')
