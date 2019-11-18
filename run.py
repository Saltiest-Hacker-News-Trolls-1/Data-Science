#!/usr/bin/env python

import logging
import psycopg2

from salt.retriever.log import startLog, getLogFile
from salt.retriever.dbfuncs import create_tables, get_max_id_retrieved
from salt.retriever.apifuncs import get_max_item
from salt.retriever.retriever import retrieve_and_add_item
from salt.models.ScoringFunctions import get_scores


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
		stop = start + 50

		for i in range(start, stop):
			retrieve_and_add_item(psql_conn, i, score_func=get_scores)


if __name__ == '__main__':
	main()
