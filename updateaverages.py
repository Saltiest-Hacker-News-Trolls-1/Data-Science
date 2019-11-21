#!/usr/bin/env python

import logging
import psycopg2

from salt.retriever.log import startLog, getLogFile
from salt.retriever.dbfuncs import populate_user_averages


def main():
	ADD_LOG.info('Connecting to database...')
	with psycopg2.connect(database='hn_salt') as psql_conn:
		ADD_LOG.info('Database connection established.')
		populate_user_averages(psql_conn)

	ADD_LOG.info('Done populating users.')
	return True


if __name__ == '__main__':
	startLog(getLogFile(__file__))
	ADD_LOG = logging.getLogger('root')
	try:
		main()
	except Exception as e:
		ADD_LOG.warning(f'Exception in updateusers: {e}')
		ADD_LOG.exception(e)

	ADD_LOG.critical('App finished, program closing...')
	ADD_LOG.critical('********************************')
