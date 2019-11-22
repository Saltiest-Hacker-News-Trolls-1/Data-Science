#!/usr/bin/env python

import logging
import psycopg2

from salt.retriever.log import startLog, getLogFile
from salt.retriever.dbfuncs import add_lda


def main():
	LDA_LOG.info('Connecting to database...')
	with psycopg2.connect(database='hn_salt') as psql_conn:
		LDA_LOG.info('Database connection established.')
		done = False
		while not done:
			done = True
			add_lda(psql_conn, [], [])

	LDA_LOG.info('Done getting lda scores.')
	return True


if __name__ == '__main__':
	startLog(getLogFile(__file__))
	LDA_LOG = logging.getLogger('root')
	try:
		main()
	except Exception as e:
		LDA_LOG.warning(f'Exception in addlda: {e}')
		LDA_LOG.exception(e)

	LDA_LOG.critical('App finished, program closing...')
	LDA_LOG.critical('********************************')
