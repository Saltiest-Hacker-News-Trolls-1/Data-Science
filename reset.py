#!/usr/bin/env python
"""
Resets all DS DB tables.

This *will* remove all data and reset the database to a clean slate.
Do not run if you're not certain you want everything gone.
"""


import logging
import psycopg2

from salt.retriever.log import startLog, getLogFile
from salt.retriever.dbfuncs import reset_tables


def main():
	startLog(getLogFile(__file__))
	RESET_LOG = logging.getLogger('root')

	RESET_LOG.info('Connecting to database...')
	with psycopg2.connect(database='hn_salt') as psql_conn:
		RESET_LOG.info('Database connection established.')
		reset_tables(psql_conn)


if __name__ == '__main__':
	if input('Are you certain you want to reset the database (y/n): ').lower() in ['y', 'yes']:
		main()
