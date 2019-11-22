#!/usr/bin/env python

import logging
import psycopg2

from salt.retriever.log import startLog, getLogFile
from salt.retriever.dbfuncs import add_lda
from salt.models.ldaTools import update_users, load_data, doc_stream


def main():
	LDA_LOG.info('Connecting to database...')
	with psycopg2.connect(database='hn_salt') as psql_conn:
		LDA_LOG.info('Database connection established.')
		LDA_LOG.info('Loading data...')
		lda, id2word = load_data()
		LDA_LOG.info('Data loaded.')
		while True:
			LDA_LOG.info('Getting lda scores...')
			scores, users = update_users(doc_stream, lda, id2word)
			LDA_LOG.info(f'Got {len(scores)} lda scores.')
			if len(scores) == 0:
				break
			add_lda(psql_conn, scores, users)

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
