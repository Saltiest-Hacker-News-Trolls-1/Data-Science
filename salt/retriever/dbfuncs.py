#!/usr/bin/env python

import logging

DB_LOG = logging.getLogger('root')


def create_tables(conn):
	query = """
		CREATE TABLE IF NOT EXISTS items (
			id INT NOT NULL PRIMARY KEY,
			by VARCHAR(15) NOT NULL,
			time INT,
			score INT,
			parent INT,
			deleted BOOLEAN DEFAULT FALSE,
			negativity NUMERIC,
			positivity NUMERIC,
			neutrality NUMERIC,
			compound NUMERIC
		);
	"""
	DB_LOG.info('Executing `items` create query...')
	curr = conn.cursor()
	curr.execute(query)
	curr.close()
	DB_LOG.info('`items` create query executed.')

	query = """
		CREATE TABLE IF NOT EXISTS users (
			id VARCHAR(15) NOT NULL PRIMARY KEY,
			karma INT,
			negativity NUMERIC
		);
	"""
	DB_LOG.info('Executing `users` create query...')
	curr = conn.cursor()
	curr.execute(query)
	curr.close()
	conn.commit()
	DB_LOG.info('`users` create query executed.')


def reset_tables(conn):
	query = """
		DROP TABLE IF EXISTS items;
	"""
	DB_LOG.info('Executing `items` drop query...')
	curr = conn.cursor()
	curr.execute(query)
	curr.close()
	DB_LOG.info('`items` drop query executed.')

	query = """
		DROP TABLE IF EXISTS users;
	"""
	DB_LOG.info('Executing `users` drop query...')
	curr = conn.cursor()
	curr.execute(query)
	curr.close()
	DB_LOG.info('`users` drop query executed.')

	create_tables(conn)
	DB_LOG.info('Tables reset.')


def add_item(conn, item):
	query = """
		INSERT INTO items(id, by, negativity)
		VALUES (%(id)s, %(by)s, %(negativity)s);
	"""
	curr = conn.cursor()
	curr.execute(
		query,
		{
			'id': item['id'],
			'by': item['by'],
			'negativity': item.get('neg', None),
			'neutrality': item.get('neu', None),
			'positivity': item.get('pos', None),
			'compound': item.get('compound', None),
		}
	)
	curr.close()
	conn.commit()
	DB_LOG.info(f'Added item with id {item["id"]}.')


def get_max_id_retrieved(conn):
	query = """
		SELECT MAX(id) FROM items;
	"""
	curr = conn.cursor()
	curr.execute(query)
	result = curr.fetchone()[0]
	curr.close()
	DB_LOG.info(f'Retrieved max id of {result} from items')
	return (result)


def get_max_id_predicted(conn):
	query = """
		SELECT MAX(id) FROM items WHERE negativity IS NOT NULL;
	"""
	curr = conn.cursor()
	curr.execute(query)
	result = curr.fetchone()[0]
	curr.close()
	DB_LOG.info(f'Retrieved max id of {result} from items with negativity populated')
	return (result)
