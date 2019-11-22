#!/usr/bin/env python

import logging

from psycopg2.extras import execute_batch

DB_LOG = logging.getLogger('root')


def create_tables(conn):
	query = """
		CREATE TABLE IF NOT EXISTS items (
			id INT NOT NULL PRIMARY KEY,
			by VARCHAR(15) NOT NULL,
			text TEXT,
			time INT,
			parent INT,
			deleted BOOLEAN DEFAULT FALSE,
			negativity NUMERIC,
			positivity NUMERIC,
			neutrality NUMERIC,
			compound NUMERIC,
			lda_salty NUMERIC
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
			negativity NUMERIC,
			positivity NUMERIC,
			neutrality NUMERIC,
			compound NUMERIC,
			commentcount INT,
			lda_run BOOLEAN DEFAULT false
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
		INSERT INTO items(id, by, negativity, positivity, neutrality, compound)
		VALUES (%(id)s, %(by)s, %(negativity)s, %(positivity)s, %(neutrality)s, %(compound)s);
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


def populate_user_averages(conn):
	query = """
		UPDATE users
		SET
			commentcount = itemsavg.count,
			negativity = itemsavg.negavg,
			positivity = itemsavg.posavg,
			neutrality = itemsavg.neuavg,
			compound = itemsavg.compavg
		FROM (
			SELECT
				by,
				COUNT(*) AS count,
				AVG(negativity) AS negavg,
				AVG(positivity) AS posavg,
				AVG(neutrality) AS neuavg,
				AVG(compound) AS compavg
			FROM items
			WHERE by IS NOT NULL
			GROUP BY by
		) as itemsavg
		WHERE itemsavg.by = id;
	"""
	DB_LOG.info('Executing `users` populate query...')
	curr = conn.cursor()
	curr.execute(query)
	curr.close()
	DB_LOG.info('`users` populate query executed.')


def get_all_users(conn):
	query = """
		SELECT DISTINCT by FROM items;
	"""
	curr = conn.cursor()
	curr.execute(query)
	results = curr.fetchall()
	curr.close()
	users_list = []
	for result in results:
		users_list.append(result[0])
	DB_LOG.info(f'Retrieved {len(users_list)} users from items.')
	return (users_list)


def get_missing_users(conn):
	query = """
		SELECT DISTINCT by
		FROM items AS i
		WHERE NOT EXISTS (
			SELECT
			FROM users
			WHERE users.id = i.by
		);
	"""
	curr = conn.cursor()
	curr.execute(query)
	results = curr.fetchall()
	curr.close()
	users_list = []
	for result in results:
		users_list.append(result[0])
	DB_LOG.info(f'Retrieved {len(users_list)} users from items that are missing from users.')
	return (users_list)


def add_users(conn, users):
	DB_LOG.info(f'Adding {len(users)} users to DB...')
	query = """
		INSERT INTO users(id, karma)
		VALUES (%(id)s, %(karma)s);
	"""
	curr = conn.cursor()
	execute_batch(curr, query, users)
	curr.close()
	conn.commit()
	DB_LOG.info(f'Added {len(users)} users.')


def add_items(conn, items):
	DB_LOG.info(f'Adding {len(items)} items to DB...')
	query = """
		INSERT INTO items(id, by, text, time, parent, negativity, positivity, neutrality, compound)
		VALUES (%(id)s, %(by)s, %(text)s, %(time)s, %(parent)s, %(neg)s, %(pos)s, %(neu)s, %(compound)s);
	"""
	curr = conn.cursor()
	execute_batch(curr, query, items)
	curr.close()
	conn.commit()
	DB_LOG.info(f'Added {len(items)} items.')


def add_lda(conn, comments, users):
	DB_LOG.info(f'Adding {len(comments)} lda scores to DB...')
	query = """
		UPDATE items SET lda_salty = %(lda)s WHERE id = %(id)s;
	"""
	curr = conn.cursor()
	execute_batch(curr, query, comments)
	curr.close()

	DB_LOG.info(f'Flagging lda_run as true for {len(users)} users...')
	usersDict = {'user': user for user in users}
	query = """
		UPDATE users SET lda_run = true WHERE id = %(id)s;
	"""
	curr = conn.cursor()
	execute_batch(curr, query, usersDict)
	curr.close()
	conn.commit()
	DB_LOG.info(f'Added {len(comments)} lda scores.')


def reset_lda_flag(conn):
	DB_LOG.info('Resetting all lda_run flags...')
	query = """
		UPDATE users SET lda_run = false;
	"""
	curr = conn.cursor()
	execute_batch(curr, query)
	curr.close()
	conn.commit()
	DB_LOG.info(f'Reset lda_run flags.')


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
