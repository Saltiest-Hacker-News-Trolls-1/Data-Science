
import psycopg2
from decouple import config

def sqlQuery(conn, query):
	""" returns SQL query translated to psychopg2
		SELECT DISTINCT by FROM items;
	"""

	curr = conn.cursor()
	curr.execute(query)
	results = curr.fetchall()
	curr.close()
	conn.commit()
	return results


def query_with_connection(query):
	with psycopg2.connect(dbname=config("DB_DB"), user=config("DB_USER"), password=config("DB_PASSWORD"), host=config("DB_HOST")) as conn:
		results = sqlQuery(conn, query)
	
	return (results)
