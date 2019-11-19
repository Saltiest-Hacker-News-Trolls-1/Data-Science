
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
