import sqlite3

from contextlib import closing

def create_connection(database_file):
    return sqlite3.connect(database_file)

def create_table(connection):
    with closing(connection.cursor()) as cursor:
        query = """CREATE TABLE IF NOT EXISTS LogEvent (
            event_product text,
            event_category text,
            event_type text,
            event_date text,
            PRIMARY KEY(event_product, event_category, event_type)
        ); """
        cursor.execute(query)

def upsert(connection, event_product, event_category, event_type, event_date):
    try:
        with closing(connection.cursor()) as cursor:
            query = """INSERT INTO LogEvent(event_product, event_category, event_type, event_date)
            VALUES (?, ?, ?, ?) ON CONFLICT(event_product, event_category, event_type) DO UPDATE set event_date=?"""
            values=(event_product, event_category, event_type, event_date, event_date)
            cursor.execute(query, values)
            connection.commit()
    except Error as e:
        print("Error occurred while replacing log event: " + str(e))
        connection.rollback()

def init():
    database_file = r"/var/lib/cassandra/nova/novautil.db"

    connection = create_connection(database_file)
    create_table(connection)

    return connection

def main():
    with closing(init()) as conn:
        print("Log events stored in Nova DB:")

        query = '''SELECT * FROM LogEvent'''
        with closing(conn.cursor()) as cursor:
            cursor.execute(query)
            output = cursor.fetchall()
            for row in output:
                print(row)

if __name__ == '__main__':
    main()
