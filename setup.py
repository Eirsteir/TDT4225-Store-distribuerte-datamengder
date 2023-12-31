from DbConnector import DbConnector
from tabulate import tabulate


class SetupProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        # Create User table
        query = "CREATE TABLE IF NOT EXISTS User (id VARCHAR(255) NOT NULL PRIMARY KEY, has_labels BOOLEAN NOT NULL DEFAULT false)"
        self.cursor.execute(query)

        # Create Activity table
        query = """CREATE TABLE IF NOT EXISTS Activity (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            user_id VARCHAR(255),
            transportation_mode VARCHAR(255),
            start_date_time DATETIME NOT NULL,
            end_date_time DATETIME NOT NULL,
            FOREIGN KEY (user_id) REFERENCES User(id))"""
        self.cursor.execute(query)

        # Create TrackPoint table
        query = """CREATE TABLE IF NOT EXISTS TrackPoint (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            activity_id INT,
            lat DOUBLE,
            lon DOUBLE,
            altitude INT,
            date_days DOUBLE NOT NULL,
            date_time DATETIME NOT NULL,
            FOREIGN KEY (activity_id) REFERENCES Activity(id))"""
        self.cursor.execute(query)
        
        # This adds table_name to the %s variable and executes the query
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = SetupProgram()
        program.create_tables()
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
