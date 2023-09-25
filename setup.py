from DbConnector import DbConnector
from tabulate import tabulate


class SetupProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        # Create User table
        query = "CREATE TABLE User (id VARCHAR(255) PRIMARY KEY, has_labels BOOLEAN)"
        self.cursor.execute(query)

        # Create Activity table
        query = """CREATE TABLE Activity (id INT PRIMARY KEY,
            user_id VARCHAR(255),
            transportation_mode VARCHAR(255),
            start_date_time DATETIME,
            end_date_time DATETIME,
            FOREIGN KEY (user_id) REFERENCES User(id))"""
        self.cursor.execute(query)

        # Create TrackPoint table
        query = """CREATE TABLE TrackPoint (id INT PRIMARY KEY AUTO_INCREMENT,
            activity_id INT,
            lat DOUBLE,
            lon DOUBLE,
            altitude INT,
            date_days DOUBLE,
            date_time DATETIME,
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
        # program.drop_table("Activity")
        # program.drop_table("User")
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
