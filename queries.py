from tabulate import tabulate
from DbConnector import DbConnector
import pandas


class Queries:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def query_one(self, table_name):
        query = "SELECT COUNT(*) FROM %s"
        self.cursor.execute(query % table_name)
        result = self.cursor.fetchone()
        print(f"Entries in {table_name}: {result[0]}")

    def query_two(self):
        number_of_trackpoints_per_user = "SELECT User.id, " \
                                         "  COUNT(TrackPoint.id) AS Number_of_trackpoints " \
                                         "FROM User " \
                                         "INNER JOIN Activity ON User.id = Activity.user_id " \
                                         "INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id " \
                                         "GROUP BY User.id"

        query = "SELECT AVG(Number_of_trackpoints), " \
                "       MAX(Number_of_trackpoints), " \
                "       MIN(Number_of_trackpoints) " \
                "FROM ( %s ) AS number_of_trackpoints_per_user_query"
        self.cursor.execute(query % number_of_trackpoints_per_user)
        result = self.cursor.fetchall()
        print(f"Average, maximum and minimum trackpoints per user")

    def query_three(self):
        query = "SELECT User.id, " \
                "   COUNT(Activity.id) AS Number_of_activities  " \
                "FROM User " \
                "INNER JOIN Activity ON User.id = Activity.user_id " \
                "GROUP BY User.id " \
                "ORDER BY Number_of_activities DESC " \
                "LIMIT 15"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(f"Top 15 users with the highest number of activities: \n{tabulate(result, ['User ID', 'Number of Activities'])}")

    def query_four(self):
        query = "SELECT DISTINCT User.id " \
                "FROM User " \
                "INNER JOIN Activity ON User.id = Activity.user_id " \
                "WHERE Activity.transportation_mode = 'bus'"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(f"Users taken the bus: \n{result}")

    def query_five(self):
        query = "SELECT User.id, " \
                "   COUNT(DISTINCT Activity.transportation_mode) AS transport_count " \
                "FROM User " \
                "INNER JOIN Activity ON User.id = Activity.user_id " \
                "WHERE Activity.transportation_mode IS NOT NULL " \
                "GROUP BY User.id " \
                "ORDER BY transport_count DESC " \
                "LIMIT 10"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(f"Top 10 users with different transportation modes: \n{tabulate(result, ['User ID', 'Number of transportation modes'])}")

    def query_six(self):
        query = "SELECT user_id, transportation_mode, start_date_time, end_date_time " \
                "FROM Activity " \
                "GROUP BY user_id, transportation_mode, start_date_time, end_date_time " \
                "HAVING COUNT(*) > 1;"
        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print(f"duplicates: \n{tabulate(result)} ")

    def query_nine(self):
        query_altitude_trackpoint = "SELECT Activity.user_id, activity_id, altitude " \
                                    "FROM TrackPoint " \
                                    "JOIN Activity ON Activity.id = TrackPoint.activity_id " \

        df = pandas.read_sql(query_altitude_trackpoint, self.db_connection)
        # handle invalid values
        df = df[df['altitude'] != -777]
        # diff(): calculates the difference between current and prev row on altitude ie. altitude difference
        df['altitude_gain'] = df.groupby(['user_id', 'activity_id'])['altitude'].diff()
        # replace negative gains with 0
        df['altitude_gain'] = df['altitude_gain'].apply(lambda x: x if x > 0 else 0)
        # summing up altitude gains, now only positive values
        total_altitude_gain = df.groupby('user_id')['altitude_gain'].sum().reset_index()
        # sort users by gain
        sorted_users = total_altitude_gain.sort_values(by='altitude_gain', ascending=False)
        top_15_users = sorted_users.head(15)
        print(top_15_users)

def main():
    program = None
    try:
        program = Queries()
        table_names = ["Activity", "TrackPoint", "User"]

        for name in table_names:
            program.query_one(table_name=name)

        program.query_two()
        program.query_three()
        program.query_four()
        program.query_five()
        program.query_six()
        program.query_nine()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
