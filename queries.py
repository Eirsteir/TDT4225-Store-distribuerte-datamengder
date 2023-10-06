import argparse
import itertools
from tabulate import tabulate
from DbConnector import DbConnector
import pandas as pd
import numpy as np
# from scipy.spatial import cKDTree
# from geopy.distance import geodesic


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

    def query_seven(self):
        subquery = "select user_id, transportation_mode, start_date_time, end_date_time " \
                "from Activity " \
                "where DATE(end_date_time) > DATE(start_date_time)"
                                
        query_a = "select COUNT(DISTINCT user_id) AS user_count " \
                "from ( %s ) as subquery "
        self.cursor.execute(query_a % subquery)
        result_a = self.cursor.fetchall()

        print(f"Number of users who have activities spanning two dates: \n{tabulate(result_a)}")

        query_b = "SELECT user_id, transportation_mode, TIMESTAMPDIFF(SECOND, start_date_time, end_date_time) AS duration " \
                "FROM ( %s ) AS subquery "
        
        self.cursor.execute(query_b % subquery)
        result_b = self.cursor.fetchall()
        print(f"Duration of activities spanning two dates: \n{tabulate(result_b)}")

    # Assuming that the task wants us to find each single person who has been close to any other person both in time and space
    def query_eight(self):
        query = """SELECT Activity.user_id, TrackPoint.date_time, TrackPoint.lat, TrackPoint.lon 
                   FROM TrackPoint 
                   INNER JOIN Activity ON TrackPoint.activity_id = Activity.id 
                   ORDER BY TrackPoint.date_time 
                """
        
        df = pd.read_sql(query, self.db_connection)

        def haversine(lat1, lon1, lat2, lon2, radius=6371):
            """
            Calculate the distance between two points on a sphere using the Haversine formula.
            """
            dlat = np.radians(lat2 - lat1)
            dlon = np.radians(lon2 - lon1)
            a = np.sin(dlat / 2) ** 2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2) ** 2
            c = 2 * np.arcsin(np.sqrt(a))
            return radius * c

        df['distance'] = haversine(df['lat'].shift(), df['lon'].shift(), df['lat'], df['lon'])
        df["time_diff"] = df.groupby("user_id")["date_time"].diff()

        m1 = df["time_diff"] <= pd.Timedelta(seconds=30)
        m3 = df["user_id"] != df["user_id"].shift()
        m2 = df["distance"] <= 50
        n_unique = df[m1 & m2 & m3]["user_id"].nunique()
        print(f"Number of unique user ids: {n_unique}")



def main(query: int):
    program = None
    try:
        program = Queries()
        table_names = ["Activity", "TrackPoint", "User"]
        # cleanly run queries based on argument
        if query == 1:
            for name in table_names:
                program.query_one(table_name=name)
        elif query == 2:
            program.query_two()
        elif query == 3:
            program.query_three()
        elif query == 4:
            program.query_four()
        elif query == 5:
            program.query_five()
        elif query == 6:
            program.query_six()
        elif query == 7:
            program.query_seven()
        elif query == 8:
            program.query_eight()
        else:
            print("ERROR: Invalid query number")

        # for name in table_names:
        #     program.query_one(table_name=name)

        # program.query_two()
        # program.query_three()
        # program.query_four()
        # program.query_five()
        # program.query_six()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
        raise e
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    # Use args to be able to choose which query you want to run
    parser = argparse.ArgumentParser(description="Choose query")
    parser.add_argument("-query", type=int, help="Choose query")
    args = parser.parse_args()
    main(args.query)
