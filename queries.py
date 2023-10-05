import pandas as pd
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
        number_of_trackpoints_per_user = "SELECT Activity.user_id, " \
                                         "  COUNT(TrackPoint.id) AS Number_of_trackpoints " \
                                         "FROM Activity " \
                                         "INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id " \
                                         "GROUP BY Activity.user_id"

        query = "SELECT AVG(Number_of_trackpoints), " \
                "       MAX(Number_of_trackpoints), " \
                "       MIN(Number_of_trackpoints) " \
                "FROM ( %s ) AS number_of_trackpoints_per_user_query"
        self.cursor.execute(query % number_of_trackpoints_per_user)
        result = self.cursor.fetchone()
        average, maximum, minimum = result
        print(f"Average: {average}, maximum: {maximum} and minimum: {minimum} trackpoints per user")

    def query_three(self):
        query = "SELECT Activity.user_id, " \
                "   COUNT(Activity.id) AS Number_of_activities  " \
                "FROM Activity " \
                "GROUP BY Activity.user_id " \
                "ORDER BY Number_of_activities DESC " \
                "LIMIT 15"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(
            f"Top 15 users with the highest number of activities: \n{tabulate(result, ['User ID', 'Number of Activities'])}")

    def query_four(self):
        query = "SELECT DISTINCT user_id " \
                "FROM Activity " \
                "WHERE transportation_mode = 'bus'"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(f"Users taken the bus:")
        for user in result:
            print(user[0])

    def query_five(self):
        query = "SELECT user_id, " \
                "   COUNT(DISTINCT transportation_mode) AS transport_count " \
                "FROM Activity " \
                "WHERE transportation_mode IS NOT NULL " \
                "GROUP BY user_id " \
                "ORDER BY transport_count DESC " \
                "LIMIT 10"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(
            f"Top 10 users with different transportation modes: \n{tabulate(result, ['User ID', 'Number of transportation modes'])}")

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
                                    "JOIN Activity ON Activity.id = TrackPoint.activity_id "
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

    def query_10_longest_distances_per_transportation_mode_per_day(self):
        query = """
             SELECT 
                transportation_mode,
                user_id,
                MAX(total_distance) AS max_distance
            FROM (
                SELECT
                    a.user_id,
                    a.transportation_mode,
                    DATE(tp1.date_time) as travel_date,
                    SUM(
                        ST_DISTANCE_SPHERE(
                            POINT(tp1.lon, tp1.lat),
                            POINT(tp2.lon, tp2.lat)
                        ) / 1000 
                    ) AS total_distance
                FROM
                    Activity a
                JOIN 
                    TrackPoint tp1 ON a.id = tp1.activity_id
                JOIN
                    TrackPoint tp2 ON a.id = tp2.activity_id AND tp1.id = tp2.id - 1
                WHERE
                    a.transportation_mode IS NOT NULL
                GROUP BY 
                    a.user_id,
                    a.transportation_mode,
                    DATE(tp1.date_time)
            ) AS distances
            GROUP BY
                transportation_mode, user_id;
        """
        result = pd.read_sql_query(query, self.db_connection)
        result = result.sort_values('max_distance', ascending=False).drop_duplicates(['transportation_mode'])

        print(
            f"Users that have traveled the longest total distance in one day for each transportation mode: \n{tabulate(result, headers=['transportation_mode', 'user_id', 'distance (km)'])} ")

    def query_11_users_with_invalid_activities(self):
        query = """
            SELECT
                a.user_id,
                COUNT(DISTINCT a.id) as invalid_activities
            FROM
                Activity a
            JOIN TrackPoint t1 ON
                a.id = t1.activity_id
            JOIN TrackPoint t2 ON
                a.id = t2.activity_id
                AND t1.id = t2.id - 1
                AND TIMESTAMPDIFF(
                    MINUTE,
                    t1.date_time,
                    t2.date_time
                ) > 5
            GROUP BY
                a.user_id;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(f"All users with invalid activities: \n{tabulate(result, headers=['user_id', 'invalid_activities'])} ")

    def query_12_users_with_their_most_used_transportation_mode(self):
        query = """
            SELECT
                user_id,
                transportation_mode
            FROM
                (
                SELECT
                    user_id,
                    transportation_mode,
                    ROW_NUMBER() OVER(
                    PARTITION BY user_id
                ORDER BY
                    COUNT(*)
                DESC
                ) AS activity_rank
            FROM
                Activity
            WHERE
                transportation_mode IS NOT NULL
            GROUP BY
                user_id,
                transportation_mode) AS ranked_activities
                WHERE
                    activity_rank = 1
                GROUP BY
                    user_id,
                    transportation_mode
                ORDER BY
                    user_id;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(
            f"All users with registered transportation modes and their most used transportation mode: \n{tabulate(result, headers=['user_id', 'transportation_mode'])} ")


def main():
    program = None
    try:
        program = Queries()
        table_names = ["Activity", "TrackPoint", "User"]

        for name in table_names:
            program.query_one(table_name=name)

        # program.query_two()
        # program.query_three()
        # program.query_four()
        # program.query_five()
        # program.query_six()
        # program.query_nine()
        program.query_10_longest_distances_per_transportation_mode_per_day()
        program.query_11_users_with_invalid_activities()
        program.query_12_users_with_their_most_used_transportation_mode()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
