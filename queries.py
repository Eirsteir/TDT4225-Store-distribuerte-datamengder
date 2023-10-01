from tabulate import tabulate
from DbConnector import DbConnector


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

    def query_10_longest_distances_per_transportation_mode_per_day(self):
        query = """
            SELECT
                user_id,
                transportation_mode,
                travel_date,
                distance AS max_distance
            FROM
                (
                SELECT
                    a.user_id,
                    a.transportation_mode,
                    DATE(a.start_date_time) AS travel_date,
                    MAX(ST_Distance_Sphere(point(t1.lon, t1.lat), point(t2.lon, t2.lat))) AS distance
                FROM
                    Activity a
                JOIN
                    TrackPoint t1 ON a.id = t1.activity_id
                JOIN
                    TrackPoint t2 ON a.id = t2.activity_id
                WHERE
                    DATE(a.start_date_time) = DATE(t1.date_time) AND
                    DATE(a.start_date_time) = DATE(t2.date_time) AND
                    t1.date_time < t2.date_time
                GROUP BY
                    a.user_id,
                    a.transportation_mode,
                    travel_date
            );
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(f"Users that have traveled the longest total distance in one day for each transportation mode: \n{tabulate(result)} ")


    def query_11_users_with_invalid_activities(self):
        query = """
        SELECT
            a.user_id,
            COUNT(a.id)
        FROM
            Activity a
        JOIN TrackPoint t1 ON
            a.id = t1.activity_id
        JOIN TrackPoint t2 ON
            a.id = t2.activity_id
        WHERE
            t2.date_time > t1.date_time AND TIMESTAMPDIFF(
                MINUTE,
                t1.date_time,
                t2.date_time
            ) > 5
        GROUP BY
            a.user_id
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(f"All users with invalid activities: \n{tabulate(result)} ")

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
        print(f"All users with registered transportation modes and their most used transportation mode: \n{tabulate(result)} ")


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
