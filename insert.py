import os

from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd



class DataLoader:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def load_users(self):
        data_dir = "./dataset"
        user_records = []

        with open(data_dir + "/labeled_ids.txt", "r") as labeled_ids:
            user_ids_with_labels = [line.strip() for line in labeled_ids]

        for user_id in os.listdir(data_dir + "/Data"):
            has_labels = user_id in user_ids_with_labels
            user_records.append((user_id, has_labels))

        self.cursor.executemany("INSERT INTO User (id, has_labels) VALUES (%s, %s)", user_records)
        self.connection.db_connection.commit()
        print(self.cursor.rowcount, "Record inserted successfully into User table")

    def create_datetime(self, date, time):
        date_time = dt.DateTime(date, time)
        return date_time

    def match_starttime(self, label_time, trajectory_time):
        # strip label time of "/" " " and ":"
        label_time = label_time.replace("/", "")
        label_time = label_time.replace(" ", "")
        label_time = label_time.replace(":", "")


    # def get_transportation_mode(self, file_name):

    def get_timestamps(self, activity_df):
        start_date = activity_df.iloc[0,5]
        start_time = activity_df.iloc[0,6]
        start_date_time = start_date + " " + start_time
        end_date = df.iloc[-1,5]
        end_time = df.iloc[-1,6]
        end_date_time = end_date + " " + end_time
        return start_date_time, end_date_time

    
    def load_activities(self):
        data_dir = "./dataset"
        activity_records = []

        for user_id in os.listdir(data_dir + "/Data"):
            user_folder = os.listdir(data_dir + "/Data/" + user_id)
            # if "labels.txt" in user_folder:
            #     for activity_file in user_folder + "/Trajectory":
            #         df = pd.read_csv(activity_file, skiprows=6, header=None)
            #         # get fifth column of df
            #         start_date_time, end_date_time = get_timestamps(df)
            # else:
            for activity_file in user_folder[0] + "/Trajectory":
                df = pd.read_csv(activity_file, skiprows=6, header=None)
                # check if df has more than 2500 lines
                if len(df) <= 2500:
                    start_date_time, end_date_time = self.get_timestamps(df)
                    activity_records.append((user_id, file_name, None, start_date_time, end_date_time))

        self.cursor.executemany("INSERT INTO Activity (user_id, file_name) VALUES (%s, %s)", activity_records)
        self.connection.db_connection.commit()
        print(self.cursor.rowcount, "Record inserted successfully into Activity table")


if __name__ == "__main__":
    loader = DataLoader()
    # loader.load_users()
    loader.load_activities()
    loader.connection.close_connection()


