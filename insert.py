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

    def get_timestamps(self, df):
        start_date = df.iloc[0, 5]
        start_time = df.iloc[0, 6]
        start_date_time = start_date + " " + start_time
        end_date = df.iloc[-1, 5]
        end_time = df.iloc[-1, 6]
        end_date_time = end_date + " " + end_time
        return start_date_time, end_date_time
    
    def load_activities(self):
        data_dir = "./dataset/Data"
        activity_records = []

        for user_id in os.listdir(data_dir):
            user_dir = data_dir + "/" + user_id
            labels = {}

            if "labels.txt" in os.listdir(user_dir):
                with open(user_dir + "/labels.txt", "r") as label_file:
                    lines = label_file.readlines()
                    for line in lines[1:]:
                        start, end, label = line.strip().split("\t")
                        start, end = start.replace("/", "-"), end.replace("/", "-")
                        labels[(start, end)] = label

            for activity in os.listdir(user_dir + "/Trajectory"):
                activity_df = pd.read_csv(user_dir + "/Trajectory/" + activity, skiprows=6, header=None)

                if len(activity_df) > 2500:
                    continue

                start_date_time, end_date_time = self.get_timestamps(activity_df)
                transportation_mode = labels.get((start_date_time, end_date_time), None)

                if labels and not transportation_mode:
                    # label found but no match on times, ignore
                    continue

                activity_records.append((user_id, transportation_mode, start_date_time, end_date_time))

            self.cursor.executemany("INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)", activity_records)
            self.connection.db_connection.commit()
            print(f"{self.cursor.rowcount} records inserted successfully into Activity table (user {user_id})")

            activity_records.clear()


if __name__ == "__main__":
    loader = DataLoader()
    # loader.load_users()
    loader.load_activities()
    loader.connection.close_connection()


