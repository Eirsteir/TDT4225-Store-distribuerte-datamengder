import os

from DbConnector import DbConnector
from tabulate import tabulate


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
        print(self.cursor.rowcount, "Record inserted successfully into Laptop table")


if __name__ == "__main__":
    loader = DataLoader()
    loader.load_users()
    loader.connection.close_connection()


