import os
import pickle
import threading
from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    """
    :param name: string         # Table name
    :param num_columns: int     # Number of columns (all are integers)
    :param key: int             # Index of primary key column
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.base_pages = []  # Stores base pages
        self.tail_pages = []  # Stores tail pages
        self.TPS = {}  # Track last merged Tail Page Sequence

        # Start merge thread
        self.merge_thread = threading.Thread(target=self.__merge, daemon=True)
        self.merge_thread.start()

    def __merge(self):
        """
        Background process that merges tail pages into base pages periodically.
        """
        while True:
            print(f"Starting merge for table: {self.name}")

            for base_page in self.base_pages:
                tail_records = [r for r in self.tail_pages if r.rid == base_page.rid]
                tail_records.sort(key=lambda r: r.rid, reverse=True)  # Apply newest updates first

                for tail in tail_records:
                    base_page.columns = tail.columns  # Apply latest values

                # Update TPS (last merged tail record)
                self.TPS[base_page.rid] = tail_records[0].rid if tail_records else self.TPS.get(base_page.rid, 0)

            print(f"Merge completed for table: {self.name}")
            threading.Event().wait(10)  # Run merge every 10 seconds

    def load_from_disk(self, db_path):
        """
        Loads table metadata and pages from disk.
        """
        table_file = os.path.join(db_path, f"{self.name}.pkl")
        if os.path.exists(table_file):
            with open(table_file, "rb") as f:
                table_data = pickle.load(f)
                self.page_directory = table_data["page_directory"]
                self.base_pages = table_data["base_pages"]
                self.tail_pages = table_data["tail_pages"]
                self.TPS = table_data["TPS"]
            print(f"Table {self.name} loaded from disk.")

    def save_to_disk(self, db_path):
        """
        Saves table metadata and pages to disk.
        """
        table_file = os.path.join(db_path, f"{self.name}.pkl")
        with open(table_file, "wb") as f:
            pickle.dump({
                "page_directory": self.page_directory,
                "base_pages": self.base_pages,
                "tail_pages": self.tail_pages,
                "TPS": self.TPS
            }, f)
        print(f"Table {self.name} saved to disk.")
