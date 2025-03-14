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
        self.page_directory = {}  # Maps RID → (Base page index, offset)
        self.index = Index(self)
        self.base_pages = []  # Stores base records
        self.tail_pages = []  # Stores updates
        self.TPS = {}  # Last merged tail record per base page
        self.lock = threading.Lock()  # Ensure thread safety

        # Start merge thread
        self.merge_thread = threading.Thread(target=self.__merge, daemon=True)
        self.merge_thread.start()

    def insert(self, rid, key, columns):
        """
        Inserts a new record into the table.
        """
        with self.lock:
            record = Record(rid, key, columns)
            self.base_pages.append(record)  # Store in base pages

            # Update the page directory
            self.page_directory[rid] = record

            # Update primary key index
            self.index.locate(key)[0].append(rid)  # Store the RID in index

            print(f"✅ INSERT SUCCESS: Stored record {columns} with RID {rid}")

    def select(self, key):
        """
        Retrieves a record by primary key.
        """
        with self.lock:
            rids = self.index.locate(key)[0]
            if not rids:
                print(f"⚠️ SELECT ERROR: No matching records found for key {key}")
                return []

            return [self.page_directory[rid] for rid in rids]

    def __merge(self):
        """
        Background process that merges tail pages into base pages periodically.
        """
        while True:
            with self.lock:
                print(f"🔄 Starting merge for table: {self.name}")

                for base_page in self.base_pages:
                    key = base_page.key
                    tail_records = [
                        r for r in self.tail_pages if r.key == key
                    ]
                    tail_records.sort(key=lambda r: r.rid, reverse=True)  # Apply newest updates first

                    if tail_records:
                        base_page.columns = tail_records[0].columns  # Apply latest values
                        self.TPS[base_page.rid] = tail_records[0].rid  # Update TPS

                print(f"✅ Merge completed for table: {self.name}")

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
            print(f"📂 Table {self.name} loaded from disk.")

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
        print(f"💾 Table {self.name} saved to disk.")
