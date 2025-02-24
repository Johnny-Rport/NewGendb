import os
import pickle
from lstore.table import Table

class Database:
    def __init__(self):
        # We use a dictionary to hold tables by name.
        self.tables = {}
        self.db_path = None  # Path to database storage folder

    def open(self, path):
        """
        Opens the database from the specified path.
        Loads tables and their data if they exist.
        """
        self.db_path = path

        # Create the directory if it doesn’t exist
        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path)

        meta_file = os.path.join(self.db_path, "meta.pkl")

        # Load metadata if it exists
        if os.path.exists(meta_file):
            with open(meta_file, "rb") as f:
                table_metadata = pickle.load(f)

            # Load each table from disk
            for table_name, (num_columns, key_index) in table_metadata.items():
                table = Table(table_name, num_columns, key_index)
                table.load_from_disk(self.db_path)
                self.tables[table_name] = table

            print(f"Database opened from {self.db_path}, {len(self.tables)} tables loaded.")
        else:
            print("No existing database found. Creating a new one.")

    def close(self):
        """
        Saves the database state before closing.
        Writes metadata and all table data to disk.
        """
        if self.db_path is None:
            print("Error: Database path not set. Cannot close.")
            return

        # Save table metadata
        meta_file = os.path.join(self.db_path, "meta.pkl")
        table_metadata = {
            name: (table.num_columns, table.key)  
            for name, table in self.tables.items()
        }
        with open(meta_file, "wb") as f:
            pickle.dump(table_metadata, f)

        # Save each table to disk
        for table in self.tables.values():
            table.save_to_disk(self.db_path)

        print(f"Database saved to {self.db_path} and closed.")

    def create_table(self, name, num_columns, key_index):
        """
        Creates a new table and adds it to the database.
        """
        table = Table(name, num_columns, key_index)
        self.tables[name] = table
        return table

    def drop_table(self, name):
        """
        Deletes the specified table from the database.
        """
        if name in self.tables:
            del self.tables[name]
            table_file = os.path.join(self.db_path, f"{name}.pkl")
            if os.path.exists(table_file):
                os.remove(table_file)  # Delete table file from disk
            print(f"Table {name} deleted.")

    def get_table(self, name):
        """
        Returns a table by name, if it exists.
        """
        return self.tables.get(name, None)
