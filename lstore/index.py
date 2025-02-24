import pickle
import os
from sortedcontainers import SortedDict  # Used for B-Tree indexing

class Index:
    def __init__(self, table):
        """
        Initializes index structure.
        - Uses B-Trees (SortedDict) for fast range queries.
        """
        self.table = table
        self.indices = [None] * table.num_columns  # One index per column

    def locate(self, column, value):
        """
        Returns all RIDs of records where column[column] == value.
        """
        print(f"DEBUG: locate() called with value={value}, column={column}")
        print(f"DEBUG: Indices length = {len(self.indices)}")

        # Ensure the column index is within valid bounds
        if column < 0 or column >= len(self.indices):
            print(f"ERROR: Column index {column} is out of range.")
            return []

        # Check if the index for the column exists
        if self.indices[column] is None:
            print(f"DEBUG: No index exists for column {column}.")
            return []  # No index exists for this column

        # Return the list of RIDs where the column value matches
        index = self.indices[column]
        rids = index.get(value, [])
        
        # Debug print to check what the index and result are
        print(f"DEBUG: Found RIDs for column {column} with value {value}: {rids}")
        
        return rids

    def locate_range(self, begin, end, column):
        """
        Returns all RIDs of records where begin <= column[column] <= end.
        """
        if column < 0 or column >= len(self.indices) or self.indices[column] is None:
            return []  # No index exists

        index = self.indices[column]
        result = []

        for key in index.irange(begin, end):
            result.extend(index[key])

        return result

    def create_index(self, column_number):
        """
        Creates an index on the specified column.
        """
        if column_number < 0 or column_number >= len(self.indices):
            print(f"ERROR: Invalid column index {column_number}")
            return
        
        if self.indices[column_number] is None:
            self.indices[column_number] = SortedDict()  # B-Tree structure
            print(f"Index created on column {column_number}")

        # Populate index with existing records
        for rid, record in self.table.page_directory.items():
            value = record.columns[column_number]
            if value not in self.indices[column_number]:
                self.indices[column_number][value] = []
            self.indices[column_number][value].append(rid)

    def drop_index(self, column_number):
        """
        Drops the index on the specified column.
        """
        if column_number < 0 or column_number >= len(self.indices):
            print(f"ERROR: Invalid column index {column_number}")
            return

        self.indices[column_number] = None
        print(f"Index dropped on column {column_number}")

    def update_index(self, column, old_value, new_value, rid):
        """
        Updates an index when a record's value is modified.
        """
        if column < 0 or column >= len(self.indices) or self.indices[column] is None:
            return  # No index exists

        # Remove old value if it exists
        if old_value in self.indices[column]:
            if rid in self.indices[column][old_value]:
                self.indices[column][old_value].remove(rid)
                if not self.indices[column][old_value]:  # Remove key if empty
                    del self.indices[column][old_value]

        # Add new value
        if new_value not in self.indices[column]:
            self.indices[column][new_value] = []
        self.indices[column][new_value].append(rid)

    def save_to_disk(self, db_path):
        """
        Saves all indexes to disk.
        """
        index_file = os.path.join(db_path, f"{self.table.name}_index.pkl")
        with open(index_file, "wb") as f:
            pickle.dump(self.indices, f)
        print(f"Indexes saved for table {self.table.name}")

    def load_from_disk(self, db_path):
        """
        Loads indexes from disk.
        """
        index_file = os.path.join(db_path, f"{self.table.name}_index.pkl")
        if os.path.exists(index_file):
            with open(index_file, "rb") as f:
                self.indices = pickle.load(f)
            print(f"Indexes loaded for table {self.table.name}")
