from lstore.index import Index
from time import time

# Constants for the physical layout (for future milestones)
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
    :param num_columns: int     # Number of user columns (excluding metadata)
    :param key: int             # Index of table key in the user columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        # The page_directory maps each record’s RID to its (base or tail) record.
        self.page_directory = {}
        # Create the index object.
        self.index = Index(self)
        # A counter for generating unique record IDs.
        self.next_rid = 1
        # A directory mapping primary key values to the RID of the base record.
        self.key_directory = {}

    def __merge(self):
        print("merge is happening")
        # For milestone 1, merge is not implemented.
        pass
