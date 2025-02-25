#  * Program Name: Database System -> L_Store Concepts
#  * Author: Next Generation
#  * Date: Feb 11/2025 (Final_Version_v04)
#  * Description:
from lstore.index import Index
from lstore.page import Page
from lstore.page_directory import Page_directory
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
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    #TODO: Imlpemenet page_directory class to store pages
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        # The page_directory maps each record’s RID to its (base or tail) record.
        self.page_directory = {}
        self.index = Index(self) # Creative a index object to support Query.py
        # A counter for generating unique record IDs.
        self.next_rid = 1
        # A directory mapping primary key values to the RID of the base record.
        self.key_directory = {} # Used
        pass
    
    # Update
    def merge(self):
        print("Merging table", self.name)
        for key_val, base_rid in self.key_directory.items():
            base_record = self.page_directory.get(base_rid)
            if base_record is None:
                continue
            # Build the tail chain in order from most recent to oldest.
            tail_chain = []
            current_rid = base_record.get('indirection')
            while current_rid is not None:
                tail_record = self.page_directory.get(current_rid)
                if tail_record is None:
                    break
                tail_chain.append(tail_record)
                current_rid = tail_record.get('indirection')
            # Now tail_chain[0] is the most recent update.
            merged_data = list(base_record['data'])
            # For each column, if there's a tail record that updated it, use the value from the most recent tail.
            for i in range(self.num_columns):
                for tail in tail_chain:
                    if tail['data'][i] is not None:
                        merged_data[i] = tail['data'][i]
                        break  # Stop at the first (most recent) update for this column.
            base_record['data'] = merged_data
            base_record['indirection'] = None
            pass


    def insert_record(self, data: list):
        record = Record(self.rid, data[0], data[1:])
        try:
            self.page_directory.get_tail(0).write(record.rid, record.key)
            for index in range(1, self.num_columns):
                self.page_directory.get_tail(index).write(record.rid, record.columns[index-1])
        except MemoryError:
            self.page_directory.expand()
            self.page_directory.get_tail(0).write(record.rid, record.key)
            for index in range(1, self.num_columns):
                self.page_directory.get_tail(index).write(record.rid, record.columns[index-1])

        self.rid += 1
        # Changed area for the Milestone2
        # Saving pages into the disk
        if self.disk_manager:
            self.disk_manager.save_pages_from_disk(self.name, record.rid, record)

    #TODO: If there are more than 5 pages, implement way to get proper page
    # Problem would occur in get_tail(i), what if we want a previous page not the latest page?

    # Changed area for the Milestone2
    """
    get the record (Or data), based on the rid to access
        @params requirment: 
            
    """
    def read_record(self, rid):
        if rid in self.page_directory:
            return self.page_directory[rid]

        if self.disk_manager:
            record = self.disk_manager.get_pages_from_disk(self.name, rid)
            if record:
                self.page_directory[rid] = record
                return record
        return None


#   FOR BASE PAGES
#   5 columns
#   i + (length_of_column - 1) * 5

# --Indirection -----------------
#   meta1       | Page 0  | Page 1 | and so on
#   meta2       | Page 5  | Page 6
#   meta3       | Page 10 | Page 11
#   meta4       | Page 15 | Page 16
#   meta5       | Page 20 | Page 21
