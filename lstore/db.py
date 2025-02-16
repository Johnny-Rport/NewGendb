#  * Program Name: Database System -> L_Store Concepts
#  * Author: Next Generation
#  * Date: Feb 11/2025 (Final_Version_v04)
#  * Description: 
import os
from lstore.table import Table
from lstore.disk import DiskManager

class Database:
    # Changed area for the Milestone2 
    def __init__(self):
        self.tables = {} # Since, we will using dictionary to hold the table by the name 
        self.DiskManager = None

    # Not required for milestone 1.
    # Changed area for the Milestone2 
    """
    Open the databse and then trying to loads the tables from the disk
    The table will be include, the name, columns and key indexs.
        @params requirment: 
            param path: need a path
    """
    def open(self, path):
        self.disk_manager = DiskManager(path)
        for filename in os.listdir(path):
            if filename.endswith("_metadata.pkl"):
                table_name = filename.replace("_metadata.pkl", "")
                table_metadata = self.disk_manager.get_table_of_metadata_from_disk(table_name)
                if table_metadata:
                    table = Table(
                        table_metadata["name"], 
                        table_metadata["num_columns"], 
                        table_metadata["key_index"]
                    )
                    table.next_rid = table_metadata["next_rid"]
                    table.page_directory = table_metadata["page_directory"]
                    self.tables[table_name] = table
    
    """
    close the databse. Before the shutting down. Close function will save all tables and pages into the disk
    The table will be include, the name, columns and key indexs.
        @params requirment: None
    """
    def close(self):
        for table in self.tables.values():
            self.disk_manager.save_table_of_metadata_from_disk(table)
            for page_id, page in table.page_directory.items():
                self.disk_manager.save_page_from_disk(table.name, page_id, page)

    """
    Creates a new table.
    :param name: string         # Table name
    :param num_columns: int     # Number of columns (user columns)
    :param key_index: int       # Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables[name] = table
        return table

    """
    Deletes the specified table.
    """
    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]

    """
    Returns table with the passed name.
    """
    def get_table(self, name):
        return self.tables.get(name, None)


