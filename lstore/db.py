import os
from lstore.table import Table
from lstore.disk import DiskManager
from lstore.bufferPool import BufferPoolManager

class Database:
    def __init__(self, path="default_db"):
        """
        Initializes the database.
        If no path is provided, it defaults to 'default_db'.
        """
        self.tables = {}  # Dictionary to hold tables
        self.db_path = path  # Store the database path
        self.disk_manager = DiskManager(self.db_path)
        self.buffer_pool = BufferPoolManager(self.db_path, pool_size=10)  # Buffer pool initialization

    def open(self, path=None):
        """ Opens the database and loads tables from disk """
        if path:
            self.db_path = path  # Update path if provided
        self.disk_manager = DiskManager(self.db_path)
        self.buffer_pool = BufferPoolManager(self.db_path, pool_size=10)

        for filename in os.listdir(self.db_path):
            if filename.endswith("_metadata.pkl"):
                table_name = filename.replace("_metadata.pkl", "")
                table_metadata = self.disk_manager.get_table_of_metadata_from_disk(table_name)
                if table_metadata:
                    table = Table(
                        table_metadata["name"], 
                        table_metadata["num_columns"], 
                        table_metadata["key_index"],
                        self.buffer_pool  # Pass buffer pool to table
                    )
                    table.next_rid = table_metadata["next_rid"]
                    table.page_directory = table_metadata["page_directory"]
                    self.tables[table_name] = table

    def close(self):
        """ Closes the database, ensuring all data is written back to disk """
        for table in self.tables.values():
            self.disk_manager.save_table_of_metadata_from_disk(table)
            for page_id, page in table.page_directory.items():
                self.disk_manager.save_page_from_disk(table.name, page_id, page)
        self.buffer_pool.close()  # Flush all dirty pages before shutting down

    def create_table(self, name, num_columns, key_index):
        """ Creates a new table and registers it in the database """
        table = Table(name, num_columns, key_index, self.buffer_pool)
        self.tables[name] = table
        return table

    def drop_table(self, name):
        """ Deletes a table from the database """
        if name in self.tables:
            del self.tables[name]

    def get_table(self, name):
        """ Retrieves a table by name """
        return self.tables.get(name, None)
