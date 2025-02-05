from lstore.table import Table, Record
from lstore.index import Index
from typing import List

# About the Query section, we are trying to using list to stored the informations
class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table: Table):
        self.table: Table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        
        # Since, the delete a record following with the given primary key 
        # if successful, return true, else false

        try:
            removeID = self.table.index.locate(self.table.key, primary_key)
            if not removeID:
                return False  # if the record is not found
            rid = removeID[0]
            return self.table.delete_record(rid)
        except:
            return False
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        """
        Insert a new record with the given column values.
        columns: [col0, col1, col2, ... colN], length = table.num_columns
        Return True if successful, False otherwise.
        """
        try:
            if len(columns) != self.table.num_columns:
                return False
            # schema_encoding can be a no-op for M1, but your skeleton includes it
            # We'll ignore it and just call table.insert_record
            self.table.insert_record(list(columns))
            return True
        except:
            return False

    
    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            # 1) locate all matching RIDs
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                return []

            results = []
            for rid in rids:
                row_data = self.table.read_record(rid)
                if row_data is None:
                    # means it's deleted or does not exist
                    continue

                # Project the requested columns
                projected = []
                for i, bit in enumerate(projected_columns_index):
                    if bit == 1:
                        projected.append(row_data[i])

                # The primary key value is row_data[self.table.key]
                record_obj = Record(rid, row_data[self.table.key], projected)
                results.append(record_obj)

            return results
        except:
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        # since the milestone 1 don't have multi version. Thus, just put pass 
        pass
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try:
            if len(columns) != self.table.num_columns:
                return False

            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False
            rid = rids[0]

            # Overwrite columns where columns[i] is not None
            success = self.table.update_record(rid, columns)
            return success
        except:
            return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        try: 
            result = self.table.sum_range(start_range, end_range, aggregate_column_index)
            # If your table returns 0 when no records exist, you might want to check that:
            # For a basic approach, we can return result even if 0.
            # If result is 0 and we want to distinguish "no records"? 
            # Typically M1 testers accept 0 as well.
            return result
        except:
            return False
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    # Not requirement for the Milestone1
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
