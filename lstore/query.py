from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):

        record = self.table.page_directory.get(primary_key)
        if not record:
            return False
        
        del self.table.page_directory[primary_key]
        return True
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        if len(columns) != self.table.num_columns:
            return False
        
        primary_key = columns[self.table.key]
        if primary_key in self.table.page_directory:
            return False
        
        rid = max(self.table.page_directory.keys(), default=0) + 1
        new_record = Record(rid, primary_key, list(columns))
        self.table.page_directory[primary_key] = new_record
        
        return True

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):

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

        results = []
        
        for record in self.table.page_directory.values():
            if record.columns[search_key_index] == search_key:
                projected_columns = [
                    record.columns[i] if projected_columns_index[i] == 1 else None
                    for i in range(len(record.columns))
                ]
                results.append(Record(record.rid, record.key, projected_columns))
        
        return results if results else False

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    #missing locking
    def update(self, primary_key, *columns):
        if primary_key not in self.table.page_directory:
            return False
        
        record = self.table.page_directory[primary_key]
        new_values = [
            columns[i] if columns[i] is not None else record.columns[i]
            for i in range(self.table.num_columns)
        ]
        
        updated_record = Record(record.rid, primary_key, new_values)
        self.table.page_directory[primary_key] = updated_record
        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        total = 0
        found = False
        
        for primary_key in range(start_range, end_range + 1):
            record = self.table.page_directory.get(primary_key)
            if record:
                total += record.columns[aggregate_column_index]
                found = True
        
        return total if found else False
        

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
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
