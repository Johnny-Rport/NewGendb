"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        pass

    """"""
A data structure holding indices for various columns of a table.
Key column should be indexed by default; other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:
    def __init__(self, table):
        self.table = table
        # One index per user column (we assume table.num_columns is the number of user columns)
        self.indices = [None] * table.num_columns
        # Create an index on the primary key (table.key) by default.
        self.create_index(table.key)

    def locate(self, column, value):
        # Returns the RID(s) of all records with the given value on column "column"
        if self.indices[column] is not None:
            if value in self.indices[column]:
                return self.indices[column][value]
            else:
                return None
        else:
            # Without an index, do a linear scan over the page directory.
            result = []
            for rid, record in self.table.page_directory.items():
                if not record.get('deleted', False) and record['data'][column] == value:
                    result.append(rid)
            return result if result else None

    def locate_range(self, begin, end, column):
        # Returns the RIDs of all records with values in column "column" between "begin" and "end"
        result = []
        if self.indices[column] is not None:
            for value, rid in self.indices[column].items():
                if begin <= value <= end:
                    result.append(rid)
        else:
            for rid, record in self.table.page_directory.items():
                if not record.get('deleted', False):
                    val = record['data'][column]
                    if begin <= val <= end:
                        result.append(rid)
        return result

    def create_index(self, column_number):
        # optional: Create index on specific column
        if self.indices[column_number] is None:
            self.indices[column_number] = {}
            # Populate the index with any existing records.
            for rid, record in self.table.page_directory.items():
                if not record.get('deleted', False):
                    key_val = record['data'][column_number]
                    self.indices[column_number][key_val] = rid

    def drop_index(self, column_number):
        # optional: Drop index of specific column
        self.indices[column_number] = None

    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
