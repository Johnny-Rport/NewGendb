"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if self.indices[column] is None:
            return []  # No index, return empty list
        return list(self.indices[column].get(value, []))
        

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if self.indices[column] is None:
            return []  # No index, return empty list
        index = self.indices[column]
        result = []
        for key in index.irange(begin, end):
            result.extend(index[key])
        return result

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] is not None:
            return  # Index already exists
        
        self.indices[column_number] = SortedDict()
        
        # Populate the index with existing data
        for rid, record in self.table.page_directory.items():
            value = record.columns[column_number]
            if value not in self.indices[column_number]:
                self.indices[column_number][value] = []
            self.indices[column_number][value].append(rid)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
