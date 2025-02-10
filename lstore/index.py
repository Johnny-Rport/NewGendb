"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = [None] *  table.num_columns

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        print(f"Index lookup for value {value} in column {column}")  # Debugging output
        if self.indices[column] is None:
            print(f"Index lookup failed: No index for column {column}")  # Debugging output
            return []

        rid_list = self.indices[column].get(value, [])
        print(f"Located RIDs in index: {rid_list}")  # Debugging output
        return rid_list

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if(self.indices[column] == None):
            return []
            
        result = []
        for key in range(begin, end + 1):
            if(key in self.indices[column]):
                result.extend(self.indices[column][key])
        return result

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if(self.indices[column_number] == None):
            return
        
        self.indices[column_number] = {}

        for rid, record in self.table.page_directory.items():
            column_value = record.columns[column_number]
            if(column_value not in self.indices[column_number]):
                self.indices[column_number][column_value] = []
            self.indices[column_number][column_value].append(rid)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
