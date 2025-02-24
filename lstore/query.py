from lstore.table import Table, Record
from lstore.index import Index

class Query:
    """
    Provides SQL-like queries: insert, select, update, delete, sum.
    Queries that fail must return False.
    """

    def __init__(self, table):
        self.table = table

    def insert(self, *columns):
        """
        Inserts a new record into the table.
        """
        if len(columns) != self.table.num_columns:
            return False  # Invalid column count

        rid = len(self.table.page_directory) + 1  # Generate new RID
        record = Record(rid, columns[self.table.key], list(columns))

        # Store in page directory
        self.table.page_directory[rid] = record
        self.table.base_pages.append(record)  # Append to base page

        # Update primary key index
        self.table.index.update_index(self.table.key, None, columns[self.table.key], rid)

        return True

    def select(self, search_key, search_key_index, projected_columns_index):
        """
        Selects records matching the given search key and projects the desired columns.
        """
        print(f"DEBUG: search_key_index = {search_key_index}, num_columns = {self.table.num_columns}")
        
        # Locate matching records using the correct column index (search_key_index)
        rids = self.table.index.locate(search_key_index, search_key)

        if not rids:
            return []  # Return an empty list if no records found

        results = []
        for rid in rids:
            record = self.table.page_directory.get(rid)
            
            if record:
                # Project the required columns based on projected_columns_index
                projected_values = [
                    record.columns[i] if projected_columns_index[i] else None
                    for i in range(self.table.num_columns)
                ]
                # Add the record with the projected values to the results
                results.append(Record(record.rid, record.key, projected_values))

        return results

    def update(self, primary_key, *columns):
        """
        Updates a record with the specified primary key.
        """
        rids = self.table.index.locate(self.table.key, primary_key)

        if not rids:
            return False  # No matching record

        rid = rids[0]
        record = self.table.page_directory.get(rid)

        if not record:
            return False  # Record doesn't exist

        updated_values = list(record.columns)
        for i, value in enumerate(columns):
            if value is not None:
                updated_values[i] = value

        new_rid = len(self.table.page_directory) + 1  # New tail RID
        updated_record = Record(new_rid, primary_key, updated_values)

        # Append to tail pages
        self.table.tail_pages.append(updated_record)

        # Update page directory & index
        self.table.page_directory[new_rid] = updated_record
        self.table.index.update_index(self.table.key, primary_key, primary_key, new_rid)

        return True

    def delete(self, primary_key):
        """
        Deletes a record with the specified primary key.
        """
        rids = self.table.index.locate(self.table.key, primary_key)

        if not rids:
            return False  # No record found

        rid = rids[0]

        # Remove from table storage
        if rid in self.table.page_directory:
            del self.table.page_directory[rid]

        # Remove from index
        self.table.index.update_index(self.table.key, primary_key, None, rid)

        return True

    def sum(self, start_range, end_range, aggregate_column_index):
        """
        Computes sum of values in a column over a range of primary keys.
        """
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)

        if not rids:
            return False  # No matching records

        total_sum = sum(self.table.page_directory[rid].columns[aggregate_column_index] for rid in rids)

        return total_sum
