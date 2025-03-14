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
            print(f"❌ INSERT ERROR: Expected {self.table.num_columns} columns, got {len(columns)}")
            return False  # Invalid column count

        rid = len(self.table.page_directory) + 1  # Generate new RID
        record = Record(rid, columns[self.table.key], list(columns))

        # Store in page directory
        self.table.page_directory[rid] = record
        self.table.base_pages.append(record)  # Append to base page

        # Update primary key index
        self.table.index.update_index(self.table.key, None, columns[self.table.key], rid)

        print(f"✅ INSERT SUCCESS: Inserted record {record.columns} with RID {rid}")
        return True

    def select(self, search_key, search_key_index, projected_columns_index):
        """
        Selects records matching the given search key.
        """
        print(f"🔎 SELECT DEBUG: Searching for key {search_key} in column {search_key_index}")

        rids = self.table.index.locate(search_key_index, search_key)

        if not rids:
            print(f"⚠️ SELECT ERROR: No matching records found for key {search_key} in column {search_key_index}")
            print(f"📌 DEBUG INDEX STATE: {self.table.index.indices}")
            return []  # Return an empty list instead of `False`

        results = []
        for rid in rids:
            record = self.table.page_directory.get(rid)
            if record:
                projected_values = [record.columns[i] if projected_columns_index[i] else None for i in range(self.table.num_columns)]
                results.append(Record(record.rid, record.key, projected_values))
            else:
                print(f"⚠️ SELECT ERROR: RID {rid} not found in page_directory")

        print(f"✅ SELECT SUCCESS: Found {len(results)} records for key {search_key}")
        return results

    def update(self, primary_key, *columns):
        """
        Updates a record with the specified primary key.
        """
        print(f"🛠️ UPDATE DEBUG: Updating record with primary key {primary_key}")

        rids = self.table.index.locate(self.table.key, primary_key)

        if not rids:
            print(f"❌ UPDATE ERROR: No record found with key {primary_key}")
            return False  # No matching record

        rid = rids[0]
        record = self.table.page_directory.get(rid)

        if not record:
            print(f"❌ UPDATE ERROR: Record with RID {rid} not found in page_directory")
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

        print(f"✅ UPDATE SUCCESS: Updated record {updated_values} with RID {new_rid}")
        return True

    def delete(self, primary_key):
        """
        Deletes a record with the specified primary key.
        """
        print(f"🗑️ DELETE DEBUG: Deleting record with primary key {primary_key}")

        rids = self.table.index.locate(self.table.key, primary_key)

        if not rids:
            print(f"❌ DELETE ERROR: No record found with key {primary_key}")
            return False  # No record found

        rid = rids[0]

        # Remove from table storage
        if rid in self.table.page_directory:
            del self.table.page_directory[rid]

        # Remove from index
        self.table.index.update_index(self.table.key, primary_key, None, rid)

        print(f"✅ DELETE SUCCESS: Removed record with RID {rid}")
        return True

    def sum(self, start_range, end_range, aggregate_column_index):
        """
        Computes sum of values in a column over a range of primary keys.
        """
        print(f"🧮 SUM DEBUG: Summing values in column {aggregate_column_index} from key {start_range} to {end_range}")

        rids = self.table.index.locate_range(start_range, end_range, self.table.key)

        if not rids:
            print(f"❌ SUM ERROR: No records found in range {start_range} to {end_range}")
            return False  # No matching records

        total_sum = sum(self.table.page_directory[rid].columns[aggregate_column_index] for rid in rids)

        print(f"✅ SUM SUCCESS: Sum = {total_sum}")
        return total_sum
