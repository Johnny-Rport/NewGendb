from lstore.index import Index
from lstore.page import Page
from lstore.page_directory import PageDirectory
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
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = PageDirectory()
        self.index = Index(self)
        
        self.base_pages = self._initialize_base_pages(num_columns)
        self.rid_counter = 0
        self.page_capacity = 512

    def insert(self, *columns):
        """
        Inserts a new record into the table.
        """
        #print(f"Inserting record: {columns}")  # Debugging output
        self.rid_counter += 1
        rid = self.rid_counter
        primary_key = columns[self.key]
        
        page_index, row_index = self._get_available_page()

        # Store each column value in its respective page
        self._store_columns_in_pages(columns, page_index)

        # Add to page directory
        self.page_directory.add_base_record(rid, self.key, page_index, row_index)
        #print("inserting rid", rid)
        print(f"Base record added for RID {rid}: Page {page_index}, Row {row_index}")  # Debugging output

        # Update primary key index if applicable
        #print(f"Inserting RID {rid} with primary key {primary_key}")  # Debugging output
        self._update_index(rid, primary_key)

        #print(f"Inserted RID {rid}: {columns}")  # Debugging output
        #print(f"Verifying insertion: {self.index.indices[self.key]}")  
        return True

    """
    def select(self, search_key, search_key_index, projected_columns_index):
        print(f"Searching for key {search_key} in column {search_key_index}")  # Debugging output
        records = []

        # Try to use the index for a fast lookup
        if self.index.indices[search_key_index] is not None:
            rid_list = self.index.locate(search_key_index, search_key)
            print(f"Located RIDs for search_key {search_key}: {rid_list}")  # Debugging output
        else:
            # If no index, scan the page directory
            rid_list = [rid for rid in self.page_directory.base_pages if self._get_primary_key(rid) == search_key]

        #print(f"Located RIDs: {rid_list}")  # Debugging output

        if not rid_list:
            print(f"Select failed: No records found for search_key {search_key}")  # Debugging output
            return []  # Return empty list instead of False

        # Retrieve records from page directory
        for rid in rid_list:
            page_info = self.page_directory.get_base_record(rid)
            if not page_info:
                print(f"Select warning: RID {rid} has no associated base record")  # Debugging output
                continue  # Skip if record does not exist

            column_id, page_index, row_index = page_info
            result = []
            for col in range(self.num_columns):
                if projected_columns_index[col] == 1:
                    result.append(self.base_pages[col][page_index].read(row_index))
            record = Record(rid, self._get_primary_key(rid), result)
            records.append(record)

        print(f"Records found: {records}")  # Debugging output
        return records  # Return a list of records
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        print(f"Selecting record with search_key: {search_key} on column {search_key_index}")  # Debugging output

        records = []

        # Use the index for lookup
        rid_list = self.index.locate(search_key_index, search_key)
        print(f"Located RIDs for search_key {search_key}: {rid_list}")  # Debugging output

        if not rid_list:
            print(f"Select failed: No records found for search_key {search_key}")  # Debugging output
            return []  # Ensure it returns an empty list instead of False

        # Retrieve records using page directory
        for rid in rid_list:
            page_info = self.page_directory.get_base_record(rid)
            if not page_info:
                print(f"Select warning: RID {rid} has no associated base record")  # Debugging output
                continue  # Skip if record does not exist

            key, page_index, row_index = page_info  # Ensure correct unpacking
            print(f"Retrieving record at Page {page_index}, Row {row_index}")  # Debugging output

            result = []
            for col in range(self.num_columns):
                if projected_columns_index[col] == 1:
                    result.append(self.base_pages[col][page_index].read(row_index))

            record = Record(rid, key, result)
            records.append(record)

        print(f"Select returning records: {records}")  # Debugging output
        return records

    def update(self, primary_key, *columns):
        """
        Updates an existing record by adding a tail record.
        """
        rid = self._get_rid(self.key, primary_key)
        if rid is None:
            print(f"Update failed: Primary key {primary_key} not found.")
            return False  # Record does not exist

        # Retrieve original record location
        page_info = self.page_directory.get_base_record(rid)
        if page_info is None:
            print(f"Update failed: RID {rid} not found in page directory.")
            return False

        column_id, page_index, row_index = page_info

        # Read the original record
        original_values = []
        for col in range(self.num_columns):
            original_values.append(self.base_pages[col][page_index].read(row_index))

        print(f"Original record: {original_values}")

        # Apply updates (skip None values)
        updated_values = [
            columns[i] if columns[i] is not None else original_values[i]
            for i in range(self.num_columns)
        ]
        
        print(f"Updated values: {updated_values}")

        # Get tail page location
        tail_page_index, tail_row_index = self._get_available_tail_page()

        # Write updated values to the tail page
        self._store_columns_in_pages(columns, tail_page_index)

        # Add tail record to page directory
        self.page_directory.add_tail_record(rid, column_id, tail_page_index, tail_row_index)

        print(f"Update successful: RID {rid} updated at tail page {tail_page_index}, row {tail_row_index}")
        return True

    def delete(self, primary_key):
        """
        Deletes a record by removing it from the page directory.
        """
        rid = self._get_rid(self.key, primary_key)
        if rid is None:
            return False

        self.page_directory.delete_record(rid)
        return True

    def sum(self, start_range, end_range, aggregate_column_index):
        """
        Computes the sum of a column over a range of primary keys.
        """
        total = 0
        found = False

        for rid in self.page_directory.base_pages:
            record = self.page_directory.get_base_record(rid)
            if record is None:
                continue

            column_id, page_index, row_index = record

            primary_key = self._get_primary_key(rid)
            
            if start_range <= primary_key <= end_range:
            # Sum values from base record
                total += self.base_pages[aggregate_column_index][page_index].read(row_index)
                found = True

            # Check for tail records and sum them too
                tail_info = self.page_directory.get_tail_record(rid)
                while tail_info:
                    _, tail_page_index, tail_row_index = tail_info
                    total += self.base_pages[aggregate_column_index][tail_page_index].read(tail_row_index)
                    tail_info = self.page_directory.get_tail_record(rid)  # Get next tail version

                if start_range <= self._get_primary_key(rid) <= end_range:
                    total += self.base_pages[aggregate_column_index][page_index].read(row_index)
                    found = True

        if found:
            return total
        return False

    # **Helper Methods**
    
    def _initialize_base_pages(self, num_columns):
        """
        Initializes the base pages structure.
        """
        base_pages = []
        for _ in range(num_columns):
            base_pages.append([])
        return base_pages

    def _create_new_pages(self):
        """
        Creates new pages for each column.
        """
        #print("Creating new set of pages!")  # Debugging output
        for i in range(self.num_columns):
            self.base_pages[i].append(Page())

    def _get_available_page(self):
        """
        Determines the next available page and row index for insertion.
        """
        if not self.base_pages[0]:
            #print("No pages found, creating new pages...")
            self._create_new_pages()
            return 0, 0

        last_page_index = len(self.base_pages[0]) - 1
        last_page = self.base_pages[0][last_page_index]

        #print(f"Last page num_records: {last_page.num_records}")  # Debugging output

        if last_page.num_records >= self.page_capacity:
            #print("Last page is full, creating new pages...")
            self._create_new_pages()
            return last_page_index + 1, 0

        #print(f"Using existing page {last_page_index}, row {last_page.num_records}")  # Debugging output
        return last_page_index, last_page.num_records

    def _store_columns_in_pages(self, columns, page_index):
        """
        Writes column values to the correct pages.
        """
        #print(f"Storing columns in page {page_index}: {columns}")  # Debugging output
        for col_index in range(len(columns)):
            success = self.base_pages[col_index][page_index].write(columns[col_index])
            if not success:
                print(f"Failed to write column {col_index} to page {page_index}")  # Debugging output
        #print(f"Page {page_index} now contains {self.base_pages[0][page_index].num_records} records")  # Debugging output

    def _update_index(self, rid, primary_key_value):
        """
        Updates the index if applicable.
        """

        """
        print(f"Updating index: {primary_key_value} -> {rid}")  # Debugging output
        if self.index.indices[self.key] is not None:
            self.index.indices[self.key][primary_key_value] = rid
        """
        if self.index.indices[self.key] is None:
            self.index.indices[self.key] = {}

        self.index.indices[self.key][primary_key_value] = rid
        #print(f"Index updated: {primary_key_value} -> {rid}")  # Debugging output

    def _get_rid(self, search_key_index, search_key):
        """
        Retrieves the RID using the index.
        """
        print(f"Searching for RID with primary key: {search_key}")  # Debugging output
        if self.index.indices[search_key_index] is not None:
            rid_list = self.index.locate(search_key_index, search_key)
            print(f"Located RID(s): {rid_list}")  # Debugging output
            if rid_list:
                return rid_list[0]
        print(f"RID not found for primary key {search_key}")  # Debugging output
        return None

    def _fetch_columns_from_pages(self, projected_columns_index, page_index, row_index):
        """
        Fetches the projected columns from the pages.
        """
        result = []
        for col in range(self.num_columns):
            if projected_columns_index[col] == 1:
                result.append(self.base_pages[col][page_index].read(row_index))
        return result

    def _get_primary_key(self, rid):
        """
        Retrieves the primary key value from a given RID.
        """
        page_info = self.page_directory.get_base_record(rid)
        if page_info is None:
            return None
        column_id, page_index, row_index = page_info
        return self.base_pages[self.key][page_index].read(row_index)

    def _get_available_tail_page(self):
        """
        Determines the next available location in the tail pages.
        """
        if not self.base_pages[0]:
            self._create_new_pages()
            return 0, 0

        last_page_index = len(self.base_pages[0]) - 1
        last_page = self.base_pages[0][last_page_index]

        if last_page.num_records >= self.page_capacity:
            self._create_new_pages()
            return last_page_index + 1, 0

        return last_page_index, last_page.num_records
    
    def __merge(self):
        print("merge is happening (for milestone 2)")
        pass
 
