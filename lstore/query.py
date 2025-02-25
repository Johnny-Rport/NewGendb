#  * Program Name: Database System -> L_Store Concepts
#  * Author: Next Generation
#  * Date: Feb 11/2025 (Final_Version_v04)
#  * Description: 

from lstore.table import Table, Record
from time import time

class Query:
    """
    Creates a Query object that can perform different queries on the specified table.
    Queries that fail must return False.
    Queries that succeed should return the result or True.
    Any query that crashes (due to exceptions) should return False.
    """
    def __init__(self, table):
        self.table = table

    #  * Function Name: Delete
    #  * Purpose: Allow to remove a record from the table 
    #  * Parameter: primary_key
    #  * Return: Bool

    def delete(self, primary_key):
        if primary_key not in self.table.key_directory: # Trying to find the base record using the key_directory.
            # print(f" Primary key {primary_key} is the not found.") # Debugging 
            return False
        rid = self.table.key_directory[primary_key]
        record = self.table.page_directory.get(rid)
        if record is None or record.get('deleted', False):
            return False
        record['deleted'] = True  # Mark the record as deleted.
        # print(f"Delete: Primary Key: {primary_key}, RID: {rid}") # Debugging 
        return True
    
    #  * Function Name: insert
    #  * Purpose: Trying to inserts a new record into the table 
    #  * Parameter: requirt (*columns), a tulple which is representing the values about the insert
    #  * Return: Bool, true -> insertion successful. Otherwise, false

    def insert(self, *columns):
        primary_key = columns[self.table.key] # Trying to check for the duplicate primary key.
        if primary_key in self.table.key_directory:
            #  print(f"Duplicate key: {primary_key} detected.") # Debugging
            return False  # Return false, if not found it

        rid = self.table.next_rid # Else create metadata for the base record.
        self.table.next_rid += 1
        record = {
            'rid': rid,
            'key': primary_key,
            'indirection': None,  # At initially, there is no tail record.
            'timestamp': time(),
            'schema_encoding': '0' * self.table.num_columns, 
            'data': list(columns),
            'deleted': False
        }
        # Store the new record.
        self.table.page_directory[rid] = record
        self.table.key_directory[primary_key] = rid

        # Update the primary key index.
        if self.table.index.indices[self.table.key] is not None:
            self.table.index.indices[self.table.key][primary_key] = rid
            # print(f"Successful inserted, Primary Key: {primary_key}, RID: {rid}") # debugging 
        return True
    
    #  * Function Name: get_last_version_value
    #  * Purpose: Trying to retrieves the recently values by the specified column into the record, based on the tail record
    #  * Parameters:
    #  *     @ record: The base record from which to retrieve the latest column value.
    #  *     @ column: The index of the column whose latest value is needed.
    #  * Return:
    #  *     @ Any: The most recent value of the specified column, retrieved from 
    #  *            either the base record or the latest tail record.

    def get_last_version_value(self, record, column):
        # Follow the tail record chain (if any) to find the most recent update for column.
        current_rid = record['indirection']
        while current_rid is not None:
            tail_record = self.table.page_directory.get(current_rid)
            # print(f"tail record: {tail_record}") # debugging 
            if tail_record is None:
                break
            if tail_record['data'][column] is not None:
                return tail_record['data'][column]
            current_rid = tail_record['indirection']
            # print(f"current rid record: {current_rid}") # debugging 
        return record['data'][column]

    #  * Function Name: select
    #  * Purpose: Retrieves a record based on a given key and projects selected columns.
    #  * Parameters:
    #  *     @ search_key: The value to search for.
    #  *     @ search_key_index: The index of the column used for searching.
    #  *     @ projected_columns_index: A list indicating which columns to return.
    #  * Return:
    #  *     @ list[Record]: A list of selected records if found.
    #  *     @ bool: Will returns false if no matching record exists.
    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            if search_key_index == self.table.key:  # For primary key search.
                if search_key not in self.table.key_directory:
                    return False
                rid = self.table.key_directory[search_key]
                # print(f"Select rid: {rid}") # debugging 
                base_record = self.table.page_directory.get(rid)
                # print(f"Select base record : {base_record}")  # debugging 
                if base_record is None or base_record.get('deleted', False):
                    return False
                result_values = []
                for i in range(self.table.num_columns):
                    if projected_columns_index[i]:
                        val = self.get_last_version_value(base_record, i)
                        # print(f"val: {val}")              # debugging 
                        result_values.append(val)
                    else:
                        result_values.append(None)
                # Return a Record object (as defined in table.py) wrapped in a list.
                return [Record(base_record['rid'], base_record['key'], result_values)]
            else:
                # For non-primary key search, perform a linear scan.
                for rid, record in self.table.page_directory.items():
                    if (not record.get('deleted', False) and 
                        record['data'][search_key_index] == search_key):
                        result_values = []
                        for i in range(self.table.num_columns):
                            if projected_columns_index[i]:
                                val = self.get_last_version_value(record, i)
                                result_values.append(val)
                            else:
                                result_values.append(None)
                        return [Record(record['rid'], record['key'], result_values)]
                return False
        except Exception:
            return False

    #  * Function Name: select_version
    #  * Purpose: Trying to retrievees a historical version, based on the relative updates
    #  * Parameters: @1th_para: search_key, the value to search for. 
    #  *             @2th_para: search_key_index (int):  index of the column used for searching.
    #  *             @3th_para projected_columns_index : A list indicating which columns to return.
    #  *             @4th_para relative_version : The version offset (0 = latest, 1 = previous, etc.).
    #  * Return List:
    #  *             if successful:
    #  *             return list[Record]: The list of selected records from the specified version.
    #  *             bool: Returns False if no matching record exists.

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
            if search_key_index == self.table.key:
                if search_key not in self.table.key_directory:
                    return False
                rid = self.table.key_directory[search_key]
                base_record = self.table.page_directory.get(rid)
                if base_record is None or base_record.get('deleted', False):
                    return False
                tail_chain = []  # Build the tail chain (newest first)
                current_rid = base_record['indirection']
                while current_rid is not None:
                    tail_record = self.table.page_directory.get(current_rid)
                    if tail_record is None:
                        break
                    tail_chain.append(tail_record)
                    current_rid = tail_record['indirection']
                # If relative_version is negative, we want to return the base record (i.e. ignore tail records)
                if relative_version < 0:
                    effective_chain = []
                else:
                    effective_chain = tail_chain[relative_version:] if relative_version < len(tail_chain) else []

                version_data = list(base_record['data'])
                for tail_record in reversed(effective_chain):
                    for i in range(self.table.num_columns):
                        if tail_record['data'][i] is not None:
                            version_data[i] = tail_record['data'][i]
                result_values = []
                for i in range(self.table.num_columns):
                    if projected_columns_index[i]:
                        result_values.append(version_data[i])
                    else:
                        result_values.append(None)
                return [Record(base_record['rid'], base_record['key'], result_values)]
            else:
                # Non-primary key search (similar changes apply)
                for rid, record in self.table.page_directory.items():
                    if (not record.get('deleted', False) and 
                        record['data'][search_key_index] == search_key):
                        tail_chain = []
                        current_rid = record['indirection']
                        while current_rid is not None:
                            tail_record = self.table.page_directory.get(current_rid)
                            if tail_record is None:
                                break
                            tail_chain.append(tail_record)
                            current_rid = tail_record['indirection']
                            # print(f"Select current rid: {current_rid}")  # dugging
                        if relative_version < 0:
                            effective_chain = []
                        else:
                            effective_chain = tail_chain[relative_version:] if relative_version < len(tail_chain) else []
                        version_data = list(record['data'])
                        for tail_record in reversed(effective_chain):
                            for i in range(self.table.num_columns):
                                if tail_record['data'][i] is not None:
                                    version_data[i] = tail_record['data'][i]
                        result_values = []
                        for i in range(self.table.num_columns):
                            if projected_columns_index[i]:
                                result_values.append(version_data[i])
                            else:
                                result_values.append(None)
                        return [Record(record['rid'], record['key'], result_values)]
                return False
        except Exception:
            return False
        
    #  * Function Name: update
    #  * Purpose: Updates the existing record by the creating a new tail record with modifications.
    #  * Parameters:
    #  *         @ rimary_key (int/str): The unique identifier of the record to update.
    #  *         @ columns: A variable-length tuple of updated values (None means no change).
    #  * Return:
    #  *       @bool: Returns True if the update is successful,
    #  *             False if the record does not exist or is deleted.

    def update(self, primary_key, *columns):
        if primary_key not in self.table.key_directory: # Check does the primary_key is into the key_directory
            return False
        base_rid = self.table.key_directory[primary_key]
        base_record = self.table.page_directory.get(base_rid)
        if base_record is None or base_record.get('deleted', False):
            return False

        new_rid = self.table.next_rid  # Create a new tail record.
        self.table.next_rid += 1
        # Trying to build a simple schema encoding (a string with 1 for updated, 0 for not).
        schema_encoding = ''.join(['1' if col is not None else '0' for col in columns])
        tail_record = {
            'rid': new_rid,
            'key': primary_key,
            'indirection': base_record['indirection'],  # Points to the previous tail record.
            'timestamp': time(),
            'schema_encoding': schema_encoding,
            'data': list(columns),  # Only non-None values are updates.
            'deleted': False
        } 
        self.table.page_directory[new_rid] = tail_record          # Store the tail record.
        base_record['indirection'] = new_rid  # Update the base record’s indirection to point to this new tail record.
        return True

    #  * Function Name: sum
    #  * Purpose: Computes the sum of values within a given range in a specified column.
    #  * Parameters:
    #  *     @ start_range : The starting key of the range.
    #  *     @ end_range: The ending key of the range.
    #  *     @ aggregate_column_index: The index of the column to sum.
    #  * Return:
    #  *     @ int or float: The computed sum if records are found.
    #  *     @ bool: Returns false, if there is no valid records exist in the range.

    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            total = 0
            found = False
            for key, rid in self.table.key_directory.items():
                if start_range <= key <= end_range:
                    record = self.table.page_directory.get(rid)
                    # print(f"sum of the record: {record}")      # debugging
                    if record is None or record.get('deleted', False):
                        continue
                    val = self.get_last_version_value(record, aggregate_column_index)
                    if val is not None:
                        total += val
                        found = True
            return total if found else False
        except Exception:
            return False
        

    #  * Function Name: sum_version
    #  * Purpose: Computes the sum of a column across a historical version of records.
    #  * Parameters:
    #  *     @ start_range : The starting key of the range.
    #  *     @ end_range : The ending key of the range.
    #  *     @ aggregate_column_index: The index of the column to sum.
    #  *     @ relative_version : The version offset, like 0 = latest, 1 = previous, and etc. 
    #  * Return:
    #  *     @ int/float: The computed sum if records are found.
    #  *     @ bool: Returns False if no valid records exist in the range.
    
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            total = 0
            found = False
            for key, rid in self.table.key_directory.items():
                if start_range <= key <= end_range:
                    record = self.table.page_directory.get(rid)
                    if record is None or record.get('deleted', False):
                        continue
                    tail_chain = []
                    current_rid = record['indirection']
                    while current_rid is not None:
                        tail_record = self.table.page_directory.get(current_rid)
                        # print(f"Tail Record: {tail_record}") # debugging
                        if tail_record is None:
                            break
                        tail_chain.append(tail_record)
                        current_rid = tail_record['indirection']
                    if relative_version < 0:
                        effective_chain = []
                    else:
                        effective_chain = tail_chain[relative_version:] if relative_version < len(tail_chain) else []
                    version_data = list(record['data'])
                    for tail_record in reversed(effective_chain):
                        for i in range(self.table.num_columns):
                            if tail_record['data'][i] is not None:
                                version_data[i] = tail_record['data'][i]
                    val = version_data[aggregate_column_index]
                    if val is not None:
                        total += val
                        found = True
            return total if found else False
        except Exception:
            return False
        
    #  * Function Name: increment
    #  * Purpose: Increments a specific column of a record by 1.
    #  * Parameters:
    #  *     @ key: The primary key of the record.
    #  *     @ column: The index of the column to increment.
    #  * Return:
    #  *     @ bool: Returns True if the increment is successful,
    #  *             False if the record does not exist.
    def increment(self, key, column):
        # This helper first selects the record then updates the specified column by +1.
        r = self.select(key, self.table.key, [1] * self.table.num_columns)
        # print(f"Increament of record: {r}")          # debugging
        if r is not False:
            base_rid = self.table.key_directory.get(key)
            # print(f"Base RID: {base_rid}")          # debugging
            if base_rid is None:
                return False
            base_record = self.table.page_directory.get(base_rid)
            if base_record is None:
                return False
            current_val = self.get_last_version_value(base_record, column)
            if current_val is None:
                return False
            updated_columns = [None] * self.table.num_columns
            # print(f"Update Columns Check: {updated_columns}")          # debugging
            updated_columns[column] = current_val + 1
            # print(f"Update the columns: {column}")          # debugging
            return self.update(key, *updated_columns)
        return False
