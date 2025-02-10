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

    def delete(self, primary_key):
        # Find the base record using the key_directory.
        if primary_key not in self.table.key_directory:
            return False
        rid = self.table.key_directory[primary_key]
        record = self.table.page_directory.get(rid)
        if record is None or record.get('deleted', False):
            return False
        # Mark the record as deleted.
        record['deleted'] = True
        # (Optionally, you might also mark tail records as deleted.)
        return True

    def insert(self, *columns):
        # Check for duplicate primary key.
        primary_key = columns[self.table.key]
        if primary_key in self.table.key_directory:
            return False  # Duplicate primary key not allowed.

        # Create metadata for the base record.
        rid = self.table.next_rid
        self.table.next_rid += 1
        record = {
            'rid': rid,
            'key': primary_key,
            'indirection': None,  # Initially, there is no tail record.
            'timestamp': time(),
            'schema_encoding': '0' * self.table.num_columns,  # No updates yet.
            'data': list(columns),
            'deleted': False
        }
        # Store the new record.
        self.table.page_directory[rid] = record
        self.table.key_directory[primary_key] = rid

        # Update the primary key index (if it exists).
        if self.table.index.indices[self.table.key] is not None:
            self.table.index.indices[self.table.key][primary_key] = rid

        return True

    def __get_latest_value(self, record, column):
        # Follow the tail record chain (if any) to find the most recent update for column.
        current_rid = record['indirection']
        while current_rid is not None:
            tail_record = self.table.page_directory.get(current_rid)
            if tail_record is None:
                break
            if tail_record['data'][column] is not None:
                return tail_record['data'][column]
            current_rid = tail_record['indirection']
        return record['data'][column]

    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            # For primary key search.
            if search_key_index == self.table.key:
                if search_key not in self.table.key_directory:
                    return False
                rid = self.table.key_directory[search_key]
                base_record = self.table.page_directory.get(rid)
                if base_record is None or base_record.get('deleted', False):
                    return False
                result_values = []
                for i in range(self.table.num_columns):
                    if projected_columns_index[i]:
                        val = self.__get_latest_value(base_record, i)
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
                                val = self.__get_latest_value(record, i)
                                result_values.append(val)
                            else:
                                result_values.append(None)
                        return [Record(record['rid'], record['key'], result_values)]
                return False
        except Exception:
            return False

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
            if search_key_index == self.table.key:
                if search_key not in self.table.key_directory:
                    return False
                rid = self.table.key_directory[search_key]
                base_record = self.table.page_directory.get(rid)
                if base_record is None or base_record.get('deleted', False):
                    return False

                # Build the tail chain (newest first)
                tail_chain = []
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


    def update(self, primary_key, *columns):
        # 'columns' is a list of new values; a value of None means “no change” for that column.
        if primary_key not in self.table.key_directory:
            return False
        base_rid = self.table.key_directory[primary_key]
        base_record = self.table.page_directory.get(base_rid)
        if base_record is None or base_record.get('deleted', False):
            return False

        # Create a new tail record.
        new_rid = self.table.next_rid
        self.table.next_rid += 1
        # Build a simple schema encoding (a string with 1 for updated, 0 for not).
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
        # Store the tail record.
        self.table.page_directory[new_rid] = tail_record
        # Update the base record’s indirection to point to this new tail record.
        base_record['indirection'] = new_rid
        return True

    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            total = 0
            found = False
            for key, rid in self.table.key_directory.items():
                if start_range <= key <= end_range:
                    record = self.table.page_directory.get(rid)
                    if record is None or record.get('deleted', False):
                        continue
                    val = self.__get_latest_value(record, aggregate_column_index)
                    if val is not None:
                        total += val
                        found = True
            return total if found else False
        except Exception:
            return False

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


    def increment(self, key, column):
        # This helper first selects the record then updates the specified column by +1.
        r = self.select(key, self.table.key, [1] * self.table.num_columns)
        if r is not False:
            base_rid = self.table.key_directory.get(key)
            if base_rid is None:
                return False
            base_record = self.table.page_directory.get(base_rid)
            if base_record is None:
                return False
            current_val = self.__get_latest_value(base_record, column)
            if current_val is None:
                return False
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = current_val + 1
            return self.update(key, *updated_columns)
        return False
