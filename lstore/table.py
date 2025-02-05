from lstore.index import Index
from time import time
from lstore.page_directory import Page_directory

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
        self.page_directory = Page_directory(num_columns)
        self.index = Index(self)
        self.next_rid = 0
        pass

    def __merge(self): # Don't need for the milestone 1
        print("merge is happening")
        pass
 
    # Todo. I think we need insert record to support query.py
    def insert_record (self, data_list):
        rid = self.next_rid
        personKey_Value = data_list[self.key]

        rec = Record(rid, personKey_Value, data_list)
        self.next_rid += 1

        # ensure we have an empty map for this rid
        self.page_directory.record_map[rid] = {}

        # Write each column
        for col_idx, col_val in enumerate(data_list):
            while True:
                page, page_id = self.page_directory.get_tail(col_idx)
                try:
                    page.write(rid, col_val)
                    # if success, store it in record_map
                    self.page_directory.record_map[rid][col_idx] = page_id
                    break  # success => break the while
                except MemoryError:
                    # expand pages for all columns, then try again
                    self.page_directory.expand()

        # Update the primary key index
        self.index.pk_index[personKey_Value] = rid

    # Todo, need delete record to support query.py
    def delete_record(self , rid ):
        """
        Soft delete: set each column's rid => None 
        Remove pk from index as well
        """
        if rid not in self.page_directory.record_map:
            return False

        # Identify the personKey_Value so we can remove from index
        record_data = self.read_record(rid)
        if record_data is None:
            # Already deleted
            return False

        pk_val = record_data[self.key]
        # set each col => None
        col_map = self.page_directory.record_map[rid]
        for col_idx, page_id in col_map.items():
            page = self.page_directory.page_list[page_id]
            page.delete(rid)

        # remove from pk_index
        if pk_val in self.index.pk_index:
            del self.index.pk_index[pk_val]

        return True


    def sum_range(self, start_key, end_key, aggregate_column_index):
        """
        Return the sum of the 'aggregate_column_index' for all
        primary keys in [start_key..end_key], ignoring deleted rows.
        If none found, return 0 or None as you prefer.
        """
        rids = self.index.locate_range(start_key, end_key, self.key)
        if not rids:
            return 0  # or return None

        total = 0
        for rid in rids:
            row_data = self.read_record(rid)
            if row_data is not None:
                val = row_data[aggregate_column_index]
                total += val
        return total
    
    def update_record(self, rid, new_values):
        """
        Overwrite each column if new_values[i] is not None.
        new_values is length = num_columns.
        """
        if rid not in self.page_directory.record_map:
            return False

        for col_idx, val in enumerate(new_values):
            if val is not None:
                page_id = self.page_directory.record_map[rid].get(col_idx, None)
                if page_id is None:
                    return False  # no page for this col?
                page = self.page_directory.page_list[page_id]

                # Overwrite in place. We might not handle page overflow on update 
                # since we expect it to fit if we are only overwriting the same rid's data.
                page.write(rid, val)
        return True


    def read_record(self, rid):
        """
        Return the list of column values for the given rid, 
        or None if not found or the record was deleted.
        """
        if rid not in self.page_directory.record_map:
            return None  # not found at all

        col_map = self.page_directory.record_map[rid]  # dict: col_idx -> page_id
        row_data = []
        for col_idx in range(self.num_columns):
            page_id = col_map.get(col_idx, None)
            if page_id is None:
                return None  # missing data => something's off
            page = self.page_directory.page_list[page_id]
            val = page.read(rid)
            if val is None:
                # means it's been soft-deleted in this column
                return None
            row_data.append(val)

        return row_data