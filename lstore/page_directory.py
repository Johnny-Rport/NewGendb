class PageDirectory:
    def __init__(self):
        """
        Initializes an empty page directory.
        - `base_pages`: Maps RIDs to (column, page_index, row_index)
        - `tail_pages`: Maps RIDs to (column, page_index, row_index) for updates
        """
        self.base_pages = {}  # {RID: (column_id, page_index, row_index)}
        self.tail_pages = {}  # {RID: (column_id, page_index, row_index)}

    def add_base_record(self, rid, column_id, page_index, row_index):
        """
        Adds a new base record to the page directory.
        """
        self.base_pages[rid] = (column_id, page_index, row_index)

    def add_tail_record(self, rid, column_id, page_index, row_index):
        """
        Adds a new tail record (for updates) to the page directory.
        """
        self.tail_pages[rid] = (column_id, page_index, row_index)

    def get_base_record(self, rid):
        print(f"Retrieving base record for RID {rid}")  # Debugging output
        if rid in self.base_pages:
            print(f"Found base record for RID {rid}: {self.base_pages[rid]}")  # Debugging output
            return self.base_pages[rid]
    
        print(f"Base record not found for RID {rid}")  # Debugging output
        return None

    def get_tail_record(self, rid):
        """
        Retrieves the tail record location for a given RID.
        """
        return self.tail_pages.get(rid, None)

    def delete_record(self, rid):
        """
        Deletes a record from the directory (both base and tail pages).
        """
        if rid in self.base_pages:
            del self.base_pages[rid]
        if rid in self.tail_pages:
            del self.tail_pages[rid]