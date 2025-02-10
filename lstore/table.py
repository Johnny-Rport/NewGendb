from lstore.index import Index
from lstore.page import Page
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
    :param name: string         # Table name
    :param num_columns: int     # Number of columns (all integers)
    :param key: int             # Index of the primary key column
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}  # Maps RIDs to pages
        self.index = Index(self)
        self.pages = []  # Stores all allocated pages

        # Metadata storage for future milestones
        self.metadata = {
            "record_count": 0,  # Total number of records
            "full_pages": 0,  # Number of fully occupied pages
            "page_ranges": {}  # Page range tracking for optimization
        }

    def insert(self, key, columns):
        rid = self.metadata["record_count"] + 1
        record = Record(rid, key, columns)

        inserted = False
        for page in self.pages:
            if not page.is_full():
                page.write(record)
                self.page_directory[rid] = page  # ✅ Ensure all records are mapped
                inserted = True
                break

        if not inserted:
            new_page = Page(self.num_columns)
            self.pages.append(new_page)
            self.metadata["full_pages"] += 1
            new_page.write(record)
            self.page_directory[rid] = new_page

        self.metadata["record_count"] += 1  # ✅ Update metadata

    def select(self, search_key, search_column):
        """
        Select records from the table based on the search key.
        Ensures that it searches across all pages, not just the latest empty one.
        """
        results = []
        for rid, page in self.page_directory.items():
            # Get the record from the page
            record = page.read(rid)

            # Ensure we are reading from all pages, not just the latest empty one
            if record and record.columns[search_column] == search_key:
                results.append(record)

        return results

    def update(self, search_key, update_columns):
        """
        Updates records in the table based on the search key.
        Uses tail pages to track updates.
        """
        for page in self.pages:  # ✅ Scan all pages for the record
            for rid, record in page.data.items():
                if record.key == search_key:
                    new_record = Record(rid, search_key, update_columns)
                    page.write(new_record)
                    self.page_directory[rid] = page  # Update reference

    def delete(self, search_key):
        """
        Deletes a record by invalidating it in the metadata.
        Ensures only one deletion per record.
        """
        deleted = False
        for page in self.pages:  # ✅ Ensure we delete from all pages
            for rid, record in list(page.data.items()):  # ✅ Iterate safely
                if record.key == search_key:
                    if not deleted:  # ✅ Ensure we only decrement ONCE
                        self.metadata["record_count"] -= 1  # ✅ Decrease count once
                        deleted = True  # ✅ Mark deletion as done
                    page.invalidate(rid)  # ✅ Mark record as deleted
                    return True  # ✅ Stop after deleting the first match

        return False  # ❌ Return False if no record was found

    def get_metadata(self):
        """
        Returns the metadata dictionary for debugging or indexing.
        """
        return self.metadata  # ✅ Now accessible!

    def sum(self, search_column, min_key, max_key):
        """
        Sums values in a given column for records in a specified key range.
        """
        total = 0
        for page in self.pages:  # ✅ Iterate over all pages
            for rid, record in page.data.items():
                if min_key <= record.key <= max_key:
                    total += record.columns[search_column]

        return total

    def __merge(self):
        """
        Placeholder for future merging logic.
        """
        print("merge is happening")
