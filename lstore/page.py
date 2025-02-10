import pickle

class Page:
    def __init__(self, num_columns):
        self.num_records = 0
        self.num_columns = num_columns  # Store number of columns
        self.data = {}  # Dictionary storing records with RID as key

    def has_capacity(self):
        """ Returns size of dictionary as bytes. """
        return len(pickle.dumps(self.data))

    def is_full(self):
        """ Checks if the page is full (4096 bytes limit). """
        return self.has_capacity() >= 4096

    def write(self, record):
        """
        Writes a record into the page.
        Uses record.rid as the key in the dictionary.
        """
        if self.is_full():
            return "Error: Page Full"
        self.data[record.rid] = record  # Store record by RID
        self.num_records += 1

    def read(self, rid):
        """
        Reads a record from the page by its RID.
        Ensures records from full pages are correctly retrieved.
        """
        if rid in self.data:
            return self.data[rid]  # ✅ Return record if found
        return None  # ✅ No unnecessary print statements

    def invalidate(self, rid):
        """
        Marks a record as deleted by removing it from the page.
        """
        if rid in self.data:
            del self.data[rid]  # Remove the record from storage
            self.num_records -= 1  # Update count
            return True  # Indicate success
        return False  # Indicate record not found
