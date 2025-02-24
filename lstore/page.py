import pickle
import os

PAGE_SIZE = 4096  # 4KB per page

class Page:
    def __init__(self):
        self.num_records = 0
        self.data = {}  # Stores records
        self.dirty = False  # Marks if page needs to be written to disk

    def has_capacity(self):
        """ Returns remaining bytes available in the page. """
        return PAGE_SIZE - len(pickle.dumps(self.data))

    def write(self, rid, value):
        """
        Writes a record to the page.
        :param rid: Record ID
        :param value: Data to be stored
        :return: Success or failure code
        """
        size_data = self.has_capacity()
        size_value = len(pickle.dumps(value))

        if size_data - size_value < 0:
            return -1  # Error code for insufficient space

        self.data[rid] = value
        self.num_records += 1
        self.dirty = True  # Mark page as modified
        return 0  # Success

    def read(self, rid):
        """
        Reads a record from the page.
        :param rid: Record ID
        :return: Record value or None
        """
        return self.data.get(rid, None)

    def save_to_disk(self, filepath):
        """
        Saves the page to disk.
        :param filepath: Path to the storage file
        """
        with open(filepath, "wb") as f:
            pickle.dump(self, f)
        self.dirty = False  # Page is now clean
        print(f"Page saved to {filepath}")

    @staticmethod
    def load_from_disk(filepath):
        """
        Loads a page from disk.
        :param filepath: Path to the storage file
        :return: Loaded Page object
        """
        if os.path.exists(filepath):
            with open(filepath, "rb") as f:
                return pickle.load(f)
        return Page()  # Return new empty page if not found
