import os
import json
import struct

PAGE_SIZE = 4096  # 4KB per page
PAGE_HEADER_SIZE = 128  # Reserve space for metadata if needed

class Page:
    def __init__(self):
        self.num_records = 0
        self.data = {}  # Stores records {RID: value}
        self.dirty = False  # Marks if the page needs to be written to disk

    def has_capacity(self):
        """ Returns remaining bytes available in the page. """
        current_size = sum(len(self.serialize_record(v)) for v in self.data.values())
        return PAGE_SIZE - current_size - PAGE_HEADER_SIZE

    def write(self, rid, value):
        """
        Writes a record to the page.
        :param rid: Record ID
        :param value: Data to be stored (tuple or list)
        :return: Success (0) or Failure (-1)
        """
        size_value = len(self.serialize_record(value))

        if self.has_capacity() < size_value:
            return -1  # Error: Insufficient space

        self.data[rid] = value
        self.num_records += 1
        self.dirty = True  # Mark as modified
        return 0  # Success

    def read(self, rid):
        """ Reads a record from the page. """
        return self.data.get(rid, None)

    def save_to_disk(self, filepath):
        """
        Saves the page to disk using binary storage.
        """
        with open(filepath, "wb") as f:
            # Save number of records (metadata)
            f.write(struct.pack("I", self.num_records))

            # Save records in binary format
            for rid, value in self.data.items():
                f.write(struct.pack("I", rid))  # Store RID
                f.write(self.serialize_record(value))  # Store actual record

        self.dirty = False  # Page is now clean
        print(f" Page saved to {filepath}")

    @staticmethod
    def load_from_disk(filepath):
        """
        Loads a page from disk.
        """
        if not os.path.exists(filepath):
            return Page()  # Return new empty page if not found

        page = Page()
        with open(filepath, "rb") as f:
            # Read number of records
            page.num_records = struct.unpack("I", f.read(4))[0]

            while True:
                rid_bytes = f.read(4)
                if not rid_bytes:
                    break

                rid = struct.unpack("I", rid_bytes)[0]
                value = Page.deserialize_record(f)
                page.data[rid] = value

        return page

    def serialize_record(self, record):
        """
        Serializes a record into binary format.
        """
        serialized = struct.pack(f"{len(record)}I", *record)
        return serialized

    @staticmethod
    def deserialize_record(file):
        """
        Reads and deserializes a record from binary format.
        """
        value_size = 4 * 5  # Assuming 5 columns of integers (adjustable)
        value_bytes = file.read(value_size)
        if not value_bytes:
            return None
        return struct.unpack("5I", value_bytes)  # Adjust based on column count
