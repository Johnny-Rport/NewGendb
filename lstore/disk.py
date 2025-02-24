"""
Update:
    New functions
    1, save_page_from_disk(self, table_name, page_id, page_data):
    2, get_pages_from_disk(self, table_name, page_id):
    3, get_pages_from_disk(self, table_name, page_id):
    4, save_table_of_metadata_from_disk(self, table):
    5, get_table_of_metadata_from_disk(self, table_name):
"""
import os
import msgpack
from lstore.disk_utilities import diskUtilities

class DiskManager:
    #     """
    #     Responsible for saving and loading pages to/from disk.
    #     Ensures data persistence when the database is closed.
    #     """
    def __init__(self, db_path):
        self.db_path = db_path
        if not os.path.exists(db_path):
            os.makedirs(db_path)

    #     """
    #     Trying to saving the page into the disk
    #     @params requirment: 
    #       :param table_name: table of the name
    #       :param page_id:  unique page of id
    #       :param page_data: page of the datas
    #     """
    def save_page_from_disk(self, table_name, page_id, page_data):
        # For page_data, if it's a Page instance, call its to_dict() method.
        if hasattr(page_data, "to_dict"):
            data_to_pack = page_data.to_dict()
        else:
            data_to_pack = page_data
        file_path = os.path.join(self.db_path, f"{table_name}_page_{page_id}.mpack")
        with open(file_path, "wb") as file:
            packed = msgpack.packb(data_to_pack, use_bin_type=True)
            file.write(packed)


    #     """
    #     get the pages informations from the disk
    #     @params requirment: 
    #         :param table_name: table of the name
    #         :param page_id:  unique page of id
    #         :return: The page of data! Otherwise, None, mean is the file doesn't exist.
    #     """

    def get_pages_from_disk(self, table_name, page_id):
        file_path = os.path.join(self.db_path, f"{table_name}_page_{page_id}.mpack")
        if os.path.exists(file_path):
            with open(file_path, "rb") as file:
                data = file.read()
            d = msgpack.unpackb(data, raw=False)
            try:
                from lstore.page import Page
                return Page.from_dict(d)
            except Exception:
                return d
        return None


    #     """
    #     delete the page from the disk
    #     @params requirment: 
    #         :param table_name: table of the name
    #         :param page_id: unique page of id
    #     """
    
    def delete_page_from_disk(self, table_name, page_id):
        file_path = os.path.join(self.db_path, f"{table_name}_page_{page_id}.mpack")
        if os.path.exists(file_path):
            os.remove(file_path)


    #     Saving the meta-data from the table, which is include the schema, indexes, and also the page directory
    #     @params requirment: 
    #         :param table: oject of the table, which will contain metadata. Like name, columns, and so on
            
    def save_table_of_metadata_from_disk(self, table):
        # Convert each Page in page_directory to a dict.
        ser_page_directory = {}
        for page_id, page in table.page_directory.items():
            if hasattr(page, "to_dict"):
                ser_page_directory[page_id] = page.to_dict()
            else:
                ser_page_directory[page_id] = page
        metadata = {
            "name": table.name,
            "num_columns": table.num_columns,
            "key_index": table.key,
            "next_rid": table.next_rid,
            "page_directory": ser_page_directory,
            "key_directory": table.key_directory
        }
        # Convert int keys to str so that msgpack accepts them.
        metadata_converted = diskUtilities.convert_keys_to_str(metadata)
        metadata_path = os.path.join(self.db_path, f"{table.name}_metadata.mpack")
        with open(metadata_path, "wb") as file:
            packed = msgpack.packb(metadata_converted, use_bin_type=True)
            file.write(packed)

    #     """
    #     get the meta-data from the disk
    #     @params requirment:
    #         :param table_name: meta-data's table name
    #         :return: the metadata of  dictionary. Otherwise None, which mean is the metadata file does not exist.
    #     """
    def get_table_of_metadata_from_disk(self, table_name):
        metadata_path = os.path.join(self.db_path, f"{table_name}_metadata.mpack")
        if os.path.exists(metadata_path) and os.path.getsize(metadata_path) > 0:
            with open(metadata_path, "rb") as file:
                data = file.read()
            try:
                metadata = msgpack.unpackb(data, raw=False)
            except Exception as e:
                print(f"Error unpacking metadata from {metadata_path}: {e}")
                return None
            # Convert keys back to int where possible.
            metadata = diskUtilities.convert_keys_to_int(metadata)
            ser_page_directory = metadata["page_directory"]
            new_page_directory = {}
            from lstore.page import Page
            for page_id, d in ser_page_directory.items():
                try:
                    new_page_directory[page_id] = Page.from_dict(d)
                except Exception:
                    new_page_directory[page_id] = d
            metadata["page_directory"] = new_page_directory
            return metadata
        return None


    # Alias for compatibility with calls in query.py:
    def save_pages_from_disk(self, table_name, page_id, page_data):
        self.save_page_from_disk(table_name, page_id, page_data)
