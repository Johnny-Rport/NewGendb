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
import pickle

class DiskManager:
    """
    Responsible for saving and loading pages to/from disk.
    Ensures data persistence when the database is closed.
    """

    def __init__(self, db_path):
        self.db_path = db_path
        if not os.path.exists(db_path):
            os.makedirs(db_path) # if doesn't exsit, creative a directory

    """
        Trying to saving the page into the disk
        @params requirment: 
            :param table_name: table of the name
            :param page_id:  unique page of id
            :param page_data: page of the datas
    """
    def save_page_from_disk(self, table_name, page_id, page_data):
        print(f"table name: {table_name} , page id: {page_id}, page data: {page_data} \n")
        file_path = os.path.join(self.db_path, f"{table_name}_page_{page_id}.pkl")
        # print(f"file path, {file_path} \n") // debugging and testing the files output
        with open(file_path, "wb") as file:
            pickle.dump(page_data, file)


    """
    get the pages informations from the disk
    @params requirment: 
        :param table_name: table of the name
        :param page_id:  unique page of id
        :return: The page of data! Otherwise, None, mean is the file doesn't exist.
    """
    def get_pages_from_disk(self, table_name, page_id):
        file_path = os.path.join(self.db_path, f"{table_name}_page_{page_id}.pkl")
        if os.path.exists(file_path):
            with open(file_path, "rb") as file:
                return pickle.load(file)
        return None  # page not found!


    """
    delete the page from the disk
    @params requirment: 
        :param table_name: table of the name
        :param page_id: unique page of id
    """
    def delete_page_from_disk(self, table_name, page_id):
        file_path = os.path.join(self.db_path, f"{table_name}_page_{page_id}.pkl")
        if os.path.exists(file_path):
            os.remove(file_path)

    """
    saving the meta-data from the table, which is include the schema, indexes, and also the page directory
    @params requirment: 
        :param table: oject of the table, which will contain metadata. Like name, columns, and so on
    """
    def save_table_of_metadata_from_disk(self, table):
        metadata_path = os.path.join(self.db_path, f"{table.name}_metadata.pkl")
        metadata = {
            "name": table.name,
            "num_columns": table.num_columns,
            "key_index": table.key,
            "next_rid": table.next_rid,
            "page_directory": table.page_directory
        }
        with open(metadata_path, "wb") as file:
            pickle.dump(metadata, file)


    """
    get the meta-data from the disk
    @params requirment:
        :param table_name: meta-data's table name
        :return: the metadata of  dictionary. Otherwise None, which mean is the metadata file does not exist.
    """

    def get_table_of_metadata_from_disk(self, table_name):
        metadata_path = os.path.join(self.db_path, f"{table_name}_metadata.pkl")
        if os.path.exists(metadata_path):
            with open(metadata_path, "rb") as file:
                return pickle.load(file)
        return None  # metadata found!
