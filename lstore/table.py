import os


class DiskManager:
    """
    Responsible for saving and loading pages to/from disk.
    Works with the buffer pool for efficient memory management.
    """

    def __init__(self, db_path):
        self.db_path = db_path
        if not os.path.exists(db_path):
            os.makedirs(db_path)  # Create directory if it doesn’t exist

    def save_page_from_disk(self, table_name, page_id, page_data):
        """ Saves a page to disk """
        file_path = os.path.join(self.db_path, f"{table_name}_page_{page_id}.pkl")
        with open(file_path, "wb") as file:
            pickle.dump(page_data, file)

    def get_pages_from_disk(self, table_name, page_id):
        """ Loads a page from disk into memory """
        file_path = os.path.join(self.db_path, f"{table_name}_page_{page_id}.pkl")
        if os.path.exists(file_path):
            with open(file_path, "rb") as file:
                return pickle.load(file)
        return None  # Page not found

    def delete_page_from_disk(self, table_name, page_id):
        """ Deletes a page from disk """
        file_path = os.path.join(self.db_path, f"{table_name}_page_{page_id}.pkl")
        if os.path.exists(file_path):
            os.remove(file_path)

    def save_table_of_metadata_from_disk(self, table):
        """ Saves table metadata (schema, indexes, etc.) to disk """
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

    def get_table_of_metadata_from_disk(self, table_name):
        """ Loads table metadata from disk """
        metadata_path = os.path.join(self.db_path, f"{table_name}_metadata.pkl")
        if os.path.exists(metadata_path):
            with open(metadata_path, "rb") as file:
                return pickle.load(file)
        return None  # Metadata not found
