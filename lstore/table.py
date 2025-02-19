from lstore.index import Index
from lstore.page import Page
from lstore.page_directory import Page_directory
from datetime import datetime

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
        self.rid = 0
        pass

    def __merge(self):
        print("merge is happening")
        pass


    """
    * Function Name: insert_record
    * Purpose: Insert record into page_directory
    * Parameter: data -> List of desired data to insert
    * Return: None
    """

    def insert_record(self, data: list):
        record = Record(self.rid, data[0], data[1:])
        try:
            self.page_directory.get_base_tail(0).write(record.rid, record.key)
            for index in range(1, self.num_columns):
                self.page_directory.get_base_tail(index).write(record.rid, record.columns[index-1])
            #version
            self.page_directory.meta(record.rid)["max_rid"] = record.rid+1
        except MemoryError:
            #version_tail???
            self.page_directory.meta(record.rid-1)["max_rid"] -= 1
            self.page_directory.expand(record.rid)
            self.page_directory.get_base_tail(0).write(record.rid, record.key)
            for index in range(1, self.num_columns):
                self.page_directory.get_base_tail(index).write(record.rid, record.columns[index-1])
        self.rid += 1
        pass

    """
    * Function Name: read_record
    * Purpose: Read record within page_directory
    * Parameter: rid -> int
    * Return: list of data, false if not found
    """

    def read_record(self, rid):
        data = []
        try:
            for i in range(0, self.num_columns):
                data.append(self.page_directory.get_base(rid, i).read(rid))
            return data
        except:
            return False
    
    """
    * Function Name: update_record
    * Purpose: Update Record within page_directory
    * Parameter: rid -> int, data -> List of desired data to insert
    * Return: none, false if not found
    """
    def update_record(self, rid, data: list):
        try:
            # Update tail_v
            self.page_directory.tail(rid).write(rid, data)    
        #except MemoryError:
        # implement error checking, if MemoryError -> Start Merge
        #    print()
        except:
            return False

    """
    * Function Name: get_tail
    * Purpose: Debugging function for tail pages
    * Parameter: rid -> int
    * Return: Tail page and the desired value in tail
    """

    # Debugger function for tail pages
    def get_tail(self, rid):
        tail = self.page_directory.tail(rid)
        print(tail)
        print(tail.read(rid))
        pass


    """
    * Function Name: get_meta
    * Purpose: Debugging function for meta pages
    * Parameter: rid -> int
    * Return: Meta page of rid
    """
    # Debugger function for metapages
    def get_meta(self, rid):
        print(self.page_directory.meta(rid))
        pass