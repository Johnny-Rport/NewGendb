from lstore.index import Index
from lstore.page import Page
from lstore.page_directory import Page_directory
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
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    #TODO: Imlpemenet page_directory class to store pages
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

    def insert_record(self, data: list):
        record = Record(self.rid, data[0], data[1:])
        self.page_directory.get_tail(0).write(record.rid, record.key)
        for index in range(1, self.num_columns):
            self.page_directory.get_tail(index).write(record.rid, record.columns[index-1])

        self.rid += 1
        pass

    #TODO: If there are more than 5 pages, implement way to get proper page, refer to page_directory
    # Problem would occur in get_tail(i), what if we want a previous page not the latest page?
    def read_record(self, rid):
        data = []
        for i in range(self.num_columns):
            data.append(self.page_directory.get_tail(i).read(rid))
        return data


#   FOR BASE PAGES
#   5 columns
#   i + (length_of_column - 1) * 5

# --Indirection -----------------
#   meta1       | Page 0  | Page 1 | and so on
#   meta2       | Page 5  | Page 6
#   meta3       | Page 10 | Page 11
#   meta4       | Page 15 | Page 16
#   meta5       | Page 20 | Page 21