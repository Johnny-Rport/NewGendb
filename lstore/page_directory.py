from lstore.page import Page

# Job is to track the pages
#TODO: Metadata
class Page_directory:
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.pageid_counter = []
        self.page_list = {}
        self.meta_pages = [] # Pending
        self.initialize()
        pass

    def initialize(self):
        #Create meta page
        for pageid in range(self.num_columns):
            self.pageid_counter.append(pageid)
            self.make_page(pageid)
        pass

    def make_page(self, pageid):
        page = Page()
        self.page_list[pageid] = page
        pass

    def generate_id(self, index):
        self.pageid_counter[index] += self.num_columns
        return self.pageid_counter[index]
    
    def get_tail(self, column):
        id = self.pageid_counter[column]
        return self.page_list[id]

    #TODO: Figure out a way to add meta data
    def meta_data(self):
        pass


#   FOR BASE PAGES
#   example: 5 columns
#   i += 5, for each page increment by 5 for easy book keeping

# --Indirection | ----------------- |
#   meta1       | Page 0  | Page 1  | and so on
#   meta2       | Page 5  | Page 6  |
#   meta3       | Page 10 | Page 11 |
#   meta4       | Page 15 | Page 16 |
#   meta5       | Page 20 | Page 21 |

# Using the list, the base tail is always being pointed at, for easy appending
# For querying use meta data to choose the page for RID
#??? What goes into the metadata?