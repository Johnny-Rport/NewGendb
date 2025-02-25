#  * Program Name: Database System -> L_Store Concepts
#  * Author: Next Generation
#  * Date: Feb 11/2025 (Final_Version_v04)
#  * Description: 

from lstore.page import Page

# Job is to track the pages
"""
page_list -> All data
tail_pages -> For updates
meta_pages -> Has the following:
    version
    tail_v (Tail version)
    min_rid
    max_rid
"""

#TODO: INDEXING TO GAIN VERSIONING or DOUBLE POITERS METHOD
class Page_directory:
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.pageid_counter = []
        self.page_list = {}
        self.tail_pages = [] # [0 1 2 3]
        self.meta_pages = [] # [0 1 2 3]
        self.initialize()
        pass
    
    """
    * Function Name: initialize
    * Purpose: Creates the initial set of pages
    * Parameter: None
    * Return: None
    """

    def initialize(self):
        self.make_meta() # Make meta and tail page
        for pageid in range(self.num_columns):
            self.pageid_counter.append(pageid)
            self.make_page(pageid)
        pass

    """
    * Function Name: expand
    * Purpose: Increase amount of pages
    * Parameter: rid -> int
    * Return: None
    """

    def expand(self, rid):
        self.make_meta(rid) # Make meta and tail page
        for index in range(self.num_columns):
            self.pageid_counter[index] += self.num_columns
            index = self.pageid_counter[index]
            self.make_page(index)
        pass

    """
    * Function Name: make_meta
    * Purpose: Make the meta and tail page
    * Parameter: min_rid -> int
    * Return: None
    """

    def make_meta(self, min_rid=0):
        page = Page()
        self.tail_pages.append(page)

        meta = {"min_rid": min_rid, "max_rid": min_rid+1}
        self.meta_pages.append(meta)
        pass

    """
    * Function Name: make_page
    * Purpose: make a generic page, id it, and insert into page_list
    * Parameter: pageid -> int from pageid_counter
    * Return: None
    """

    def make_page(self, pageid):
        page = Page()
        self.page_list[pageid] = page
        pass

    """
    * Function Name: get_base_tail
    * Purpose: Grabs the most recent base pages
    * Parameter: column -> int
    * Return: page in specified column
    """

    def get_base_tail(self, column):
        id = self.pageid_counter[column]
        return self.page_list[id]

    """
    * Function Name: get_base
    * Purpose: Find the base page of specified rid
    * Parameter: rid -> int, i -> iterator
    * Return: The RID's Base page
    """

    def get_base(self, rid, i):
        index = self.meta_pages.index(self.meta(rid))
        offset = index*5
        return self.page_list[i+offset]

    """
    * Function Name: tail
    * Purpose: Find the tail page for RID
    * Parameter: rid -> int
    * Return: tail page for RID
    """

    def tail(self, rid):
        index = self.meta_pages.index(self.meta(rid))
        return self.tail_pages[index]

    """
    * Function Name: meta
    * Purpose: Find the meta page with the specified rid value
    * Parameter: rid -> int
    * Return: Meta page of RID
    """

    def meta(self, rid):
        for meta_page in self.meta_pages:
            if(meta_page["min_rid"] <= rid <= meta_page["max_rid"]):
                return meta_page

#  Version | Version_Tail | MIN rid range | MAX rid range 
    
#   FOR BASE PAGES
#   example: 5 columns
#   i += 5, for each page increment by 5 for easy book keeping

# --Indirection | --Column Pages--- |
#   meta0-tail  | Page 0  | Page 1  | and so on
#   meta1-tail  | Page 5  | Page 6  |
#   meta2-tail  | Page 10 | Page 11 |
#   meta3-tail  | Page 15 | Page 16 |
#   meta4-tail  | Page 20 | Page 21 |

# metaID is connected to index
# Meta page and tail page are connected through the index
# Whether to check the base page or tail page is based on the version and version_tail
# Debating the dirty boolean because we can merge when the tail page is full
