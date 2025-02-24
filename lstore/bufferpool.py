import collections
from lstore.page import Page
"""
    Police control rules (MRU & LRU: 
    I'm using the MRU (Most recently use ) and LRU (least recently use) rules to managing the data
    trieved like professor talking durng the lecture time. 
        I'm using the in_order dictionary to implementation this skill
            i.e the first element into the dictionary (OrderedDict) that is the LRU
            i.e the last  element into the dictionary that is the MRL 
    It caches pages loaded from disk and, when full, evicts the least-recently-used
    page that is not currently pinned. Dirty pages are flushed to disk before eviction.

    Function have it:
        get_page(self, table_name, page_id):
        unpin_page(self, table_name, page_id):
        mark_dirty(self, table_name, page_id):
        evict_page(self):
        flush_all(self):
"""
class BufferPool:
    def __init__(self, capacity, disk_manager):
        self.capacity = capacity                # The maximum number of the pages to the bufferPool
        self.disk_manager = disk_manager        # Control the read and write pages from the disk
        # Hold the data into the cache buffer pool 
        self.cache_bufferPool = collections.OrderedDict()   # key: (table_name, page_id) -> Page
        self.pin_count = {}                     # key: (table_name, page_id) -> int
        self.dirty = {}                         # key: (table_name, page_id) -> bool

    """
        Returns the requested page from cache (if present) or loads it from disk.
        The page is pinned (its pin count increases).
         
    """
    def get_page(self, table_name, page_id):
        key = (table_name, page_id)
        if key in self.cache_bufferPool: # if the key can be found into the bufferPool
            self.cache_bufferPool.move_to_end(key)  # Mark as the MRU (most recently used )
            self.pin_count[key] += 1 # Increment by 1, indicate the page has been used
            return self.cache_bufferPool[key]
        
        # if not in cache BufferPool, trying to load from disk
        page = self.disk_manager.get_pages_from_disk(table_name, page_id)
        if page is None:
            # if not, -> create a new page if not found
            page = Page()
        # Consider, if data was overflow the capacity? 
        # Evict the page 
        if len(self.cache_bufferPool) >= self.capacity:
            self.evict_page()
        self.cache_bufferPool[key] = page
        self.pin_count[key] = 1
        self.dirty[key] = False
        return page
    

    """
        Unpins a page so that its pin count decreases. When the pin count is zero,
        the page becomes a candidate for eviction.
    """
    def unpin_page(self, table_name, page_id):
        key = (table_name, page_id)
        if key in self.pin_count:
            self.pin_count[key] = max(0, self.pin_count[key] - 1)


    """
        Indicate the page has been modified it
    """
    def mark_dirty(self, table_name, page_id):
        key = (table_name, page_id)
        self.dirty[key] = True

    """
        My evicts function will remove the LRU (lease recently used) page, also is unpinned page

        Evicts the least recently used unpinned page. If the page is dirty,
        it is flushed to disk first.
    """
    def evict_page(self):
        for key in list(self.cache_bufferPool.keys()):
            if self.pin_count.get(key, 0) == 0: # the page is no long to used. Thus, legal to evict
                if self.dirty.get(key, False):
                    table_name, page_id = key
                    self.disk_manager.save_page_from_disk(table_name, page_id, self.cache_bufferPool[key])
                del self.cache_bufferPool[key]
                del self.pin_count[key]
                del self.dirty[key]
                return
        # Still have the pinned pages !! 
        raise Exception("Error: evict fail, because still have pinned pages!!!! \n")
    

    """
        Saving all of the dirty page into the disk
        when function has been called
    """
    def flush_all(self):
        for key, page in list(self.cache_bufferPool.items()):
            if self.dirty.get(key, False):
                table_name, page_id = key
                self.disk_manager.save_page_from_disk(table_name, page_id, page)
                self.dirty[key] = False
