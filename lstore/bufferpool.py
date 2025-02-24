import collections
from lstore.page import Page

class BufferPool:
    """
    I'm using the MRU (Most recently use ) and LRU (least recently use) rules to managing the data
    like professor talking into the class lecture. 

    Such as, it caches pages loaded from disk and, when is full, then evicts the least-recently-used
    page that is not currently pinned. Finally, the dirty pages are flushed to disk before the eviction.

    The rules are following with if 
    """
    def __init__(self, capacity, disk_manager):
        self.capacity = capacity                # The maximum number of the pages to the bufferPool
        self.disk_manager = disk_manager        # Control the read and write pages from the disk
        self.pool = collections.OrderedDict()   # key: (table_name, page_id) -> Page
        self.pin_count = {}                     # key: (table_name, page_id) -> int
        self.dirty = {}                         # key: (table_name, page_id) -> bool

    """
        Returns the requested page from cache (if present) or loads it from disk.
        The page is pinned (its pin count increases).
    """
    def get_page(self, table_name, page_id):
        key = (table_name, page_id)
        if key in self.pool:
            self.pool.move_to_end(key)  # Mark as the MRU (most recently used )
            self.pin_count[key] += 1
            return self.pool[key]
        
        # Since, if not in cache, then load from disk or create a new page if not found it !!! 
        page = self.disk_manager.get_pages_from_disk(table_name, page_id)
        if page is None:
            page = Page()
        if len(self.pool) >= self.capacity:
            self.evict_page()
        self.pool[key] = page
        self.pin_count[key] = 1
        self.dirty[key] = False
        return page

     """
        Unpins a page so that its pin count decreases. When the pin count is zero,
        then the page becomes a candidate for eviction.
     """
    def unpin_page(self, table_name, page_id):
        key = (table_name, page_id)
        if key in self.pin_count:
            self.pin_count[key] = max(0, self.pin_count[key] - 1)

    """
        Marks a page as dirty so it will be written back to disk upon eviction.
    """
    def mark_dirty(self, table_name, page_id):
        key = (table_name, page_id)
        self.dirty[key] = True
      
     """
        Evicts the least recently used unpinned page. If the page is dirty,
        it is flushed to disk first.
    """
    def evict_page(self):
        for key in list(self.pool.keys()):
            if self.pin_count.get(key, 0) == 0:
                if self.dirty.get(key, False):
                    table_name, page_id = key
                    self.disk_manager.save_page_from_disk(table_name, page_id, self.pool[key])
                del self.pool[key]
                del self.pin_count[key]
                del self.dirty[key]
                return
        raise Exception("No unpinned pages available for eviction.")
      
    """
      Flushes all dirty pages to disk.
    """
    def flush_all(self):
        for key, page in list(self.pool.items()):
            if self.dirty.get(key, False):
                table_name, page_id = key
                self.disk_manager.save_page_from_disk(table_name, page_id, page)
                self.dirty[key] = False
