import os
import pickle
from collections import OrderedDict

class BufferPoolManager:
    def __init__(self, db_path, pool_size=10):
        """
        Initializes...
        :param db_path: Path to the database directory on disk
        :param pool_size: Maximum number of pages to hold in memory at any time
        """
        self.db_path = db_path
        self.pool_size = pool_size
        self.buffer_pool = OrderedDict()  # Tracks pages in memory (LRU Cache)
        self.dirty_pages = set()  # Tracks modified pages that need to be written to disk
        self.pin_count = {}  # Tracks pinned pages (currently in use)

        # Ensure the database path exists
        if not os.path.exists(db_path):
            os.makedirs(db_path)

    def _get_page_path(self, table_name, page_id):
        """Generates file path for a given page."""
        return os.path.join(self.db_path, f"{table_name}_page_{page_id}.pkl")

    def fetch_page(self, table_name, page_id):  #Loads a page into the buffer pool, evicting if necessary.
        """
        :param table_name: Name of the table
        :param page_id: Unique identifier for the page
        """
        if (table_name, page_id) in self.buffer_pool:
            # Move accessed page to the end (Most Recently Used)
            self.buffer_pool.move_to_end((table_name, page_id))
            return self.buffer_pool[(table_name, page_id)]   #returns The requested page data

        # If buffer pool is full, evict a page
        if len(self.buffer_pool) >= self.pool_size:
            self._evict_page()

        # Load page from disk
        page_path = self._get_page_path(table_name, page_id)
        if os.path.exists(page_path):
            with open(page_path, "rb") as file:
                page_data = pickle.load(file)
        else:
            page_data = {}  # Empty page if it doesn’t exist on disk

        # Add to buffer pool (LRU: new entry goes to end)
        self.buffer_pool[(table_name, page_id)] = page_data
        self.pin_count[(table_name, page_id)] = 1  # Initially pinned
        return page_data

    def mark_dirty(self, table_name, page_id):
        """Marks a page as modified so it must be written to disk before eviction."""
        
        self.dirty_pages.add((table_name, page_id))

    def pin_page(self, table_name, page_id):
        """Increases the pin count to prevent eviction while in use."""
        if (table_name, page_id) in self.pin_count:
            self.pin_count[(table_name, page_id)] += 1
        else:
            self.pin_count[(table_name, page_id)] = 1

    def unpin_page(self, table_name, page_id):
        """Decreases the pin count, allowing eviction when necessary."""
        if (table_name, page_id) in self.pin_count:
            self.pin_count[(table_name, page_id)] -= 1
            if self.pin_count[(table_name, page_id)] == 0:
                del self.pin_count[(table_name, page_id)]  # Allow eviction

    def _evict_page(self):
        """Evicts the least recently used page from the buffer pool."""
        for (table_name, page_id), _ in self.buffer_pool.items():
            if (table_name, page_id) in self.pin_count:
                continue  # Skip pinned pages

            # If the page is dirty, write it back to disk before eviction
            if (table_name, page_id) in self.dirty_pages:
                self._write_page_to_disk(table_name, page_id)
                self.dirty_pages.remove((table_name, page_id))

            # Remove from buffer pool
            del self.buffer_pool[(table_name, page_id)]
            break  # Evict only one page at a time

    def _write_page_to_disk(self, table_name, page_id):
        """Writes a dirty page back to disk."""
        page_path = self._get_page_path(table_name, page_id)
        with open(page_path, "wb") as file:
            pickle.dump(self.buffer_pool[(table_name, page_id)], file)

    def flush(self):
        """Writes all dirty pages back to disk before closing the database."""
        for (table_name, page_id) in list(self.dirty_pages):
            self._write_page_to_disk(table_name, page_id)
            self.dirty_pages.remove((table_name, page_id))
        self.buffer_pool.clear()
        self.pin_count.clear()

    def close(self):
        self.flush()
