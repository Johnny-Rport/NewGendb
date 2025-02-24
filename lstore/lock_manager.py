# lock_manager.py

class LockManager:
    def __init__(self):
        self.locks = {}

    def acquire_read(self, rid):
        # Implement read lock logic (blocking or non-blocking)
        if rid not in self.locks:
            self.locks[rid] = "read"
            return True
        return False

    def release_read(self, rid):
        # Release the read lock for the given RID
        if rid in self.locks:
            del self.locks[rid]
