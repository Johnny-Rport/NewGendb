import pickle

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = {}

    def has_capacity(self):
        # Returns size of dictionary as bytes
        return len(pickle.dumps(self.data))

    def write(self, rid, value):
        self.num_records += 1
        
        # Size of current bytes in page
        size_data = self.has_capacity()
        
        # Size of value being inserted
        size_value = len(pickle.dumps(value))

        if(size_data + size_value) >= 4096:
            raise MemoryError(f"Insufficient space in page for RID: {rid}")
        else:
            self.data[rid] = value
        pass

    def read(self, rid):
        return self.data[rid]

    # TODO: Mark it for reclamation, need a functioning indirection column for this
    def delete(self, rid):
        self.data[rid] = None
        pass
