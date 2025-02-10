import pickle

PAGE_SIZE = 4096

class Page:

    def __init__(self):
        #print("Creating new page.")  # Debugging output
        self.num_records = 0
        self.data = []

    def has_capacity(self):
        # Returns size of dictionary as bytes
        return (len(pickle.dumps(self.data)) < PAGE_SIZE)

    def write(self, value):
        #print(f"Before write: num_records = {self.num_records}, data = {self.data}")  # Debugging output
        if(not self.has_capacity()):
            #print("Page full, cannot write more data.")  # Debugging output
            return False
        self.data.append(value)
        self.num_records += 1
        #print(f"After write: num_records = {self.num_records}, data = {self.data}")  # Debugging output
        return True

    def read(self, index):
        if(0 <= index and index < len(self.data)):
            return self.data[index]
        return None

