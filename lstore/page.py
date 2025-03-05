#  * Program Name: Database System -> L_Store Concepts
#  * Author: Next Generation
#  * Date: Feb 11/2025 (Final_Version_v04)
#  * Description:
'''
    Update: Milestone2
    Surgery area: We are changed package from Pickle to the msgpack! 
    But the code is not modify too much. Only changed obj from (Pickle to msgpack)
    and some smallest adjustment !!!! 
    Since, the pickle package has security issues. Thus, Tianren Chen and Johnny make final decison
    changed msgpack became the new package.
'''
import msgpack

PAGE_SIZE = 4096

class Page:

    def __init__(self):
        #print("Creating new page.")  # Debugging output
        self.num_records = 0
        self.data = []

    def has_capacity(self):
        # Returns size of dictionary as bytes
        return (len(msgpack.dumps(self.data)) < PAGE_SIZE)

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
