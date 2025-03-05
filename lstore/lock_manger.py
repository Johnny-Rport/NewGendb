#  * Program Name: Database System -> Milestone3 Update
#  * Author: Next Genrartion
#  * Date: Mar 05/2025 (Final_Version_v04)
#  * Description:
import threading

class LockManager:
    def __init__(self):
        # Using map that record_id -> (lock_type, set of txn_ids)
        self.locks = {}
        self.mutex = threading.Lock()

    def acquire_shared(self, record_id, txn_id):
        with self.mutex:
            if record_id not in self.locks:
                self.locks[record_id] = ('S', {txn_id})
                return True
            lock_type, holders = self.locks[record_id]
            if lock_type == 'S':
                holders.add(txn_id)
                return True
            # If held exclusively by another txn, cannot acquire shared lock.
            if lock_type == 'X' and txn_id in holders:
                return True
            return False

    def acquire_exclusive(self, record_id, txn_id):
        with self.mutex:
            if record_id not in self.locks:
                self.locks[record_id] = ('X', {txn_id})
                return True
            lock_type, holders = self.locks[record_id]
            # Already held exclusively by the same txn.
            if lock_type == 'X' and txn_id in holders:
                return True
            # Allow upgrade if the txn already holds the only shared lock.
            if lock_type == 'S' and holders == {txn_id}:
                self.locks[record_id] = ('X', {txn_id})
                return True
            return False

    def release(self, record_id, txn_id):
        with self.mutex:
            if record_id in self.locks:
                lock_type, holders = self.locks[record_id]
                if txn_id in holders:
                    holders.remove(txn_id)
                    if not holders:
                        del self.locks[record_id]
