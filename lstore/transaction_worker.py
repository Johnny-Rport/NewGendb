
#  * Program Name: Database System -> Milestone3 Update
#  * Author: Next Genrartion
#  * Date: Mar 05/2025 (Final_Version_v04)
#  * Description:
from lstore.table import Table, Record
from lstore.index import Index
import threading

class TransactionWorker:
    def __init__(self, transactions=None):
        if transactions is None:
            transactions = []
        self.transactions = transactions
        self.stats = []
        self.result = 0
        self.worker_thread = None

    def add_transaction(self, t):
        self.transactions.append(t)

    def run(self):
        # Since, need to create a dedicated worker thread that runs all assigned transactions concurrently.
        self.worker_thread = threading.Thread(target=self.__run)
        self.worker_thread.start()

    def join(self):
        if self.worker_thread:
            self.worker_thread.join()

    def __run(self):
        threads = []
        results = [None] * len(self.transactions)

        def run_transaction(i, transaction):
            results[i] = transaction.run()

        # spawn a thread for each transaction.
        for i, transaction in enumerate(self.transactions):
            t = threading.Thread(target=run_transaction, args=(i, transaction))
            threads.append(t)
            t.start()

        # wait for all transaction threads and to finish.
        for t in threads:
            t.join()

        self.stats = results
        # Trying to count the number of transactions that successfully committed.
        self.result = sum(1 for r in results if r)
