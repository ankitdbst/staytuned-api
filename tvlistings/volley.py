import Queue
import threading
import requests


class Volley:
    queue = Queue.Queue()

    def __init__(self, thread_pool=8):
        for i in range(thread_pool):
            t = threading.Thread(target=self.worker)
            t.daemon = True
            t.start()

    def worker(self):
        job = self.queue.get()
        requests.get(job.get('url'), params=job.get('params'), hooks=dict(response=job.get('cb')))
        self.queue.task_done()

    def get(self, url, params, cb):
        job = {
            'url': url,
            'params': params,
            'cb': cb
        }
        self.queue.put(job)

    def join(self):
        self.queue.join()
