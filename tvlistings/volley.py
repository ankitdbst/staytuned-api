import Queue
import threading
import requests


class WorkerThread(threading.Thread):
    def __init__(self, queue):
        super(WorkerThread, self).__init__()
        self.queue = queue

    def run(self):
        while True:
            job = self.queue.get()
            if job:
                requests.get(job.get('url'), params=job.get('params'), hooks=dict(response=job.get('cb')))
                self.queue.task_done()


class Volley:
    queue = Queue.Queue()

    def __init__(self, thread_pool=8):
        for i in range(thread_pool):
            t = WorkerThread(self.queue)
            t.daemon = True
            t.start()

    def get(self, url, params, cb):
        job = {
            'url': url,
            'params': params,
            'cb': cb
        }
        self.queue.put(job)

    def join(self):
        self.queue.join()
