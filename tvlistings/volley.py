import Queue
import threading
import requests
import time


class WorkerThread(threading.Thread):
    def __init__(self, queue):
        super(WorkerThread, self).__init__()
        self.queue = queue

    def run(self):
        while True:
            job = self.queue.get()
            if job:
                r = requests.get(job.get('url'), params=job.get('params'))
                # requests.get(job.get('url'), params=job.get('params'), hooks=dict(response=job.get('cb')))
                job.get('cb')(r)
                self.queue.task_done()
                print 'Task done'


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

        if self.queue.qsize() > 30:
            time.sleep(5)

    def join(self):
        self.queue.join()
