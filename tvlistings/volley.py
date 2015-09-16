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
                requests.get(job.get('url'), params=job.get('params'), hooks=dict(response=job.get('cb')))
                # job.get('cb')(r)
                self.queue.task_done()
                # print 'Task done'


class Volley:
    queue_list = []
    providers_map = {}

    def __init__(self, providers, thread_pool=8):
        i = 0
        for api_name, api_id in providers.iteritems():
            self.providers_map[api_id] = i
            q = Queue.Queue()
            self.queue_list.append(q)
            for j in range(thread_pool):
                t = WorkerThread(q)
                t.daemon = True
                t.start()
            i += 1

    def get(self, url, params, cb, provider):
        job = {
            'url': url,
            'params': params,
            'cb': cb
        }
        self.queue_list[self.providers_map[provider]].put(job)
        # self.queue.put(job)

    def join(self):
        for q in self.queue_list:
            q.join()
