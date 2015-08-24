import Queue
import threading
import requests
import time
# from BeautifulSoup import BeautifulSoup


class Volley:
    #queues = []
    queue = Queue.Queue()

    def __init__(self, queue_size=4, thread_pool=8):
        """
        for i in range(queue_size):
            self.queues.append(Queue.Queue())
        """
        for i in range(thread_pool):
            t = threading.Thread(target=self.worker)
            t.daemon = True
            t.start()
    #
    # def max_load_queue(self):
    #     max_q, max_load = None, -1
    #     for q in self.queues:
    #         if max_load < q.qsize():
    #             max_load = q.qsize()
    #             max_q = q
    #
    #     print 'max queue: ' + str(max_q.qsize())
    #     return max_q
    #
    # def min_load_queue(self):
    #     min_q, min_load = None, 1000000
    #     for q in self.queues:
    #         if min_load > q.qsize():
    #             min_load = q.qsize()
    #             min_q = q
    #
    #     print 'min queue: ' + str(min_q.qsize())
    #     return min_q

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


def cb(r, *args, **kwargs):
    print 'Response: ' + str(r)

if __name__ == '__main__':
    v = Volley()
    v.get('http://stay-tunedapp.rhcloud.com/api/listings?startime=201508261557&stoptime=201508261857&channels=Star%20Movies,HBO', {
        'a': 'b'
    }, cb)

    v.get('http://stay-tunedapp.rhcloud.com/api/listings?startime=201508261557&stoptime=201508261857&channels=Star%20Movies,HBO', {
        'a': 'b'
    }, cb)

    v.get('http://stay-tunedapp.rhcloud.com/api/listings?startime=201508261557&stoptime=201508261857&channels=Star%20Movies,HBO', {
        'a': 'b'
    }, cb)


    v.get('http://stay-tunedapp.rhcloud.com/api/listings?startime=201508261557&stoptime=201508261857&channels=Star%20Movies,HBO', {
        'a': 'b'
    }, cb)
    v.get('http://stay-tunedapp.rhcloud.com/api/listings?startime=201508261557&stoptime=201508261857&channels=Star%20Movies,HBO', {
        'a': 'b'
    }, cb)
    v.get('http://stay-tunedapp.rhcloud.com/api/listings?startime=201508261557&stoptime=201508261857&channels=Star%20Movies,HBO', {
        'a': 'b'
    }, cb)
    v.get('http://stay-tunedapp.rhcloud.com/api/listings?startime=201508261557&stoptime=201508261857&channels=Star%20Movies,HBO', {
        'a': 'b'
    }, cb)

    v.join()
    print 'hello workd'

