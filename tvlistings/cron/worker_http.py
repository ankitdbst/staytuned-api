#!/usr/bin/env python
from gevent import monkey
monkey.patch_all()

import time
import logging

import copy_reg
import types

import grequests
import requests

from multiprocessing import JoinableQueue, Process

SLEEP_INTERVAL = 5


# We need to pickle instance methods of the Worker Class below so this snippet does that
# Refer: http://stackoverflow.com/questions/1816958/cant-pickle-type-instancemethod-
# when-using-pythons-multiprocessing-pool-ma

# START
def _pickle_method(method):
    func_name = method.im_func.__name__
    obj = method.im_self
    cls = method.im_class
    return _unpickle_method, (func_name, obj, cls)


def _unpickle_method(func_name, obj, cls):
    for cls in cls.mro():
        try:
            func = cls.__dict__[func_name]
        except KeyError:
            pass
        else:
            break
    return func.__get__(obj, cls)

copy_reg.pickle(types.MethodType, _pickle_method, _unpickle_method)
# END


class WorkerProcessor(Process):
    """
    Worker to process the responses to HTTP requests sent
    Abstract class
    """
    def __init__(self, queue, processor_fn):
        """
        Constructor
        :param queue: JoinableQueue object which will contain the responses
        :param processor_fn: Function to perform the processing
        :return: None
        """
        super(WorkerProcessor, self).__init__()
        self.queue = queue
        self.processor_fn = processor_fn

    def run(self):
        """
        Run
        :return: None
        """
        while True:
            rs = self.queue.get()
            self.processor_fn(rs)
            self.queue.task_done()


class WorkerHTTP(object):
    """
    Worker to sent HTTP requests
    """
    def __init__(self, worker_size=4, pool_size=15, max_retries=None, sleep_interval=SLEEP_INTERVAL):
        """
        Constructor
        :param worker_size: No of child processes or workers to start
        :param pool_size: Size of the Pool to setup for sending concurrent requests using grequests
        :param max_retries: No of retries after which we need to shutdown the Workers
        :return: None
        """
        self._session = requests.Session()
        self._to_process_mq = JoinableQueue()

        self._worker_size = worker_size
        self._pool_size = pool_size
        self._max_retries = max_retries

        self._requests_list = []
        self._retries_list = []

    # Here we remove the Queue object from the dict that has to be pickled
    # Since the instance object is already being pickled
    def __getstate__(self):
        self_dict = self.__dict__.copy()
        del self_dict['_to_process_mq']
        return self_dict

    def __setstate__(self, self_dict):
        self.__dict__ = self_dict

    def start(self):
        """
        Start the Processor Workers to process response of HTTP requests sent
        :return: None
        """
        for i in range(self._worker_size):
            p = WorkerProcessor(self._to_process_mq, self.process_response)
            p.daemon = True
            p.start()

        self.prepare()

        working = True
        retry_count = 0

        while working:
            grequests.map(self._requests_list, size=self._pool_size, stream=False)
            if len(self._retries_list) == 0:
                break
            # sleep before a retry
            time.sleep(SLEEP_INTERVAL)
            # reset state of requests and retries array
            self._requests_list = self._retries_list
            self._retries_list = []
            logging.info("Retrying ... for %d URLs" % len(self._requests_list))
            if self._max_retries is not None:
                retry_count += 1
                working = retry_count == self._max_retries

        self._to_process_mq.join()

    def prepare(self):
        """
        Method to prepare the Worker for sending/processing HTTP requests
        :return: None
        """
        raise NotImplementedError, "Method not implemented"

    def process_response(self, item):
        """
        Method to process response for all the HTTP requests' response added to MQ
        :param item: Response object in the MQ
        :return: None
        """
        raise NotImplementedError, "Callback not implemented"

    def process_request(self, r, *args, **kwargs):
        """
        Method to process the request sent by the HTTP Worker (add to the MQ for response processing)
        :param r: HTTP requests' response object
        :param args:
        :param kwargs:
        :return: None
        """
        raise NotImplementedError, "Callback not implemented"

    def put_request(self, url, payload, retry=False):
        """
        Method to add a request to the request_list to be sent by the HTTP Worker
        :param url:
        :param payload:
        :param retry:
        :return:
        """
        r = grequests.get(url, params=payload, hooks={'response': self.process_request}, session=self._session)
        if retry:
            self._retries_list.append(r)
        else:
            self._requests_list.append(r)

    def put_response(self, rs):
        """
        Method to add the response of the request to the MQ
        :param rs:
        :return:
        """
        self._to_process_mq.put(rs)
