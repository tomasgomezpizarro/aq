from logging import StreamHandler

from threading import Thread

from multiprocessing import Queue


class AsyncHandlerMixin(object):

    def __init__(self, *args, **kwargs):
        super(AsyncHandlerMixin, self).__init__(*args, **kwargs)
        self.__queue = Queue()
        self.__thread = Thread(target=self.__loop)
        self.__thread.daemon = True
        self.__thread.start()

    def emit(self, record):
        self.__queue.put(record)

    def __loop(self):

        while True:
            record = self.__queue.get()
            try:
                super(AsyncHandlerMixin, self).emit(record)
            except:
                pass


class AsyncStreamHandler(AsyncHandlerMixin, StreamHandler):
    pass