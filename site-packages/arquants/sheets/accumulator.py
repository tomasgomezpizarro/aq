import traceback
from multiprocessing import Manager

from apscheduler.schedulers.background import BackgroundScheduler
import threading


class Accumulator:

    def __init__(self, processor):
        manager = Manager()
        self._scheduler = BackgroundScheduler()
        self._interval = 4
        self._items = manager.list()
        self._lock = threading.Lock()
        self._processor = processor

    def process_item(self, item):
        with self._lock:
            print('agregando item a acumulator')
            self._items.append(item)

    def start(self):
        self._scheduler.start()
        self._scheduler.add_job(func=self._purge, trigger='interval', seconds=self._interval, args=[])

    def stop(self):
        self._scheduler.shutdown()

    def _purge(self):
        with self._lock:
            if len(self._items) == 0:
                return
            print('enviando item a batch')
            extracted_items = self._items[:]
            try:
                self._processor(extracted_items)
            except:
                print(traceback.format_exc())
            self._items[:] = []

# ==============================================================================

# print('starting test..')
#
# def printer_processor(message):
#     print(message)
#
# acc = Accumulator(printer_processor)
# acc.start()
#
# for i in range(20):
#     acc.process_item('foo' + str(i))
#     if not i % 5:
#         time.sleep(4)
#
# time.sleep(10)
# acc.stop()
#
# print('..done')
