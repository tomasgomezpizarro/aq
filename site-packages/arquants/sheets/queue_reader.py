from multiprocessing import Process


class QueueReader(Process):
    def __init__(self, queue=None, processor=None):
        super(QueueReader, self).__init__()
        self.logger_queue = queue
        self.processor = processor

    def run(self):
        while True:
            item = self.logger_queue.get()
            print("se leyo un item en queue reader")
            self.processor.process_item(item)
