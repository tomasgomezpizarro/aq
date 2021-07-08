class BaseHandlerLayer:
    """
    Interface para la comunicación entre la librería y el exterior.
    Los objetos que hereden ésta clase, deberán implementar métodos para comunicarse.
    """
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.callbacks = dict() # diccionario con los callback definidos.
        self.public = None

    def send(self, event, *args):
        """
        Envía mensaje al exterior.
        """
        pass

    def recv(self):
        """
        Recibo mensaje desde el exterior.
        """
        pass

    def register_callback(self, topic, method):
        """
        Registra callbacks en un dict().
        """
        pass

    def subscribe(self, market=None, symbol=None, md_type=None, queue_name=None, routing_key=None):
        """
        Reemplaza al método add_queue. Polimorfismo para las clases que heredan.
        """
        pass

    def unsubscribe(self, market=None, symbol=None, md_type=None, queue_name=None, routing_key=None):
        pass