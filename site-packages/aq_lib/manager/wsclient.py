import threading
import websocket
import simplejson
import datetime
import urllib
import ssl
from queue import Queue

# Cola donde van a ir los mensajes que entren del websocket.
from aq_lib.utils.logger import logger

manager_queue = Queue()


class WebSocketClient:
    def __init__(self, _ws_url, _token=None, ws_uuid=None):
        """
        Params:
            url_base: Url for connect and get token.
            url_ws: Url for websocket connection.
        """
        self.ws_url = _ws_url
        self.token = _token
        self.ws = None
        self.ws_thread = None
        self.active = False
        self.ws_uuid = ws_uuid

    def md_subscribe(self, _payload):
        """
        Me suscribo a un channel del websocket. Por ejemplo marketdata.
        :param _payload: Estructura de "WSMessageMD".
        :return
        """
        data = {'type': 'subscribe', 'payload': _payload}
        self.send_msg(data)

    def md_unsubscribe(self, _payload):
        """
        Me desusbribo a un channel del websocket. Por ejemplo marketdata.
        :param _payload: Estructura de "WSMessageMD".
        :return:
        """
        data = {'type': 'unsubscribe', 'payload': _payload}
        self.send_msg(data)

    def start(self):
        """
        Inicia el websocket, pasandole el querystring como parámetro.
        """
        if not self.active:
            # Check token.
            if self.token is not None:
                query_string = {
                    'token': self.token
                }
                # websocket.enableTrace(True)
                self.ws = websocket.WebSocketApp(self.ws_url + '?' + urllib.parse.urlencode(query_string),
                                                 on_message=lambda ws, msg: self.on_message(ws, msg),
                                                 on_error=lambda ws, msg: self.on_error(ws, msg),
                                                 on_close=lambda ws: self.on_close(ws),
                                                 on_open=lambda ws: self.on_open(ws)
                                                 )
                # sslopt = {"cert_reqs": ssl.CERT_NONE}
                self.ws_thread = threading.Thread(target=self.ws.run_forever,
                                                  kwargs={'ping_interval': 10, 'sslopt': {'cert_reqs': ssl.CERT_NONE}})
                self.ws_thread.start()
                logger.info('Websocket connected! %s ' % self.ws_thread.getName())
                self.active = True
            else:
                logger.info('Websocket cannot connect!')
        else:
            # Websocket is already active.
            logger.info('Websocket is alive!')

    def stop(self):
        """
        Detengo el websocket.
        """
        if self.active:
            self.ws.keep_running = False
            self.active = False
            logger.info('Websocket disconnected!')
        else:
            logger.info('Websocket is not alive!')

    @staticmethod
    def on_message(ws, message):
        """
        Recibo mensaje, evento AQ.
        """
        print(message)
        msg = simplejson.loads(message)
        logger.info(msg)
        # Meto el mensaje en la queue.
        manager_queue.put(msg)

    @staticmethod
    def on_error(ws, error):
        print(error)
        print("### connection error ###")
        print(datetime.datetime.now())
        logger.info('Websocket connection error.')

    @staticmethod
    def on_close(ws):
        print("### connection closed ###")
        print(datetime.datetime.now())
        logger.info('Websocket connection closed.')

    def on_open(self, ws):
        print("Conection Open...")
        print(datetime.datetime.now())
        # Agrego el UUID en la data que envío para crear un mapa del lado de channels #
        self.send_msg({
            'payload': {
                'uuid': self.ws_uuid
            }
        })
        logger.info('Websocket connection opened.')

    def send_msg(self, params):
        """
        Envío mensajes al websocket.
        :param params: dict() con los parametros de la request al ws.
        :return: response
        """
        msg = simplejson.dumps(params)
        self.ws.send(msg)


class WSMessageMD:
    """
    Contiene los campos que tiene un mensaje de websocket.
    :param _market:     Mercado correspondiente de _symbol.
    :param _symbol:     Instrumento a solicitar.
    :param _md_type:    Tipo de marketdata. ('top' | 'aggregated' | 'disaggregated').
    """
    def __init__(self, _market: str, _symbol: str, _md_type: int):
        self.market = _market
        self.symbol = _symbol
        self.md_type = _md_type

    def get_dict(self):
        """
        Retorno dict armado para agregar al payload, y luego al websocket.
        :return dict():
        """
        return self.__dict__


if __name__ == "__main__":
    pass
