from aq_lib.event.event_mapper import EventMapper
from aq_lib.event.event_type import EventType
from aq_lib.manager.handler import BaseHandlerLayer
from aq_lib.manager.wsclient import WebSocketClient
from threading import Thread
import json
import uuid

from aq_lib.manager.httpclient import HttpClient
from aq_lib.utils.logger import logger


class PublicAQHandlerLayer(BaseHandlerLayer):
    """
    Estrategia que implementa a BaseHandlerLayer para interactuar con entorno Demo.
    """
    def __init__(self, _aq_username, _aq_password, _api_endpoint, _ws_endpoint_er, _ws_endpoint_md, _queue):
        BaseHandlerLayer.__init__(self)
        # Diccionario con los endpoints de la api.
        self.endpoint_dict = {
            # EventType.MARKET_DATA_REQUEST_EVENT.value: 'market_data_request',
            EventType.NEW_ORDER_EVENT.value: 'new_order/',
            EventType.BULK_NEW_ORDER_EVENT.value: 'bulk_new_order/',
            EventType.ORDER_CANCEL_REQUEST_EVENT.value: 'order_cancel_request/',
            EventType.ORDER_CANCEL_REPLACE_REQUEST_EVENT.value: 'order_cancel_replace_request/',
            EventType.ORDER_MASS_CANCEL_REQUEST_EVENT.value: 'order_mass_cancel_request/',
            EventType.ORDER_STATUS_REQUEST_EVENT: 'oder_status_request/',
            EventType.TRADE_EXECUTED_EVENT.value: 'trade_execute'
        }
        self.public = True
        self.queue = _queue
        self.http_client = HttpClient(_aq_username, _aq_password, _api_endpoint)
        self.ws_uuid = uuid.uuid4().hex[:16]
        self.ws_er_client = WebSocketClient(_ws_endpoint_er, ws_uuid=self.ws_uuid)
        self.ws_md_client = WebSocketClient(_ws_endpoint_md, ws_uuid=self.ws_uuid)

        self.param_account_id = None
        self.param_negagent_id = None
        self.account = None

        self.active = False
        self.t = None

        # Ésto inicia la clase una vez instanciada.
        # Comentar en caso de querer hacerlo por afuera.
        # self.init_public_aq()

    def init_public_aq(self):
        """
        Inicializo el Public Handler.
        """
        self.init_http()
        self.ws_md_client.token = self.http_client.token
        self.ws_er_client.token = self.http_client.token
        self.init_all_ws()
        # Creo hilo para que el handler empieze a recibir los mensajes de forma asíncrona.
        self.init_thread()
        self._set_account()

    def stop_public_aq(self):
        """
        Finalizo el Public Handler.
        """
        self.active = False
        # self.stop_http()
        self.stop_all_ws()

    def init_all_ws(self):
        """
        Inicio el ws.
        :return:
        """
        self.ws_md_client.start()
        self.ws_er_client.start()

    def stop_all_ws(self):
        """
        Detengo el ws.
        :return:
        """
        self.ws_md_client.stop()
        self.ws_er_client.stop()

    def init_http(self):
        """
        Inicio el http client.
        :return:
        """
        self.http_client.get_token()

    def init_thread(self):
        """
        Inicio un hilo para el método self.recv()
        :return:
        """
        if not self.active:
            self.active = True
            self.t = Thread(target=self.recv)
            self.t.start()
            logger.info('Manager started. %s ' % self.t.getName())

    def send(self, event, *args):
        """
        Según el event.topic, mapear a un endpoint.
        :param event:     event
        :param args:      No se usa, está solamente para completar (polimorfismo).
        :return:
        """
        marketdata_type = {
            '0': 'aggregated',
            '1': 'disaggregated',
            '2': 'top'
        }
        # Lo vuelvo a convertir a dict porque necesito saber su topic.
        event = json.loads(event)

        # Me el tipo de topic. Si es market_data_request, mando un mensaje de suscripcion.
        if event['topic'] == 'market_data_request_event':
            md_type = marketdata_type[str(event['md_type'])]
            for symbol in event['symbols']:
                self.subscribe(market=event['market'], symbol=symbol, md_type=md_type)
        else:
            # Agrego el uuid al evento, para los mensajes de order routing.
            event.update({
                'uuid': self.ws_uuid,
                'param_account_id': self.param_account_id,
                'param_negagent_id': self.param_negagent_id
            })
            if event['topic'] in self.endpoint_dict.keys():
                endpoint = self.endpoint_dict[event['topic']]
                response = self.http_client.post(endpoint, event)
                logger.info(response)
            else:
                logger.info('Not relevant event: %s ' % event['topic'])

    def recv(self):
        """
        Recibo mensaje desde el exterior.
        Lee mensajes desde una queue instanciada.
        """
        while self.active:
            event = self.queue.get()['payload']
            self.process_message(event)

    def subscribe(self, market=None, symbol=None, md_type=None, queue_name=None, routing_key=None):
        """
        Reemplaza al método add_queue.
        queue_name y routing_key no se usan en éste método.

        :param market:      Nombre del mercado.
        :param symbol:      Ticker del Instrumento.
        :param md_type:     Tipo de marketdata (top | aggregated | disaggregated)
        :param queue_name:  [No Usar].
        :param routing_key: [No Usar].
        :return:
        """
        # Armo el payload para el websocket.
        payload = {
            'market': market,
            'symbol': symbol,
            'md_type': md_type
        }
        # Envío mensaje de suscripción al websocket.
        self.ws_md_client.md_subscribe(payload)

    def unsubscribe(self, market=None, symbol=None, md_type=None, queue_name=None, routing_key=None):
        """
        Desuscripción de un instrumento dado. Todo: Ver si es necesario..
        :param market:      Nombre del mercado.
        :param symbol:      Ticker del Instrumento.
        :param md_type:     Tipo de marketdata (top | aggregated | disaggregated)
        :param queue_name:  [No Usar].
        :param routing_key: [No Usar].
        :return:
        """
        # Armo el payload para el websocket.
        payload = {
            'market': market,
            'symbol': symbol,
            'md_type': md_type
        }
        # Envío mensaje de suscripción al websocket.
        self.ws_md_client.md_unsubscribe(payload)

    def register_callback(self, topic, method):
        """
        Registra callbacks en un dict().
        :return:
        """
        self.callbacks[topic] = method

    def get_account(self):
        return self.account

    def process(self, evt):
        if self.callbacks.__contains__(evt.topic):
            self.callbacks[evt.topic](evt)
        else:
            print("No callback defined for %s" % evt.topic)

    def process_message(self, evt):
        try:
            if evt is not None:
                logger.info("Event Received %s" % evt)
                event_mapper = EventMapper()
                evt = event_mapper[evt['topic']](**evt)
                self.process(evt)
        except KeyError:
            logger.error(evt)
            logger.exception("Unknow event topic")
        except Exception as e:
            logger.error(str(evt))
            logger.exception(e)
        finally:
            pass

    def _set_account(self):
        system_accounts_reponse = self.http_client.get('system_account/', None)
        system_accounts = json.loads(system_accounts_reponse.text)
        for system_account in system_accounts:
            for negotiation_account in system_account.get('negotiation_accounts'):
                if negotiation_account.get('negotiation_agent').get('description') == 'Remarket':
                    self.param_account_id = system_account.get('id')
                    self.param_negagent_id = negotiation_account.get('negotiation_agent').get('id')
                    self.account = system_account.get('alias')
                    break
            break
        if self.param_account_id is None or self.param_negagent_id is None:
            raise Exception('Your user cannot run the development lib. Contact support.')



if __name__ == "__main__":
    pass
