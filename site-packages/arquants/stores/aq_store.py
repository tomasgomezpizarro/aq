#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015, 2016, 2017 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function, unicode_literals)

import collections
import itertools
import json
import random
import threading
import uuid
from copy import copy
from datetime import datetime, timedelta
import traceback
from aq_lib.event.event_type import EventType
from aq_lib.model.md_subscription_type import MDSubscriptionType
from aq_lib.utils.logger import logger
from backtrader.metabase import MetaParams
from backtrader.utils import AutoDict
from backtrader.utils.py3 import bstr, queue, with_metaclass, long
from arquants.definitions import aqhandler
from arquants.event.event_factory import eventFactory
from arquants.observers.position_calculator import PositionCalculator

bytes = bstr  # py2/3 need for ibpy


def _ts2dt(tstamp=None):
    # Transforms a RTVolume timestamp to a datetime object
    if not tstamp:
        return datetime.utcnow()

    sec, msec = divmod(long(tstamp), 1000)
    usec = msec * 1000
    return datetime.utcfromtimestamp(sec).replace(microsecond=usec)


class RTVolume(object):
    """Parses a tickString tickType 48 (RTVolume) event from the IB API into its
    constituent fields

    Supports using a "price" to simulate an RTVolume from a tickPrice event
    """
    _fields = [
        ('price', float),
        ('size', int),
        ('datetime', _ts2dt),
        ('volume', int),
        ('vwap', float),
        ('single', bool)
    ]

    def __init__(self, rtvol='', price=None, tmoffset=None):
        # Use a provided string or simulate a list of empty tokens
        tokens = iter(rtvol.split(';'))

        # Put the tokens as attributes using the corresponding func
        for name, func in self._fields:
            setattr(self, name, func(next(tokens)) if rtvol else func())

        # If price was provided use it
        if price is not None:
            self.price = price

        if tmoffset is not None:
            self.datetime += tmoffset


class MetaSingleton(MetaParams):
    """Metaclass to make a metaclassed class a singleton"""

    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton


class AQStore(with_metaclass(MetaSingleton, object)):
    """Singleton class wrapping an ibpy ibConnection instance.

    The parameters can also be specified in the classes which use this store,
    like ``IBData`` and ``IBBroker``

    Params:

      - ``notifyall`` (default: ``False``)

        If ``False`` only ``error`` messages will be sent to the
        ``notify_store`` methods of ``Cerebro`` and ``Strategy``.

        If ``True``, each and every message received from TWS will be notified

      - ``_debug`` (default: ``False``)

        Print all messages received from TWS to standard output

      - ``timeoffset`` (default: ``True``)

        If True, the time obtained from ``reqCurrentTime`` (IB Server time)
        will be used to calculate the offset to localtime and this offset will
        be used for the price notifications (tickPrice events, for example for
        CASH markets) to modify the locally calculated timestamp.

        The time offset will propagate to other parts of the ``backtrader``
        ecosystem like the **resampling** to align resampling timestamps using
        the calculated offset.

      - ``timerefresh`` (default: ``60.0``)

        Time in seconds: how often the time offset has to be refreshed

    """

    # Set a base for the data requests (historical/realtime) to distinguish the
    # id in the error notifications from orders, where the basis (usually
    # starting at 1) is set by TWS
    REQIDBASE = 0x01000000

    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will auto register

    datas_class = dict()

    params = (
        ('notifyall', False),
        ('_debug', False),
        ('clientId', None),
        ('timeoffset', True),  # Use offset to server for timestamps if needed
        ('timerefresh', 60.0),  # How often to refresh the timeoffset
    )

    @classmethod
    def getdata(cls, *args, **kwargs):
        """Returns ``DataCls`` with args, kwargs"""
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getdataclass(cls, _type, *args, **kwargs):
        return cls.datas_class[_type]

    @classmethod
    def getbroker(cls, *args, **kwargs):
        """Returns broker with *args, **kwargs from registered ``BrokerCls``"""
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self, async_queue):
        super(AQStore, self).__init__()
        self.queue = async_queue

        self._lock_q = threading.Lock()  # sync access to _tickerId/Queues
        self._lock_accupd = threading.Lock()  # sync account updates       #TODO Definir si es necesario
        self._lock_pos = threading.Lock()  # sync account updates          #TODO Definir si es necesario
        self._lock_notif = threading.Lock()  # sync access to notif queue

        # Account list received
        self._event_managed_accounts = threading.Event()  # TODO Definir si es necesario
        self._event_accdownload = threading.Event()  # TODO Definir si es necesario

        self._env = None  # reference to cerebro for general notifications
        self.broker = None  # broker instance
        self.datas = list()  # datas that have registered over start
        self.ccount = 0  # requests to start (from cerebro or datas)       #TODO Definir si es necesario

        self._lock_tmoffset = threading.Lock()  # TODO Definir si es necesario
        self.tmoffset = timedelta()  # to control time difference with server    #TODO Definir si es necesario

        # Structures to hold datas requests
        self.qs = collections.OrderedDict()  # key: tickerId -> queues
        self.ts = collections.OrderedDict()  # key: queue -> tickerId
        self.subsbysymbol = collections.OrderedDict()  # key: symbol -> tickerIds

        self.iscash = dict()  # tickerIds from cash products (for ex: EUR.JPY)

        self.histexreq = dict()  # holds segmented historical requests
        self.histfmt = dict()  # holds datetimeformat for request
        self.histsend = dict()  # holds sessionend (data time) for request
        self.histtz = dict()  # holds sessionend (data time) for request

        self.acc_cash = AutoDict()  # current total cash per account
        self.acc_value = AutoDict()  # current total value per account
        self.acc_upds = AutoDict()  # current account valueinfos per account

        self.port_update = False  # indicate whether to signal to broker

        self.positions = collections.defaultdict(PositionCalculator)  # actual positions

        self._tickerId = itertools.count(self.REQIDBASE)  # unique tickerIds
        self.orderid = None  # next possible orderid (will be itertools.count)

        self.cdetails = collections.defaultdict(list)  # hold cdetails requests

        self.managed_accounts = list()  # received via managedAccounts

        self.notifs = queue.Queue()  # store notifications for cerebro

        # This utility key function transforms a barsize into a:
        #   (Timeframe, Compression) tuple which can be sorted
        def keyfn(x):
            n, t = x.split()
            tf, comp = self._sizes[t]
            return tf, int(n) * comp

        # This utility key function transforms a duration into a:
        #   (Timeframe, Compression) tuple which can be sorted
        def key2fn(x):
            n, d = x.split()
            tf = self._dur2tf[d]
            return tf, int(n)

        aqhandler.register_callback(EventType.MARKET_DATA_SNAPSHOT_FULL_REFRESH_EVENT.value, self.md_callback)
        aqhandler.register_callback(EventType.EXECUTION_REPORT_EVENT.value, self.exec_details)
        # self.routing_key = definitions.configuration.get_property("RABBITMQ", "OutcomeEventRoutingKey")
        if self.p.clientId is None:
            self.clientId = random.randint(1, pow(2, 16) - 1)
        else:
            self.clientId = self.p.clientId

    def start(self, data=None, broker=None):
        if data is not None:
            self._env = data._env
            self.datas.append(data)
            return self.req_mkt_data(data)
        elif broker is not None:
            self.broker = broker

    # TODO Definir que debe hacer este metodo
    def stop(self):
        # Unblock any calls set on these events
        self._event_managed_accounts.set()
        self._event_accdownload.set()

    def logmsg(self, *args):
        # for logging purposes
        if self.p._debug:
            logger.debug(*args)

    # TODO Definir que debe hacer este metodo
    def watcher(self, msg):
        # will be registered to see all messages if debug is requested
        self.logmsg(str(msg))
        if self.p.notifyall:
            self.notifs.put((msg, tuple(msg.values()), dict(msg.items())))

    # TODO Chequear si esto debe hacerse para todas las datas juntas
    def startdatas(self):
        # kickstrat datas, not returning until all of them have been done
        ts = list()
        for data in self.datas:
            t = threading.Thread(target=data.reqdata)
            t.start()
            ts.append(t)

        for t in ts:
            t.join()

    # TODO Chequear si esto debe hacerse para todas las datas juntas
    def stopdatas(self):
        # stop subs and force datas out of the loop (in LIFO order)
        qs = list(self.qs.values())
        ts = list()
        for data in self.datas:
            t = threading.Thread(target=data.canceldata)
            t.start()
            ts.append(t)

        for t in ts:
            t.join()

        for q in reversed(qs):  # datamaster the last one to get a None
            q.put(None)

    # TODO Ver quien usa este metodo y si es necesario
    def get_notifications(self):
        """Return the pending "store" notifications"""
        # The background thread could keep on adding notifications. The None
        # mark allows to identify which is the last notification to deliver
        self.notifs.put(None)  # put a mark
        notifs = list()
        while True:
            notif = self.notifs.get()
            if notif is None:  # mark is reached
                break
            notifs.append(notif)

        return notifs

    def get_ticker_queue(self, data_type, contract):
        q = queue.Queue(maxsize=1)
        with self._lock_q:
            symbol = ''.join(e for e in contract if e.isalnum())
            ticker_id = "MD-" + str(self.generate_id())
            key = data_type + "-" + symbol
            if not self.subsbysymbol.__contains__(key):
                self.subsbysymbol[key] = list()
            self.subsbysymbol[key].append(ticker_id)
            self.qs[ticker_id] = q
            self.ts[q] = ticker_id
        return ticker_id, q

    def cancel_queue(self, q, sendnone=False):
        """Cancels a Queue for data delivery"""
        # pop ts (tickers) and with the result qs (queues)
        ticker_id = self.ts.pop(q, None)
        self.qs.pop(ticker_id, None)

        self.iscash.pop(ticker_id, None)

        if sendnone:
            q.put(None)

    def valid_queue(self, q):
        """Returns (bool)  if a queue is still valid"""
        return q in self.ts  # queue -> ticker

    def req_mkt_data(self, data):
        md_request_id, q = self.get_ticker_queue(data.data_type, data.tradecontract)
        self.publish_market_data_request(data, md_request_id)
        return q

    @staticmethod
    def publish_market_data_request(data, md_request_id):
        logger.debug("Sending MD request - Symbol: %s - MDRequestID: %s" % (data.tradecontract, md_request_id))
        md_type = None
        if data.data_type is 'aggregated':
            md_type = MDSubscriptionType.AGGREGATED.value
        elif data.data_type is 'disaggregated':
            md_type = MDSubscriptionType.DISAGGREGATED.value
        elif data.data_type is 'top':
            md_type = MDSubscriptionType.TOP.value

        a = json.dumps(eventFactory.createMarketDataRequestEvent(contract=data.tradecontract,
                                                                 settlement_date=data.settlement_date,
                                                                 security_type=data.security_type,
                                                                 currency=data.currency,
                                                                 settlement=data.settlement,
                                                                 md_request_id=md_request_id,
                                                                 data_type=md_type,
                                                                 market=data.market,
                                                                 negotiation_agent_id=data.negotiation_agent_id
                                                                 ).__dict__)
        aqhandler.send(a, data.routingkey)

    def cancel_mkt_data(self, q):
        """Cancels an existing MarketData subscription

        Params:
          - q: the Queue returned by reqMktData
        """
        with self._lock_q:
            ticker_id = self.ts.get(q, None)
            if ticker_id is not None:
                logger.debug("TODO: Aca hay que enviar un evento al bus para desubscribirse")
            self.cancel_queue(q, True)

    def md_callback(self, md):

        logger.info("Llegó MD")
        s = ''.join(e for e in md.symbol if e.isalnum())
        if md.md_type is MDSubscriptionType.DISAGGREGATED.value:
            prefix = 'disaggregated-'
        elif md.md_type is MDSubscriptionType.AGGREGATED.value:
            prefix = 'aggregated-'
        elif md.md_type is MDSubscriptionType.TOP.value:
            prefix = 'top-'
        symbol = prefix + s
        if self.subsbysymbol.__contains__(symbol):
            keys = self.subsbysymbol[symbol]
            for key in keys:
                if self.qs.__contains__(key):
                    if self.qs[key].full():
                        try:
                            self.qs[key].get(block=False)
                        except queue.Empty:
                            pass
                    self.qs[key].put(md)
                    # Disparar señal para activar cerebro
                    self.queue.put('Llego algo')
                else:
                    logger.debug("The queue doesn't have the symbol %s" % key)
        else:
            logger.debug("Doesn't exists a subscription for this symbol")

    @staticmethod
    def cancel_order(order):
        try:
            aqhandler.send(json.dumps(eventFactory.createCancelOrderEventFromOrder(order).to_dict()),
                           order.data.routingkey)
        except Exception as e:
            just_the_string = traceback.format_exc()
            logger.debug(just_the_string)
            logger.exception("cancel_order: Error processing order cancel", e)

    def mass_cancel(self, routing_key=None, segment=None, account=None):
        evt = eventFactory.createOrderMassCancelRequestEvent(segment=segment, account=account,
                                                             clientOrderId=self.generate_id()).to_dict()
        aqhandler.send(json.dumps(evt), routing_key)

    # TODO esto deberia soportar multiples mercados. Va por el lado de la routing key
    def send_bulk_new_orders(self, orders):
        aqhandler.send(json.dumps(eventFactory.createBulkNewOrderEvent(orders).to_dict()), orders[0].data.routingkey)
        logger.info("send_bulk_new_orders: publishing orders in bulk")
        for order in orders:
            aqhandler.send(json.dumps(eventFactory.createOrderUpdatedEventFromOrder(order).to_dict()),
                           order.data.routingkey)

    @staticmethod
    def place_order(order):
        if getattr(order, 'm_send', True):
            if hasattr(order, 'm_originalClientOrderId'):
                aqhandler.send(json.dumps(eventFactory.createOrderCancelReplaceEvent(order).to_dict()),
                               order.data.routingkey)
            else:
                aqhandler.send(json.dumps(eventFactory.createNewOrderEventFromOrder(order).to_dict()),
                               order.data.routingkey)

    def exec_details(self, msg):
        try:
            self.broker.push_execution(msg)
            self.queue.put('exec_details: received execution report ')
        except Exception as e:
            just_the_string = traceback.format_exc()
            logger.debug(just_the_string)
            logger.exception("exec_details: exception processing execution report", e)

    # TODO Deberia transformarse en un metodo que setea la posicion inicial a partir de lo que envia ROFEX
    def update_portfolio(self, msg):
        # Lock access to the position dicts. This is called in sub-thread and
        # can kick in at any time
        with self._lock_pos:
            if not self._event_accdownload.is_set():  # 1st event seen
                position = PositionCalculator()
                self.positions[msg.contract.m_conId] = position
            else:
                position = self.positions[msg.contract.m_conId]
                if not position.fix(msg.position, msg.averageCost):
                    err = ('The current calculated position and '
                           'the position reported by the broker do not match. '
                           'Operation can continue, but the trades '
                           'calculated in the strategy may be wrong')

                    self.notifs.put((err, (), {}))

                # Flag signal to broker at the end of account download
                # self.port_update = True
                self.broker.push_portupdate()

    def getposition(self, contract, clone=False):
        # Lock access to the position dicts. This is called from main thread
        # and updates could be happening in the background
        with self._lock_pos:
            position = self.positions[contract]
            if clone:
                return copy(position)

            return position

    def get_delivery_rk(self):
        return self._env.delivery_routing_key

    @staticmethod
    def generate_id():
        cl_ord_id = uuid.uuid4().hex[:15]
        return cl_ord_id
