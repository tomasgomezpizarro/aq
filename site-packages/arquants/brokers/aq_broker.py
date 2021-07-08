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
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections
import json
import threading
import uuid
from datetime import date, datetime, timedelta

from aq_lib.model.exec_type import ExecType
from aq_lib.model.order_status import OrderStatus
from aq_lib.utils.logger import logger
from backtrader import (num2date, BrokerBase, Order, OrderBase)
from backtrader.comminfo import CommInfoBase
from backtrader.utils.py3 import bytes, bstr, with_metaclass, queue

from arquants.definitions import aqhandler
from arquants import eventFactory
from arquants.stores import aq_store

bytes = bstr  # py2/3 need for ibpy


class Order(OrderBase):
    """Subclasses the IBPy order to provide the minimum extra functionality
    needed to be compatible with the internally defined orders

    Once ``OrderBase`` has processed the parameters, the __init__ method takes
    over to use the parameter values and set the appropriate values in the
    ib.ext.Order.Order object

    Any extra parameters supplied with kwargs are applied directly to the
    ib.ext.Order.Order object, which could be used as follows::

      Example: if the 4 order execution types directly supported by
      ``backtrader`` are not enough, in the case of for example
      *Interactive Brokers* the following could be passed as *kwargs*::

        orderType='LIT', lmtPrice=10.0, auxPrice=9.8

      This would override the settings created by ``backtrader`` and
      generate a ``LIMIT IF TOUCHED`` order with a *touched* price of 9.8
      and a *limit* price of 10.0.

    This would be done almost always from the ``Buy`` and ``Sell`` methods of
    the ``Strategy`` subclass being used in ``Cerebro``
    """

    def __str__(self):
        """Get the printout from the base class and add some ib.Order specific
        fields"""
        basetxt = super(Order, self).__str__()
        tojoin = [basetxt]
        tojoin.append('Ref: {}'.format(self.ref))
        tojoin.append('orderId: {}'.format(self.m_orderId))
        tojoin.append('Action: {}'.format(self.m_action))
        tojoin.append('Size (ib): {}'.format(self.m_totalQuantity))
        tojoin.append('Lmt Price: {}'.format(self.m_lmtPrice))
        tojoin.append('Aux Price: {}'.format(self.m_auxPrice))
        tojoin.append('OrderType: {}'.format(self.m_orderType))
        tojoin.append('Tif (Time in Force): {}'.format(self.m_tif))
        tojoin.append('GoodTillDate: {}'.format(self.m_goodTillDate))
        return '\n'.join(tojoin)

    # Map backtrader order types to the ib specifics
    _IBOrdTypes = {
        None: bytes('MKT'),  # default
        Order.Market: bytes('MKT'),
        Order.Limit: bytes('LMT'),
        Order.Close: bytes('MOC'),
        Order.Stop: bytes('STP'),
        Order.StopLimit: bytes('STPLMT'),
        Order.StopTrail: bytes('TRAIL'),
        Order.StopTrailLimit: bytes('TRAIL LIMIT'),
    }

    (MarketToLimit) = range(8, 9)

    def __init__(self, action, **kwargs):

        # Marker to indicate an openOrder has been seen with
        # PendinCancel/Cancelled which is indication of an upcoming
        # cancellation

        self._willexpire = False

        self.ordtype = self.Buy if action == 'BUY' else self.Sell

        super(Order, self).__init__()

        # Now fill in the specific IB parameters
        self.m_orderType = self._IBOrdTypes[self.exectype]
        self.m_permid = 0

        # 'B' or 'S' should be enough
        self.m_action = bytes(action)

        # Set the prices
        self.m_lmtPrice = 0.0
        self.m_auxPrice = 0.0

        if self.exectype == self.Market:  # is it really needed for Market?
            pass
        elif self.exectype == self.Close:  # is it ireally needed for Close?
            pass
        elif self.exectype == self.Limit:
            self.m_lmtPrice = self.price
        elif self.exectype == self.Stop:
            self.m_auxPrice = self.price  # stop price / exec is market
        elif self.exectype == self.StopLimit:
            self.m_lmtPrice = self.pricelimit  # req limit execution
            self.m_auxPrice = self.price  # trigger price
        elif self.exectype == self.StopTrail:
            if self.trailamount is not None:
                self.m_auxPrice = self.trailamount
            elif self.trailpercent is not None:
                # value expected in % format ... multiply 100.0
                self.m_trailingPercent = self.trailpercent * 100.0
        elif self.exectype == self.StopTrailLimit:
            self.m_trailStopPrice = self.m_lmtPrice = self.price
            # The limit offset is set relative to the price difference in TWS
            self.m_lmtPrice = self.pricelimit
            if self.trailamount is not None:
                self.m_auxPrice = self.trailamount
            elif self.trailpercent is not None:
                # value expected in % format ... multiply 100.0
                self.m_trailingPercent = self.trailpercent * 100.0

        self.m_totalQuantity = self.abs_size = abs(self.size)  # ib takes only positives

        self.m_transmit = self.transmit
        if self.parent is not None:
            self.m_parentId = self.parent.m_orderId

        # Time In Force: DAY, GTC, IOC, GTD
        if self.valid is None:
            tif = 'GTC'  # Good til cancelled
        elif isinstance(self.valid, (datetime, date)):
            tif = 'GTD'  # Good til date
            self.m_goodTillDate = bytes(self.valid.strftime('%Y%m%d %H:%M:%S'))
        elif isinstance(self.valid, (timedelta,)):
            if self.valid == self.DAY:
                tif = 'DAY'
            else:
                tif = 'GTD'  # Good til date
                valid = datetime.now() + self.valid  # .now, using localtime
                self.m_goodTillDate = bytes(valid.strftime('%Y%m%d %H:%M:%S'))

        elif self.valid == 0:
            tif = 'DAY'
        else:
            tif = 'GTD'  # Good til date
            valid = num2date(self.valid)
            self.m_goodTillDate = bytes(valid.strftime('%Y%m%d %H:%M:%S'))

        self.m_tif = bytes(tif)

        # OCA
        self.m_ocaType = 1  # Cancel all remaining orders with block

        # pass any custom arguments to the order
        for k in kwargs:
            setattr(self, (not hasattr(self, k)) * 'm_' + k, kwargs[k])


class AQCommInfo(CommInfoBase):
    """
    Commissions are calculated by ib, but the trades calculations in the
    ```Strategy`` rely on the order carrying a CommInfo object attached for the
    calculation of the operation cost and value.

    These are non-critical informations, but removing them from the trade could
    break existing usage and it is better to provide a CommInfo objet which
    enables those calculations even if with approvimate values.

    The margin calculation is not a known in advance information with IB
    (margin impact can be gotten from OrderState objects) and therefore it is
    left as future exercise to get it"""

    def getvaluesize(self, size, price):
        # In real life the margin approaches the price
        return abs(size) * price

    def getoperationcost(self, size, price):
        """Returns the needed amount of cash an operation would cost"""
        # Same reasoning as above
        return abs(size) * price


class MetaAQBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        """Class has already been created ... register"""
        # Initialize the class
        super(MetaAQBroker, cls).__init__(name, bases, dct)
        aq_store.AQStore.BrokerCls = cls


class AQBroker(with_metaclass(MetaAQBroker, BrokerBase)):
    """Broker implementation for Interactive Brokers.

    This class maps the orders/positions from Interactive Brokers to the
    internal API of ``backtrader``.

    Notes:

      - ``tradeid`` is not really supported, because the profit and loss are
        taken directly from IB. Because (as expected) calculates it in FIFO
        manner, the pnl is not accurate for the tradeid.

      - Position

        If there is an open position for an asset at the beginning of
        operaitons or orders given by other means change a position, the trades
        calculated in the ``Strategy`` in cerebro will not reflect the reality.

        To avoid this, this broker would have to do its own position
        management which would also allow tradeid with multiple ids (profit and
        loss would also be calculated locally), but could be considered to be
        defeating the purpose of working with a live broker
    """
    params = ()

    def __init__(self, **kwargs):
        super(AQBroker, self).__init__()

        self.ib = aq_store.AQStore(**kwargs)

        self.startingcash = self.cash = 0.0
        self.startingvalue = self.value = 0.0

        self._lock_orders = threading.Lock()  # control access
        self._lock_ers = threading.Lock()  # control access
        self.orderbyid = dict()  # orders by order id
        self.executions = dict()  # notified executions
        self.ordstatus = collections.defaultdict(dict)
        self.notifs = queue.Queue()  # holds orders which are notified
        self.tonotify = collections.deque()  # hold oids to be notified
        self.ers = queue.Queue()

    def start(self):
        super(AQBroker, self).start()
        self.ib.start(broker=self)

        # if self.ib.connected():
        #   self.ib.reqAccountUpdates()
        #  self.startingcash = self.cash = self.ib.get_acc_cash()
        # self.startingvalue = self.value = self.ib.get_acc_value()
        # else:
        self.startingcash = self.cash = 0.0
        self.startingvalue = self.value = 0.0

    def stop(self):
        super(AQBroker, self).stop()
        self.ib.stop()

    def getcash(self):
        # This call cannot block if no answer is available from ib
        # self.cash = self.ib.get_acc_cash()
        return self.cash

    def getvalue(self, datas=None):
        # self.value = self.ib.get_acc_value()
        return self.value

    def getposition(self, data, clone=True):
        return self.ib.getposition(data.tradecontract, clone=clone)

    def manual_cancel(self, order_id):
        try:
            o = self.orderbyid[order_id]
            self.cancel(o)
        except (ValueError, KeyError):
            logger.debug("manual_cancel: unknown order {}".format(order_id))
            return  # not found ... not cancellable

    def cancel(self, order):
        try:
            self.orderbyid[order.m_orderId]
        except (ValueError, KeyError):
            logger.debug("cancel: unknown order {}".format(order.m_orderId))
            return  # not found ... not cancellable
        if order.status == Order.Cancelled:  # already cancelled
            return
        self.ib.cancel_order(order)

    def mass_cancel(self, routing_key=None, segment=None, account=None):
        self.ib.mass_cancel(routing_key=routing_key, segment=segment, account=account)

    def orderstatus(self, order):
        try:
            o = self.orderbyid[order.m_orderId]
        except (ValueError, KeyError):
            o = order

        return o.status

    def send_bulk_new_orders(self, orders):
        self.ib.send_bulk_new_orders(orders)

    def submit(self, order):
        order.submit(self)

        # ocoize if needed. One cancel the other order
        if order.oco is None:  # Generate a UniqueId
            order.m_ocaGroup = bytes(uuid.uuid4())
        else:
            order.m_ocaGroup = self.orderbyid[order.oco.m_orderId].m_ocaGroup

        self.orderbyid[order.m_orderId] = order
        self.ib.place_order(order)
        self.notify(order)

        return order

    @staticmethod
    def getcommissioninfo(data):
        # contract = data.tradecontract
        # try:
        #   mult = float(contract.m_multiplier)
        # except (ValueError, TypeError):
        mult = 1.0
        # stocklike = contract.m_secType not in ('FUT', 'OPT', 'FOP',)

        return AQCommInfo(mult=mult, stocklike=True)

    def _makeorder(self, action, owner, data,
                   size, price=None, plimit=None,
                   exectype=None, valid=None,
                   tradeid=0, **kwargs):

        order = Order(action, owner=owner, data=data,
                        size=size, price=price, pricelimit=plimit,
                        exectype=exectype, valid=valid,
                        tradeid=tradeid,
                        clientId=self.ib.clientId,
                        orderId=self.ib.generate_id(),
                        **kwargs)

        order.addcomminfo(self.getcommissioninfo(data))
        return order

    def buy(self, owner, data,
            size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0,
            **kwargs):

        order = self._makeorder(
            'BUY',
            owner, data, size, price, plimit, exectype, valid, tradeid,
            **kwargs)

        return self.submit(order)

    def sell(self, owner, data,
             size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0,
             **kwargs):

        order = self._makeorder(
            'SELL',
            owner, data, size, price, plimit, exectype, valid, tradeid,
            **kwargs)

        return self.submit(order)

    def replace(self, owner, size, price=None, order=None, **kwargs):
        neworder = self._makeorder('BUY' if order.ordtype == 0 else 'SELL', owner, order.data, size, price,
                                   exectype=order.exectype, originalOrderId=order.m_serverOrderId,
                                   originalClientOrderId=order.m_orderId, **kwargs)
        return self.submit(neworder)

    def notify(self, order):
        self.notifs.put(order.clone())

    def get_notification(self):
        try:
            return self.notifs.get(False)
        except queue.Empty:
            pass

        return None

    def next(self):
        self.notifs.put(None)  # mark notificatino boundary

    def update_order(self, order, ex):
        notify = False
        if ex.orderStatus == OrderStatus.New.value:
            if order.status != Order.Accepted:
                order.m_serverOrderId = ex.orderID
                order.accept(self)
                notify = True
        elif ex.orderStatus == OrderStatus.Canceled.value:
            if order.status != Order.Canceled:
                order.cancel()
                order.m_serverOrderId = ex.orderID
                notify = True
        elif ex.orderStatus == OrderStatus.Rejected.value:
            if order.status != Order.Rejected:
                order.reject()
                notify = True
        elif ex.orderStatus == OrderStatus.Replaced.value:
            order.m_serverOrderId = ex.orderID
            order.cancel()
            notify = True
        elif ex.orderStatus == OrderStatus.Expired.value:
            if order.status != Order.Expired:
                order.m_serverOrderId = ex.orderID
                order.expire()
                notify = True
        elif ex.orderStatus == OrderStatus.PartiallyFilled.value:
            if order.status != Order.Partial:
                order.m_serverOrderId = ex.orderID
                order.partial()
                notify = True
        elif ex.orderStatus == OrderStatus.Filled.value:
            if order.status != Order.Completed:
                order.m_serverOrderId = ex.orderID
                order.completed()
                notify = True
        if notify:
            self.notify(order)
        # En varios casos queda pendiente actualizar la posicion, pero eso va a ser algo que se va a realizar
        # en el modulo de riesgo

    def push_execution(self, ex):
        with self._lock_orders:
            order = None
            original_order = None
            try:
                order = self.orderbyid[ex.clientOrderID]
            except KeyError:
                logger.debug("Debe ser un cancel")

            if ex.execType in [ExecType.Canceled.value]:
                if ex.originalClientOrderId is not None:
                    original_order = self.orderbyid[ex.originalClientOrderId]
                    original_order.cancel()
                    self.notify(original_order)
                elif order is not None:
                    order.cancel()
                    self.notify(order)
            elif ex.execType in [ExecType.Replaced.value]:
                original_order = self.orderbyid[ex.originalClientOrderId]
                original_order.cancel()
                if ex.orderStatus in (OrderStatus.New.value, OrderStatus.PartiallyFilled.value):
                    order.m_serverOrderId = ex.orderID
                    order.accept(self)
                    self.notify(order)
                self.notify(original_order)

            elif ex.execType == ExecType.New.value:
                if not order.status == Order.Accepted:  # Duplicate detection
                    order.m_serverOrderId = ex.orderID
                    order.accept(self)
                    self.notify(order)

            elif ex.execType == ExecType.Rejected.value:
                if order is not None:
                    order.reject()
                    self.notify(order)
                else:
                    logger.warning("Received an OrderCancelReject - clientOrderId %s" % ex.clientOrderID)
            elif ex.execType == ExecType.Trade.value:
                order.m_serverOrderId = ex.orderID
                position = self.getposition(order.data, clone=False)
                # pprice_orig = position.price
                size = ex.lastQty if int(ex.side) == 1 else -ex.lastQty
                price = ex.lastPrice
                # use pseudoupdate and let the updateportfolio do the real update?
                psize, pprice, opened, closed = position.update(size, price)
                # split commission between closed and opened
                comm = 0
                closedcomm = comm * closed / size
                openedcomm = comm - closedcomm

                # comminfo = order.comminfo
                # closedvalue = comminfo.getoperationcost(closed, pprice_orig)
                # openedvalue = comminfo.getoperationcost(opened, price)

                # default in m_pnl is MAXFLOAT
                # pnl = ex.m_realizedPNL if closed else 0.0
                pnl = 0
                # The internal br
                # Use the actual time provided by the execution object
                # The report from TWS is in actual local time, not the data's tz
                dt = ex.transactTime

                # Need to simulate a margin, but it plays no role, because it is
                # controlled by a real broker. Let's set the price of the item
                margin = order.data.close[0]
                order.tradeid = ex.execID
                order.execute(dt, size, price,
                              closed, 0, closedcomm,
                              opened, 0, openedcomm,
                              margin, pnl,
                              psize, pprice)
                if ex.orderStatus == OrderStatus.Filled.value:
                    order.completed()
                elif ex.orderStatus == OrderStatus.PartiallyFilled.value:
                    if order.status != Order.Completed or order.staus not in (Order.Canceled, Order.Cancelled):
                        order.partial()
                self.notify(order)
                self.trade_executed(ex)
            elif ex.execType == ExecType.OrderStatus.value:
                self.update_order(order, ex)

    def trade_executed(self, ex):
        aqhandler.send(json.dumps(eventFactory.createTradeExecutedEvent(ex).to_dict()), self.ib.get_delivery_rk())

    def cancel_all(self):
        for key in self.orderbyid:
            order = self.orderbyid[key]
            if order.status == Order.Accepted or order.status == Order.Partial:
                order.cancel()
                self.ib.cancel_order(order)
