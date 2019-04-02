#  Copyright 2018 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import queue
import threading
import zmq

from anna.zmq_util import SocketCache
from include import server_utils

class AbstractFluentUserLibrary:
    # Stores a lattice value at ref.
    def put(self, ref, ltc):
        raise NotImplementedError

    # Retrives the lattice value at ref.
    def get(self, ref, ltype):
        raise NotImplementedError

    # Sends a bytestring message to the specified destination.
    # TODO: type and format for destination ID?
    def send(self, dest, bytestr):
        raise NotImplementedError

    # Receives messages sent by send() to this function.
    # Receives all outstanding messages as a list [(sender id, bytestring message), ...]
    def recv(self):
        raise NotImplementedError

class FluentUserLibrary(AbstractFluentUserLibrary):
    RECV_INBOX_PORT = 5500  # hopefully this is free? unsure if this should be declared elsewhere

    def __init__(self, anna_client):
        self.ctx = zmq.Context()
        self.send_socket_cache = SocketCache(self.ctx, zmq.REQ)

        # The Anna client, used for interfacing with the kvs.
        self.client = anna_client

        # Threadsafe queue to serve as this node's inbox.
        # Items are (sender string, message bytestring).
        # NB: currently unbounded in size.
        self.recv_inbox = queue.Queue()

        # Thread for receiving messages into our inbox.
        self.recv_inbox_thread = threading.Thread(target=self._recv_inbox_listener, args=(self,))
        self.recv_inbox_thread.start()

    def put(self, ref, ltc):
        return self.client.put(ref, ltc)

    def get(self, ref, ltype):
        return self.client.get(ref, ltype)

    # Provisional precondition:
    # dest is an IP address of another function executor, resolvable by ZMQ.
    # (If/when we change to node identifiers, we'll need to add an address resolution function.)
    def send(self, dest, bytestr):
        socket = self.send_socket_cache.get(dest)
        socket.send_pyobj((dest, bytestr))

    def recv(self):
        res = []
        while True:
            try:
                (sender, msg) = self.recv_inbox.get()
                res.append((sender, msg))
            except queue.Empty:
                break
        return res


    # Function that continuously listens for send()s sent by other nodes,
    # and stores the messages in an inbox.
    def _recv_inbox_listener(self):
        # Socket for receiving send() messages from other nodes.
        recv_inbox_socket = self.ctx.socket.(zmq.DEALER)
        recv_inbox_socket.bind(server_utils.BIND_ADDR_TEMPLATE % (RECV_INBOX_PORT))

        while True:
            (sender, msg) = recv_inbox_socket.recv_pyobj(0, copy=True)  # Blocking.
            self.recv_inbox.put((sender, msg))
