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
    # TODO: there's two choices of semantics here:
    # receive next message (blocking if there are none),
    # or receive all outstanding messages (returning nothing if there are none).
    # Messages will be (sender id, bytestring message).
    def recv(self):
        raise NotImplementedError

class FluentUserLibrary(AbstractFluentUserLibrary):
    def __init__(self, anna_client):
        self.client = anna_client

    def put(self, ref, ltc):
        return self.client.put(ref, ltc)

    def get(self, ref, ltype):
        return self.client.get(ref, ltype)
