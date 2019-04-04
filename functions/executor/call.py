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

import logging
import sys
import uuid
import zmq

import user_library

from anna.lattices import *
from include.functions_pb2 import *
from include.shared import *
from include.serializer import *
from include import server_utils as sutils
from . import utils

def exec_function(exec_socket, kvs, ip, tid, status):
    call = FunctionCall()
    call.ParseFromString(exec_socket.recv())
    logging.info('Received call for ' + call.name)

    if not status.running:
        sutils.error.error = INVALID_TARGET
        exec_socket.send(sutils.SerializeToString())
        return

    obj_id = str(uuid.uuid4())
    if not call.HasField('resp_id'):
        call.resp_id = obj_id
    else:
        obj_id = call.resp_id

    reqid = call.request_id
    fargs = _process_args(call.args)

    f = utils._retrieve_function(call.name, kvs)
    if not f:
        sutils.error.error = FUNC_NOT_FOUND
        exec_socket.send(sutils.error.SerializeToString())
        return

    resp = GenericResponse()
    resp.success = True
    resp.response_id = obj_id

    exec_socket.send(resp.SerializeToString())
    user_library.FluentUserLibrary(ip, tid, kvs)
    result = _exec_func(kvs, f, fargs)
    result = serialize_val(result)

    result_lattice = LWWPairLattice(generate_timestamp(0), result)
    kvs.put(obj_id, result_lattice)


def exec_dag_function(pusher_cache, kvs, ip, tid, trigger, function, schedule):
    fname = trigger.target_function
    logging.info('Executing function %s for DAG %s (ID %d).' %
            (schedule.dag.name, fname, trigger.id))

    fargs = list(schedule.arguments[fname].args) + list(trigger.arguments.args)
    fargs = _process_args(fargs)

    user_lib = user_library.FluentUserLibrary(ip, tid, kvs)
    result = _exec_func(function, fargs, user_lib)

    result_triggers = []

    is_sink = True
    for conn in schedule.dag.connections:
        if conn.source == fname:
            is_sink = False
            new_trigger = DagTrigger()
            new_trigger.id = trigger.id
            new_trigger.target_function = conn.sink

            if type(result) != tuple:
                result = (result,)

            al = new_trigger.arguments
            al.args.extend(list(map(lambda v: serialize_val(v, None, False),
                result)))

            dest_ip = schedule.locations[conn.sink]
            sckt = pusher_cache.get(sutils._get_dag_trigger_address(dest_ip))
            sckt.send(new_trigger.SerializeToString())

    if is_sink:
        logging.info('DAG %s (ID %d) completed; result at %s.' %
                (schedule.dag.name, trigger.id, schedule.response_id))
        l = LWWPairLattice(generate_timestamp(0), serialize_val(result))
        kvs.put(schedule.response_id, l)


def _process_args(arg_list):
    return [get_serializer(arg.type).load(arg.body) for arg in arg_list]


def _exec_func(func, args, user_lib):
    # First argument is the fluent library that user code can call.
    func_args = (user_lib)

    # resolve any references to KVS objects
    for arg in args:
        if isinstance(arg, FluentReference):
            func_args += (_resolve_ref(arg, kvs),)
        else:
            func_args += (arg,)

    # execute the function
    return  func(*func_args)

def _resolve_ref(ref, kvs):
    ref_data = kvs.get(ref.key, ref.obj_type)

    # when chaining function executions, we must wait
    while not ref_data:
        ref_data = kvs.get(ref.key, ref.obj_type)

    if ref.deserialize:
        ref_data = deserialize_val(ref_data.reveal()[1])

    return ref_data

