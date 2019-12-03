#!/usr/bin/env python3.6

import logging
import sys

# IPython imports
from ipykernel.ipkernel import IPythonKernel

import client as flclient




class SLIPKernel(IPythonKernel):
    # Kernel info fields
    implementation = 'Fluent IPython'
    implementation_version = '0.1'
    language_info = {
        'name': 'Fluent IPython',
        'version': sys.version.split()[0],
        'mimetype': 'text/x-python',
        'codemirror_mode': {
            'name': 'ipython',
            'version': sys.version_info[0]
        },
        'pygments_lexer': 'ipython3',
        'nbconvert_exporter': 'python',
        'file_extension': '.py'
    }

    banner = "IPython on Fluent"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # self.ns_store = NameSpaceStore(self.shell.user_ns)

    def do_execute(self, code, silent, store_history=True,
                   user_expressions=None, allow_stdin=False):

        #print("Keys:", list(self.shell.user_ns.keys())[:3]) # dbg

        # self.ns_store.update_ns()
        # out = do_execute_simple(code, self.shell.user_ns)
        out = do_execute_simple(code)
        # self.ns_store.update()
        return out

# -> messaging reply dict
def do_execute_simple(code):
    run_cell_fluent(code)
    reply_content = {}
    reply_content[u'status'] = u'ok'
    reply_content['execution_count'] = 1 # XXX DUMMY
    reply_content[u'user_expressions'] = {} # XXX unused but also dummy
    return reply_content


# # -> ()
# def run_cell_simple(code, user_ns):
#     nodelist = ast.parse(code).body
#     nodes_exec, nodes_interactive = nodelist[:-1], nodelist[-1:]
#     bytecodes = []

#     for node in nodes_exec:
#         node = ast.Module([node])
#         bytecode = compile(node, '<string>', 'exec')
#         bytecodes.append(bytecode)

#     for node in nodes_interactive:
#         node = ast.Interactive([node])
#         bytecode = compile(node, '<string>', 'single')
#         bytecodes.append(bytecode)

#     for bytecode in bytecodes:
#         exec(bytecode, globals(), user_ns)


def run_cell_fluent(code):
    f_elb = 'a94158c8fa9aa11e9a4e50ee6f1fe091-184805043.us-east-1.elb.amazonaws.com'
    ip = '34.239.146.81'
    flconn = flclient.FluentConnection(f_elb, ip)
    cloud_run_cell = flconn.get('run_cell_lazy')
    res = cloud_run_cell(code, 'user0').get()
    for output in res:
        sys.stderr.write(output)


# if __name__ == '__main__':
#     from ipykernel.kernelapp import IPKernelApp
#     IPKernelApp.launch_instance(kernel_class=SLIPKernel)

f_elb = 'a94158c8fa9aa11e9a4e50ee6f1fe091-184805043.us-east-1.elb.amazonaws.com'
ip = '34.239.146.81'
flconn = flclient.FluentConnection(f_elb, ip)

bname = sys.argv[1]

if bname == 'register':
    # Define and register functions.

    def run_cell(fluent, code, user_id):
        # print("running run_cell")
        import ast
        import sys
        import time
        import cloudpickle as cp

        import anna.lattices


        import collections.abc
            # see bottom of
            # https://docs.python.org/3/library/collections.abc.html
            # for collections.abc tutorial

        # FIX BUG
        class LoggedDict(collections.abc.MutableMapping):
            def __init__(self, d):
                self.d = d
            def __getitem__(self, key):
                print("getitem({}) called!".format(key))
                return self.d.__getitem__(key)
            def __setitem__(self, key, value):
                print("setitem({}, {}) called!".format(key, value))
                return self.d.__setitem__(key, value)
            def __delitem__(self, key):
                print("delitem({}) called!".format(key))
                return self.d.__delitem__(key)
            def __iter__(self):
                print("iter() called!")
                return self.d.__iter__()
            def __len__(self):
                print("len() called!")
                return self.d.__len__()


        class CachedDict(collections.abc.MutableMapping):
            def __init__(self, dict_store):
                self.dict_store = dict_store
                self.dict_cache = dict()
                # todo: start a background thread for loading items
                # this requires list of keys to be stored as well lol
                # todo: threadsafety of ns_cache obv

            def __getitem__(self, key):
                # XXX single user only - todo cache staleness etc
                if key not in self.dict_cache:
                    print("\u001b[36mLoading \u001b[35m{}\u001b[36m from Hydro.\u001b[0m".format(key))
                    self.dict_cache[key] = self.dict_store[key]
                    # If a KeyError is raised, we will just propagate it upwards.
                    print("\u001b[36m\u001b[35m{}\u001b[36m loaded.\u001b[0m".format(key))
                return self.dict_cache[key]

            def __setitem__(self, key, value):
                print("\u001b[36mWriting \u001b[35m{}\u001b[36m to Hydro.\u001b[0m".format(key))
                self.dict_store[key] = value
                print("\u001b[36m\u001b[35m{}\u001b[36m written.\u001b[0m".format(key))
                # (write to cache too)
                self.dict_cache[key] = value

            def __delitem__(self, key):
                del self.dict_store[key]
                del self.dict_cache[key]

            def __iter__(self):
                raise NotImplementedError

            def __len__(self):
                raise NotImplementedError


        class FluentNSStore(collections.abc.MutableMapping):
            def __init__(self, session_id, fluent):
                self.session_id = session_id
                self.fluent = fluent
            def __getitem__(self, key):
                def kvs_get_bytestr(key):
                    lat = self.fluent.get(key)
                    if lat is None:
                        raise KeyError(key)
                    value = lat.reveal()[1]
                    return value
                res = kvs_get_bytestr("{}:{}".format(self.session_id, key))
                return cp.loads(res)
            def __setitem__(self, key, value):
                def kvs_set_bytestr(key, value):
                    ts = int(time.time())
                    lat = anna.lattices.LWWPairLattice(timestamp=ts, value=value)
                    self.fluent.put(key, lat)
                value = cp.dumps(value)
                return kvs_set_bytestr("{}:{}".format(self.session_id, key), value)
            def __delitem__(self, key):
                raise NotImplementedError
            def __iter__(self):
                raise NotImplementedError
            def __len__(self):
                raise NotImplementedError


        # Redirect output.
        output = []
        class logger:
            def write(self, data):
                output.append(data)
        sys.stdout = logger()
        sys.stderr = logger()

        ns_store = FluentNSStore(user_id, fluent)
        user_ns = CachedDict(ns_store)
        nodelist = ast.parse(code).body
        nodes_exec, nodes_interactive = nodelist[:-1], nodelist[-1:]

        # print("doing execs")
        bytecodes = []
        for node in nodes_exec:
            node = ast.Module([node])
            bytecode = compile(node, '<string>', 'exec')
            bytecodes.append(bytecode)

        for node in nodes_interactive:
            node = ast.Interactive([node])
            bytecode = compile(node, '<string>', 'single')
            bytecodes.append(bytecode)

        for bytecode in bytecodes:
            exec(bytecode, globals(), user_ns)

        # print(output)
        return output

    cf = flconn.register(run_cell, 'run_cell_lazy')
    if cf:
        print("Successfully registered.")

else:

    cloud_run_cell = flconn.get('run_cell_lazy')
    res = cloud_run_cell(bname, 'charles').get()
    for output in res:
        sys.stderr.write(output)

