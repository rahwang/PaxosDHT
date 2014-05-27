from Queue import Queue
import random
import json
import sys
import signal
import time
import zmq
from zmq.eventloop import ioloop, zmqstream
ioloop.install()

class Node:
  def __init__(self, node_name, pub_endpoint, router_endpoint, spammer, peer_names):#, group):
    self.loop = ioloop.ZMQIOLoop.current()
    self.context = zmq.Context()
    # SUB socket for receiving messages from the broker
    self.sub_sock = self.context.socket(zmq.SUB)
    self.sub_sock.connect(pub_endpoint)
    # make sure we get messages meant for us!
    self.sub_sock.set(zmq.SUBSCRIBE, node_name)
    self.sub = zmqstream.ZMQStream(self.sub_sock, self.loop)
    self.sub.on_recv(self.handle)

    # REQ socket for sending messages to the broker
    self.req_sock = self.context.socket(zmq.REQ)
    self.req_sock.connect(router_endpoint)
    self.req = zmqstream.ZMQStream(self.req_sock, self.loop)
    self.req.on_recv(self.handle_broker_message)

    self.name = node_name
    self.range = []
    self.spammer = spammer
    self.peer_names = peer_names
    self.succ_names = succ_names
    self.prev_names = prev_names

    #self.group = group
    if self.peer_names[0] == self.name:
        self.leader = True
    else:
        self.leader = False

    self.store = {'foo': 'bar'}

    for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
      signal.signal(sig, self.shutdown)

  def start(self):
    '''
    Simple manual poller, dispatching received messages and sending those in
    the message queue whenever possible.
    '''
    self.loop.start()

  def handle_broker_message(self, msg_frames):
    '''
    Nothing important to do here yet.
    '''
    pass

  def handle(self, msg_frames):
    assert len(msg_frames) == 3
    assert msg_frames[0] == self.name
    # Second field is the empty delimiter
    msg = json.loads(msg_frames[2])

    if msg['type'] == 'get':
      # TODO: handle errors, esp. KeyError
      k = msg['key']
      self.req.send_json({'type': 'log', 'debug': {'event': 'getting', 'node': self.name, 'key': k, 'value': v}})
      self.consistentGet(self, k)
      #self.req.send_json({'type': 'getResponse', 'id': msg['id'], 'value': v})
    elif msg['type'] == 'set':
      k = msg['key']
      v = msg['value']
        
      self.req.send_json({'type': 'log', 'debug': {'event': 'setting', 'node': self.name, 'key': k, 'value': v}})
      self.consistentSet(self, k, v)
      #self.store[k] = v


    elif msg['type'] == 'nodeset':
        k = msg['key']
        v = msg['value']
        self.store[k] = v
    elif msg['type'] == 'hello':
      # should be the very first message we see
      self.req.send_json({'type': 'hello', 'source': self.name})
    else:
      self.req.send_json({'type': 'log', 'debug': {'event': 'unknown', 'node': self.name}})

  def shutdown(self, sig, frame):
    self.loop.stop()
    self.sub_sock.close()
    self.req_sock.close()
    sys.exit(0)

  def consistentSet(self, v, k):
    self.req.send_json({'type': 'nodeset', 'key' : k, 'value' : v, 'destination': self.peer_names, 'id': msg['id']})    

    self.store[k] = v
    self.req.send_json({'type': 'setResponse', 'id': msg['id'], 'value': v})


if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument('--pub-endpoint',
      dest='pub_endpoint', type=str,
      default='tcp://127.0.0.1:23310')
  parser.add_argument('--router-endpoint',
      dest='router_endpoint', type=str,
      default='tcp://127.0.0.1:23311')
  parser.add_argument('--node-name',
      dest='node_name', type=str,
      default='test_node')
  parser.add_argument('--spammer',
      dest='spammer', action='store_true')
  parser.set_defaults(spammer=False)
  parser.add_argument('--peer-names',
      dest='peer_names', type=str,
      default='')
 # parser.add_argument('--group',
 #     dest='group', type=str,
 #     default='')
  args = parser.parse_args()
  args.peer_names = args.peer_names.split(',')
  #args.group = int(args.group)
  Node(args.node_name, args.pub_endpoint, args.router_endpoint, args.spammer, args.peer_names).start()#, args.group).start()

