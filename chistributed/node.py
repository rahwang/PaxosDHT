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
  def __init__(self, node_name, pub_endpoint, router_endpoint, spammer, peer_names):#prev_names, succ_names, peer_names):
    sys.stdout = open('logging', 'a') 
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
    if self.name == 'test2':
        self.keyrange = ['foo']
    else:
        self.keyrange = ['foo']
    self.spammer = spammer
    self.peer_names = peer_names
    self.sent_id = 0
    #self.succ_names = succ_names
    #self.prev_names = prev_names
    #self.set_acks_needed = []

    self.registered = False

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
    print msg['type'], self.name
    if msg['type'] == 'get':
      # TODO: handle errors, esp. KeyError
      k = msg['key']
      v = self.store['key']
      self.req.send_json({'type': 'log', 
                          'debug': {'event': 'getting', 
                                    'node': self.name, 
                                    'key': k, 
                                    'value': v}})
      if 'origin' not in msg.keys():
        msg['origin'] = self.name
      if not self.forward(msg):
        self.consistentGet(k, msg)
    elif msg['type'] == 'set':
      k = msg['key']
      v = msg['value']
        
      self.req.send_json({'type': 'log', 
                          'debug': {'event': 'setting', 
                                    'node': self.name, 
                                    'key': k, 
                                    'value': v}})
      if 'origin' not in msg.keys():
        msg['origin'] = self.name
      if not self.forward(msg):
        self.consistentSet(k, v, msg)
    elif msg['type'] == 'nodeset':
        k = msg['key']
        v = msg['value']
        self.store[k] = v
    elif msg['type'] == 'hello':
      # should be the very first message we see
      if not self.registered:
        self.req.send_json({'type': 'hello', 'source': self.name})
        self.registered = True
    elif msg['type'] == 'getReply':
      self.reply(msg)
    elif msg['type'] == 'setReply':
      self.reply(msg)
    else:
      self.req.send_json({'type': 'log', 
                          'debug': {'event': 'unknown', 
                                    'node': self.name}})

  def shutdown(self, sig, frame):
    self.loop.stop()
    self.sub_sock.close()
    self.req_sock.close()
    sys.exit(0)

  # Forwards msg to correct nodes. Returns True is msg forwarded, False if no forwarding needed
  def forward(self, msg):
    if msg['key'] not in self.keyrange:
      self.req.send_json({'type': msg['type'], 
                          'key': msg['key'], 
                          'value': msg['value'], 
                          #'destination': self.succ_names,
                          'destination': self.peer_names, 
                          'id': msg['id'], 
                          'origin': msg['origin']})
      self.req.send_json({'type': msg['type'], 
                          'key': msg['key'], 
                          'value': msg['value'], 
                          'destination': self.peer_names,
                          #'destination': self.prev_names, 
                          'id': msg['id'], 
                          'origin': msg['origin']})
    elif not self.leader:
      self.req.send_json({'type': msg['type'], 
                          'key': msg['key'], 
                          'value': msg['value'], 
                          'destination': [self.peer_names[0]], 
                          'id': msg['id'], 
                          'origin': msg['origin']})
      print 'sending to recipient'
    else:
      return False
    return True

  def reply(self, msg):
    if msg['type'] in ['setReply', 'set']:
      mType = 'set'
    else:
      mType = 'get'      
    if msg['origin'] != self.name:
      if msg['origin'] in self.peer_names:
        dst = msg['origin']
      else:
        # Else change to next_names
        dst = self.peer_names
      self.req.send_json({'type': mType + 'Reply', 
                          'id': msg['id'], 
                          'value': msg['value'],
                          'destination': dst, 
                          'origin': msg['origin']})
    else:
      print 'id', msg['id'], self.sent_id
      if self.sent_id >= int(msg['id']):
        return
      self.req.send_json({'type': mType + 'Response', 
                          'id': msg['id'], 
                          'value': msg['value']})
      
  def consistentSet(self, k, v, msg):
    self.req.send_json({'type': 'nodeset', 'key' : k, 'value' : v, 'destination': self.peer_names, 'id': msg['id']})    
    self.store[k] = v
    self.reply(msg)

  def consistentGet(self, k, msg):
    #TODO PAXOS
    v = self.store[k]
    self.reply(msg)

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
  parser.add_argument('--succ-group',
      dest='peer_names', type=str,
      default='')
  parser.add_argument('--prev-group',
      dest='peer_names', type=str,
      default='')
 # parser.add_argument('--group',
 #     dest='group', type=str,
 #     default='')
  args = parser.parse_args()
  args.peer_names = args.peer_names.split(',')
  #args.prev_group = args.prev_group.split(',')
  #args.succ_group = args.succ_group.split(',')
  #args.group = int(args.group)
  Node(args.node_name, args.pub_endpoint, args.router_endpoint, args.spammer, args.peer_names).start()# args.prev_names, args.succ_names, args.peer_names).start()

