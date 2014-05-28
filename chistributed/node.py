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
  def __init__(self, node_name, pub_endpoint, router_endpoint, spammer, peer_names, prev_group, succ_group):
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
    if self.name in ['test1','test2']:
        self.keyrange = ['foo']
    if self.name in ['test3','test4']:
        self.keyrange = ['bar']
    if self.name in ['test5','test6']:
        self.keyrange = ['baz']
    self.spammer = spammer
    self.peer_names = peer_names
    self.sent_id = 0
    self.succ_group = succ_group
    self.prev_group = prev_group
    #self.set_acks_needed = []

    self.registered = False
    self.waiting = False

    #self.group = group
    if self.peer_names[0] == self.name:
        self.leader = True
    else:
        self.leader = False
    if len(self.succ_group) > 0 and len(self.prev_group) > 0:
        self.prev_leader = self.prev_group[0]
        self.succ_leader = self.succ_group[0]
        self.forward_nodes = [self.succ_leader, self.prev_leader]
    else:
        self.forward_nodes = self.peer_names

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
    if msg['type'] != 'hello':
        print msg['type'], self.name
    if msg['type'] == 'get':
      # TODO: handle errors, esp. KeyError
      k = msg['key']
      v = self.store[k]
      self.req.send_json({'type': 'log', 
                          'debug': {'event': 'getting', 
                                    'node': self.name, 
                                    'key': k, 
                                    'value': v}})
      if 'origin' not in msg.keys():
        msg['origin'] = self.name
        self.waiting = True
      if not self.forward(msg):
        self.consistentGet(k, msg)
      if msg['origin'] == self.name:
        time.sleep(0.5)
        self.req.send_json({'type': 'time_elapsed', 'orig_type': msg['type'], 'key': msg['key'], 'destination': [self.name], 'id': msg['id']})
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
        if k in self.keyrange:
            self.store[k] = v
    elif msg['type'] == 'time_elapsed':
        if not self.waiting:
            return
        if msg['orig_type'] == 'get':
            if self.sent_id >= int(msg['id']):
                return
            if msg['key'] not in self.keyrange:
                self.req.send_json({'type': 'getResponse', 'key': msg['key'], 'id':msg['id'], 'error': 'Key not accessible'})
            else:
                self.req.send_json({'type': 'getResponse', 'key': msg['key'], 'id':msg['id'], 'value': self.store[msg['key']]})
            self.sent_id = int(msg['id'])
            self.waiting = False
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
    print 'forwarding', msg['type'], 'leader', self.leader, 'key', msg['key'], self.keyrange
    if msg['key'] not in self.keyrange:
      msg['destination'] = self.forward_nodes #[self.succ_leader, self.prev_leader] #self.peer_names
      self.req.send_json(msg) 
    elif not self.leader:
      print 'sending to recipient', msg['type']
      msg['destination'] = [self.peer_names[0]]
      self.req.send_json(msg) 
      #self.waiting = True
      #time.sleep(0.2)
      #self.req.send_json({'type': 'time_elapsed', 'orig_type': msg['type'], 'key': msg['key'], 'destination': [self.name], 'id': msg['id']})
      #print 'sleeping'
    else:
      return False
    return True

  def reply(self, msg):
    print 'origin:', msg['type'], msg['origin'], self.name
    if msg['origin'] != self.name:
      if msg['origin'] in self.peer_names:
        msg['destination'] = [msg['origin']]
      else:
        # Else change to next_names
        msg['destination'] = self.peer_names
      msg['type'] = msg['type'][:3] + 'Reply'
      self.req.send_json(msg)
    else:
      print 'id', msg['id'], self.sent_id
      if self.sent_id >= int(msg['id']):
        return
      msg['type'] = msg['type'][:3] + 'Response'
      self.sent_id = int(msg['id'])
      self.req.send_json(msg)
      self.waiting = False
      
  def consistentSet(self, k, v, msg):
    self.req.send_json({'type': 'nodeset', 'key' : k, 'value' : v, 'destination': self.peer_names, 'id': msg['id']})    
    self.store[k] = v
    self.reply(msg)

  def consistentGet(self, k, msg):
    #TODO PAXOS
    v = self.store[k]
    msg['value'] = v
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
      dest='succ_group', type=str,
      default='')
  parser.add_argument('--prev-group',
      dest='prev_group', type=str,
      default='')
 # parser.add_argument('--group',
 #     dest='group', type=str,
 #     default='')
  args = parser.parse_args()
  args.peer_names = args.peer_names.split(',')
  args.prev_group = args.prev_group.split(',')
  args.succ_group = args.succ_group.split(',')
  #args.group = int(args.group)
  Node(args.node_name, args.pub_endpoint, args.router_endpoint, args.spammer, args.peer_names, args.prev_group, args.succ_group).start()

