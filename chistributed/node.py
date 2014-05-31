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
    self.nodeseted = []
    self.dead = []

    self.registered = False
    self.waiting = False

    #self.group = group
    if self.peer_names[0] == self.name:
        self.leader = True
    else:
        self.leader = False
    if len(self.succ_group) > 1 and len(self.prev_group) > 1:
        self.prev_leader = self.prev_group[0]
        self.succ_leader = self.succ_group[0]
        self.forward_nodes = [self.succ_leader]
    else:
        self.forward_nodes = self.peer_names[:]
        self.forward_nodes.remove(self.name)

    # This the actual data storage. Takes the form {'key': (msg_id, value), ...}
    #self.store = {'foo': (0, None)}
    self.store = {}
    for k in self.keyrange:
        self.store[k] = (0, 'hello')
    for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
      signal.signal(sig, self.shutdown)

    # Attributes for paxos get
    self.state = 'WAIT_PROPOSE'
    self.value = (0, None)
    self.consensus = False
    self.prepared = []
    self.promised = []
    self.accepted = []
    self.rejected = []

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
  def sendError(self, msg):
    if not self.waiting:
        return
    print msg
    if self.sent_id >= msg['id']:
        return
    self.req.send_json({'type': msg['type'] + 'Response', 'key': msg['key'], 'id': msg['id'], 'error': 'Key not accessible'})    
    self.waiting = False

  def majority(self, nodes):
    if len(nodes) > (len(self.peer_names) - len(self.dead)):
      return True
    return False

  # If node is the origin, wait for correct get/set info to be forwarded.
  # In case of timeout, send an error response for get
  def collectReply(self, msg):
    if msg['origin'] == self.name:
      self.loop.add_timeout(time.time() + .5, lambda: self.sendError(msg))
      #if not self.receive():
      #  self.req.send_json({'type': msg['type'] + 'Response', 'key': msg['key'], 'id': msg['id'], 'error': 'Key not accessible'})    

  def collectAcks(self, msg):
    #print 'replies:', self.nodeseted
    # TODO check failed
    for n in self.forward_nodes:
      if (n not in self.nodeseted) and (n not in self.dead):
        self.dead.append(n)
    self.nodeseted = []
    self.reply(msg)

  def collectPrepare(self, msg):
    # TODO check failed
    for n in self.peer_names:
      if (n not in self.dead) and (n not in self.prepared):
        self.dead.append(n)
      #self.prepared = []

  def collectPromise(self, msg):
    # TODO check failed
    k = msg['key']
    self.promised = List(Set(self.promised))
    if self.majority(self.promised):
      self.state = "WAIT_ACCEPTED"
      self.req.send_json({'type': 'accept', 
                          'key': k,
                          'value': self.store[k],
                          'source': self.name,
                          'destination': peer_names,
                          'id': msg['id']})
      self.loop.add_timeout(time.time() + .5, lambda: self.collectAccepted(msg))
      
  def collectAccepted(self, msg):
    # TODO check failed
    k = msg['key']
    self.accepted = List(Set(self.accepted))
    if self.majority(self.accepted):
      self.state = "CONSENSUS"
      self.req.send_json({'type': 'consensus', 
                          'key': k,
                          'value': self.store[k],
                          'source': self.name,
                          'destination': peer_names,
                          'id': msg['id']})
    elif self.majority(self.rejected):
      self.state = "WAIT_PROMISE"
      self.req.send_json({'type': 'prepare', 
                          'key': k,
                          'value': self.store[k],
                          'source': self.name,
                          'destination': peer_names,
                          'id': msg['id']})
      self.loop.add_timeout(time.time() + .5, lambda: self.collectPromise(msg)) 

  def handle(self, msg_frames):
    assert len(msg_frames) == 3
    assert msg_frames[0] == self.name
    # Second field is the empty delimiter
    msg = json.loads(msg_frames[2])

    if msg['type'] != 'hello':
      print msg['type'], self.name, msg['id']
    if msg['type'] == 'get':
      # TODO: handle errors, esp. KeyError
      k = msg['key']
      if k in self.keyrange:
        v = self.store[k][1]
      else:
        v = (0, '')
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
      self.collectReply(msg)
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
        self.waiting = True
      if not self.forward(msg):
        self.consistentSet(k, v, msg)
    elif msg['type'] == 'nodeset':
      k = msg['key']
      v = msg['value']
      if k in self.keyrange:
        self.store[k] = (msg['id'], v)
      msg['destination'] = [self.peer_names[0]]
      msg['source'] = self.name
      msg['type'] = 'nodesetAck'
      print msg
      self.req.send_json(msg)
    elif msg['type'] == 'nodesetAck':
      self.nodeseted.append(msg['source'])
    elif msg['type'] == 'hello':
      # should be the very first message we see
      if not self.registered:
        self.req.send_json({'type': 'hello', 'source': self.name})
        self.registered = True
    elif msg['type'] == 'getReply':
      self.reply(msg)
    elif msg['type'] == 'setReply':
      self.reply(msg)
    elif msg['type'] == 'propose':
      k = msg['key']
      if self.state == 'WAIT_PROPOSE':
        self.state = 'WAIT_PROMISE'
        self.req.send_json({'type': 'prepare', 
                            'key': k,
                            'value': self.store[k],
                            'source': self.name,
                            'destination': peer_names,
                            'id': msg['id']})
        self.loop.add_timeout(time.time() + .5, lambda: self.collectPromise(msg))
    elif msg['type'] == 'promise':
      if self.state == 'WAIT_PROMISE':
        self.promised.append(msg['source'])
    elif msg['type'] == 'accepted':
      if self.state == 'WAIT_ACCEPTED':
        self.accepted.append(msg['source'])
    elif msg['type'] == 'rejected':
      if self.state == 'WAIT_ACCEPTED':
        self.rejected.append(msg['source'])
    elif msg['type'] == 'prepare':
      k = msg['key']
      if msg['value'][0] > self.value[0]:
        self.store = msg['value']
        self.promised = []
        self.accepted = []
        self.rejected = []
        self.prepared.append(msg['source']) 
        self.req.send_json({'type': 'promise', 
                            'key': k,
                            'value': self.store[k],
                            'source': self.name,
                            'destination': msg['source'],
                            'id': msg['id']})
        #self.state = 'WAIT_PROMISE'
        #self.req.send_json({'type': 'prepare', 
        #                    'key': k,
        #                    'value': self.store[k],
        #                    'source': self.name,
        #                    'destination': peer_names,
        #                    'id': msg['id']})
        self.loop.add_timeout(time.time() + .5, lambda: self.collectPromise(msg))
    elif msg['type'] == 'accept':
      k = msg['key']
      if self.value == msg['value']:
        self.req.send_json({'type': 'accepted', 
                            'key': k,
                            'value': self.store[k],
                            'source': self.name,
                            'destination': msg['source'],
                            'id': msg['id']})
      elif self.value[0] < msg['value'][0]:
        self.req.send_json({'type': 'rejected', 
                            'key': k,
                            'value': self.store[k],
                            'source': self.name,
                            'destination': msg['source'],
                            'id': msg['id']})
    elif msg['type'] == 'consensus':
      self.consensus = True
      self.value = msg['value']
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
      print msg
      self.req.send_json(msg) 
    elif not self.leader:
      #print 'sending to recipient', msg['type']
      msg['destination'] = [self.peer_names[0]]
      print msg
      self.req.send_json(msg) 
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
        msg['destination'] = self.forward_nodes
      msg['type'] = msg['type'][:3] + 'Reply'
      print msg
      self.req.send_json(msg)
    else:
      #print 'id', msg['id'], self.sent_id
      if self.sent_id >= int(msg['id']):
        return
      msg['type'] = msg['type'][:3] + 'Response'
      self.sent_id = int(msg['id'])
      print msg
      self.req.send_json(msg)
      self.waiting = False
      
  def consistentSet(self, k, v, msg):
    print msg
    self.req.send_json({'type': 'nodeset', 
                        'key' : k, 
                        'value' : v, 
                        'source': self.name, 
                        'destination': self.peer_names, 
                        'id': msg['id']})    
    self.loop.add_timeout(time.time() + .5, lambda: self.collectAcks(msg))
    #self.store[k] = (msg['id'], v)

  def consistentGet(self, k, msg):
    #START PAXOS
    self.req.send_json({'type': 'propose', 
                        'key' : k, 
                        'value' : None, 
                        'source': self.name, 
                        'destination': self.peer_names, 
                        'id': msg['id']})    
    self.loop.add_timeout(time.time() + .5, lambda: self.collectPrepare(msg))

    self.loop.add_timeout(time.time() + 10, lambda: self.checkConsensus(msg))

    #v = self.store[k][1]
    msg['value'] = v
    self.reply(msg)

  def checkConsensus(self, msg):
    if self.consensus:
      self.store[k] = self.value
      msg['value'] = self.value
      self.reply(msg)
    else:
      self.req.send_json({'type': msg['type'] + 'Response', 'key': msg['key'], 'id': msg['id'], 'error': 'No consensus reached'})   

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

