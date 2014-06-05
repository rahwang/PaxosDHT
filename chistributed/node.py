from Queue import Queue
import random
import json
import sys
import signal
import time
import zmq
import math
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
    '''
    if self.name in ['test1','test2','test3']:
        self.keyrange = ['foo']
    if self.name in ['test3','test4']:
        self.keyrange = ['bar']
    if self.name in ['test5','test6']:
        self.keyrange = ['baz']
    '''
    if self.name in ['testA','test2','test3','test4','test5']:
        self.keyrange = [i for i in range(1,6)]
    if self.name in ['test6','test7','test8','test9','test10']:
        self.keyrange = [i for i in range(6,11)]
    if self.name in ['test11','test12','test13','test14','test15']:
        self.keyrange = [i for i in range(11,16)]
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
        self.peer_leader = self.peer_names[0]
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

  def reqsendjson(self,msg):
    # Send msg
    self.req.send_json(msg)

    # The following message types need acks
    if msg['type'] in ['get', 'set', 'getReply', 'setReply', 'nodeset']:
      self.loop.add_timeout(time.time() + 0.5, lambda: self.collectAcks(msg))
      
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
    #print msg
    if self.sent_id >= msg['id']:
        return
    self.reqsendjson({'type': msg['type'] + 'Response', 'key': msg['key'], 'id': msg['id'], 'error': 'Key not accessible'})    
    self.waiting = False

  def majority(self, nodes):
    print len(nodes), math.ceil((len(self.peer_names) - len(self.dead)) / 2)
    if len(nodes) >= math.ceil((len(self.peer_names) - len(self.dead)) / 2):
      return True
    return False

  # If node is the origin, wait for correct get/set info to be forwarded.
  # In case of timeout, send an error response for get
  def collectReply(self, msg):
    if msg['origin'] == self.name:
      self.loop.add_timeout(time.time() + 10, lambda: self.sendError(msg))
      #if not self.receive():
      #  self.reqsendjson({'type': msg['type'] + 'Response', 'key': msg['key'], 'id': msg['id'], 'error': 'Key not accessible'})    

  def collectAcks(self, msg):
    if msg['type'] == 'nodeset':
      for n in self.peer_names:
        if (n not in self.nodeseted) and (n not in self.dead):
          self.dead.append(n)
      self.nodeseted = []
      self.reply(msg)
    elif msg['type'] in ['get', 'set']:
      if msg['destination'] == self.peer_leader:
        # Start leader election
        pass
      else:
        # Try sending to the next node
        pass
    elif msg['type'] in ['getReply', 'setReply']:
      if msg['destination'] == msg['origin']:
        # Fail
        pass
      else:
        # Try sending to the next node
        pass

  def collectPrepare(self, msg):
    # TODO check failed
    for n in self.peer_names:
      if (n not in self.dead) and (n not in self.prepared):
        self.dead.append(n)
      #self.prepared = []

  def collectPromise(self, msg):
    # TODO check failed
    k = msg['key']
    self.promised = list(set(self.promised))
    print 'promises', self.name, self.promised
    if self.majority(self.promised):# and self.state == 'WAIT_PROMISE':
      self.value = self.store[k]#299msg['value']
      print 'promise majority'
      self.state = "WAIT_ACCEPTED"
      self.reqsendjson({'type': 'accept', 
                          'key': k,
                          'value': self.store[k],
                          'source': self.name,
                          'destination': self.peer_names,
                          'id': msg['id']})
      self.loop.add_timeout(time.time() + .5, lambda: self.collectAccepted(msg))
      
  def collectAccepted(self, msg):
    # TODO check failed
    k = msg['key']
    self.accepted = list(set(self.accepted))
    if self.majority(self.accepted):
      print 'majority'
      self.state = "CONSENSUS"
      self.reqsendjson({'type': 'consensus', 
                          'key': k,
                          'value': self.store[k],
                          'source': self.name,
                          'destination': self.peer_names,
                          'id': msg['id']})
    elif self.majority(self.rejected):
      self.state = "WAIT_PROMISE"
      self.reqsendjson({'type': 'prepare', 
                          'key': k,
                          'value': self.store[k],
                          'source': self.name,
                          'destination': self.peer_names,
                          'id': msg['id']})
      self.loop.add_timeout(time.time() + .5, lambda: self.collectPromise(msg)) 

  def handle(self, msg_frames):
    assert len(msg_frames) == 3
    if msg_frames[0] != self.name:
        print msg_frames[0], self.name
    assert msg_frames[0] == self.name
    # Second field is the empty delimiter
    msg = json.loads(msg_frames[2])

    if msg['type'] != 'hello':
      print msg['type'], self.name, msg['id']

    if msg['type'] in ['propose', 'promise', 'accepted', 'rejected', 'accept', 'prepare', 'consensus']:
      self.handle_paxos(msg)
      return
    elif msg['type'] == 'ack':
      # TODO SOME HANDLING
      return
    elif msg['type'] == 'hello':
      # should be the very first message we see
      if not self.registered:
        self.reqsendjson({'type': 'hello', 'source': self.name})
        self.registered = True
      return
   
    # Messages which require acks

    msg['key'] = int(msg['key'])
    k = msg['key']
    if 'value' in msg.keys():
      v = msg['value'] 

    if msg['type'] == 'get':
      # TODO: handle errors, esp. KeyError
      if k in self.keyrange:
        v = self.store[k][1]
      else:
        v = (0, '')
      self.reqsendjson({'type': 'log', 
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
      print self.name, 'collectReplied'
    elif msg['type'] == 'set':
      self.reqsendjson({'type': 'log', 
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
      if k in self.keyrange:
        self.store[k] = (msg['id'], v)
        print 'NODESET', self.store[k], self.name
    elif msg['type'] == 'getReply':
      self.reply(msg)
    elif msg['type'] == 'setReply':
      self.reply(msg)
    else:
      self.reqsendjson({'type': 'log', 
                          'debug': {'event': 'unknown', 
                                    'node': self.name}})
    self.sendack(msg)

  def sendack(self, msg):
    if 'source' in msg.keys():
      msg['prevtype'] = msg['type']
      msg['type'] = 'ack'
      msg['destination'] = msg['source']
      msg['source'] = self.name
      self.reqsendjson(msg)

  def handle_paxos(self, msg):
    k = msg['key']
    if 'value' in msg.keys():
      v = msg['value'] 
    
    if msg['type'] == 'propose':
      #print 'received propose', self.name
      if self.state == 'WAIT_PROPOSE':
        self.state = 'WAIT_PROMISE'
        self.reqsendjson({'type': 'prepare', 
                            'key': k,
                            'value': self.store[k],
                            'source': self.name,
                            'destination': self.peer_names,
                            'id': msg['id']})
        print 'sending prepare', self.name, 'to', self.peer_names
        self.loop.add_timeout(time.time() + 1.5, lambda: self.collectPromise(msg))
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
      print 'received prepare', self.name, 'from', msg['source']
      if msg['value'][0] > self.value[0]:
        self.store[msg['key']] = msg['value']
        self.promised = []
        self.accepted = []
        self.rejected = []
        self.prepared.append(msg['source']) 
        self.reqsendjson({'type': 'promise', 
                            'key': k,
                            'value': self.store[k],
                            'source': self.name,
                            'destination': [msg['source']],
                            'id': msg['id']})
        print 'promise sent', self.name
        self.loop.add_timeout(time.time() + 1.5, lambda: self.collectPromise(msg))
    elif msg['type'] == 'accept':
      print 'accept message', self.value, msg['value'], self.name
      if self.value == msg['value']:
        self.reqsendjson({'type': 'accepted', 
                            'key': k,
                            'value': self.store[k],
                            'source': self.name,
                            'destination': [msg['source']],
                            'id': msg['id']})
      elif self.value[0] < msg['value'][0]:
        self.reqsendjson({'type': 'rejected', 
                            'key': k,
                            'value': self.store[k],
                            'source': self.name,
                            'destination': msg['source'],
                            'id': msg['id']})
    elif msg['type'] == 'consensus':
      self.consensus = True
      self.value = msg['value']
      self.store[msg['key']] = self.value
      self.state = 'WAIT_PROPOSE'
    else:
      self.reqsendjson({'type': 'log', 
                          'debug': {'event': 'unknown', 
                                    'node': self.name}})

  def shutdown(self, sig, frame):
    self.loop.stop()
    self.sub_sock.close()
    self.req_sock.close()
    sys.exit(0)

  # Forwards msg to correct nodes. Returns True is msg forwarded, False if no forwarding needed
  def forward(self, msg):
    #print 'forwarding', msg['type'], 'leader', self.leader, 'key', msg['key'], self.keyrange
    if msg['key'] not in self.keyrange:
      msg['destination'] = self.forward_nodes #[self.succ_leader, self.prev_leader] #self.peer_names
      #print msg
      self.reqsendjson(msg) 
    elif not self.leader:
      #print 'sending to recipient', msg['type']
      msg['destination'] = [self.peer_leader]
      #print msg
      self.reqsendjson(msg) 
    else:
      return False
    return True

  def reply(self, msg):
    #print 'origin:', msg['type'], msg['origin'], self.name
    if msg['origin'] != self.name:
      if msg['origin'] in self.peer_names:
        msg[u'destination'] = [msg['origin']]
      else:
        # Else change to next_names
        msg['destination'] = self.forward_nodes
      msg['type'] = msg['type'][:3] + 'Reply'
      msg['source'] = self.name
      print msg
      self.reqsendjson(msg)
      print 'sent'
    else:
      #print 'id', msg['id'], self.sent_id
      if self.sent_id >= int(msg['id']):
        return
      msg['type'] = msg['type'][:3] + 'Response'
      self.sent_id = int(msg['id'])
      #print msg
      self.reqsendjson(msg)
      self.waiting = False
      if msg['type'] == 'getResponse':
        print self.name, 'accepted:', self.accepted, 'rejected:', self.rejected, 'promised:', self.promised, self.value
      
  def consistentSet(self, k, v, msg):
    #print msg
    new_msg = {'type': 'nodeset', 
               'key' : k, 
               'value' : v, 
               'source': self.name, 
               'destination': self.peer_names, 
               'origin': msg['origin'],
               'id': msg['id']}
    self.reqsendjson(new_msg)
    self.loop.add_timeout(time.time() + .5, lambda: self.collectAcks(new_msg))

  def consistentGet(self, k, msg):
    #START PAXOS
    self.reqsendjson({'type': 'propose', 
                        'key' : k, 
                        'value' : None, 
                        'source': self.name, 
                        'destination': self.peer_names, 
                        'id': msg['id']})    
    self.loop.add_timeout(time.time() + .5, lambda: self.collectPrepare(msg))

    self.loop.add_timeout(time.time() + 6, lambda: self.checkConsensus(msg))

    #v = self.store[k][1]
    #msg['value'] = v
    #self.reply(msg)

  def checkConsensus(self, msg):
    #print 'consensus', self.consensus, msg['origin']
    print self.name, 'accepted:', self.accepted, 'rejected:', self.rejected, 'promised:', self.promised, self.value
    if self.consensus:
      print 'self.value', self.value
      self.store[msg['key']] = self.value
      msg['value'] = self.value[1]
      print 'consensus message', msg
      self.reply(msg)
    else:
      
      #new_msg = 
      self.reply({'type': msg['type'] + 'Response', 'key': msg['key'], 'id': msg['id'], 'value': 'No consensus reached', 'origin': msg['origin']})
      #new_msg['origin'] = msg['origin']
      #self.reply(new_msg)

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

