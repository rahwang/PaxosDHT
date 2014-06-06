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
    if self.name in ['testA','test2','test3','test4','test5']:
        self.keyrange = [i for i in range(1,6)]
    if self.name in ['test6','test7','test8','test9','test10']:
        self.keyrange = [i for i in range(6,11)]
    if self.name in ['test11','test12','test13','test14','test15']:
        self.keyrange = [i for i in range(11,16)]
    self.peer_names = peer_names
    self.sent_id = 0
    self.succ_group = succ_group
    self.dead = []
    self.outstanding_acks = []

    self.registered = False
    self.waiting = False
    self.seen_id = 0

    if self.peer_names[0] == self.name:
        self.leader = True
    else:
        self.leader = False
    if len(self.succ_group) > 1:
        self.succ_leader = self.succ_group[0]
        self.forward_nodes = self.succ_group
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
    self.nopromised = []
    self.accepted = []
    self.rejected = []
    self.group_tried = [False]*len(self.succ_group)
    
    # Attributes for Leader Election
    self.timeoutTime = 0.5
    self.leaderID = 1
    self.leaderPromiseID = 1
    self.leaderPromise = 1
    self.leaderUpdateTimeout = 0.5
    self.leaderUpdateTime = 0.0
    self.old_peer_leader = self.peer_leader
    self.old_leader_ref = None
    self.forwardMessage = None 
    
    self.electionVotes = 0
    self.electionVetos = 0
    self.vetoLeaderID = 0 
    

  def reqsendjson(self,msg):
    # Send msg
    try:
        if self.seen_id > int(msg['id']):
            return
    except KeyError:
         self.req.send_json({'type': 'log', 
                           'debug': {'event': 'error', 
                                     'node': self.name, 
                                     'msg': msg}})
    self.req.send_json(msg)

    # The following message types need acks
    if msg['type'] in ['get', 'set', 'getReply', 'setReply', 'nodeset']:
      self.loop.add_timeout(time.time() + 0.5, lambda: self.collectAcks(msg.copy()))
      for i in msg['destination']:
        if i != self.name:
            self.outstanding_acks.append((i, msg['id']))
            self.outstanding_acks[:] = [a for a in self.outstanding_acks if int(a[1]) >= int(msg['id'])]
      
      
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
    if self.sent_id >= msg['id']:
        return
    self.reqsendjson({'type': msg['type'] + 'Response', 'key': msg['key'], 'id': msg['id'], 'error': 'Key not accessible'})    
    self.waiting = False

  def majority(self, nodes):
    if len(nodes) >= math.ceil((len(self.peer_names) - len(self.dead)) / 2):
      return True
    return False

  # If node is the origin, wait for correct get/set info to be forwarded.
  # In case of timeout, send an error response for get
  def collectReply(self, msg):
    if msg['origin'] == self.name:
      self.loop.add_timeout(time.time() + 10, lambda: self.sendError(msg.copy()))

  def collectAcks(self, msg):
    try:
        if self.seen_id > int(msg['id']):
            return
    except KeyError:
         self.req.send_json({'type': 'log', 
                           'debug': {'event': 'error', 
                                     'node': self.name, 
                                     'msg': msg}})
        
    if msg['type'] == 'nodeset':
      for n in self.peer_names:
        if ((n,msg['id']) in self.outstanding_acks) and (n not in self.dead):
          self.dead.append(n)
      msg['type'] = 'set'
      self.reply(msg.copy())
      return
    if ((msg['destination'][0],msg['id']) not in self.outstanding_acks):
        return 
    elif msg['type'] in ['get', 'set']:
      if msg['destination'] == [self.peer_leader]:
        # Start leader election
        self.outstanding_acks[:] = [i for i in self.outstanding_acks if i != (msg['source'],msg['id'])]
        if self.old_leader_ref == None:
          self.old_leader_ref = msg['destination'][0]
          self.forwardMessage = msg.copy()
          self.leaderElection(msg.copy())
        pass
      elif msg['destination'][0] in self.peer_names:
        pass
      else:
        i = (self.succ_group.index(msg['destination'][0]) + 1) % len(self.succ_group)
        self.outstanding_acks[:] = [x for x in self.outstanding_acks if x != (msg['destination'][0],msg['id']) and int(x[1]) >= self.seen_id]
        self.group_tried[self.succ_group.index(msg['destination'][0])] = True
        msg['destination'] = [self.succ_group[i]]
        msg['source'] = self.name
        if msg['type'] == 'get' and self.name == 'test33':
         self.req.send_json({'type': 'log', 
                           'debug': {'event': 'getting', 
                                     'node': self.name, 
                                     'destination': msg['destination'],
                                     'acks': self.outstanding_acks}})
        if self.waiting:
          self.reqsendjson(msg)
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

  def collectPromise(self, msg):
    # TODO check failed
    k = msg['key']
    self.promised = list(set(self.promised))
    self.nopromised = list(set(self.nopromised))
    if len(self.promised) > len(self.nopromised):
      self.value = self.store[k]
      self.state = "WAIT_ACCEPTED"
      self.reqsendjson({'type': 'accept', 
                          'key': k,
                          'value': self.store[k],
                          'source': self.name,
                          'destination': self.peer_names,
                          'id': msg['id']})
      self.loop.add_timeout(time.time() + .5, lambda: self.collectAccepted(msg.copy()))
      
  def collectAccepted(self, msg):
    # TODO check failed
    k = msg['key']
    self.accepted = list(set(self.accepted))
    if len(self.accepted) > len(self.rejected):
      self.state = "CONSENSUS"
      self.reqsendjson({'type': 'consensus', 
                          'key': k,
                          'value': self.store[k],
                          'source': self.name,
                          'destination': self.peer_names,
                          'id': msg['id']})
    else:
      self.state = "WAIT_PROMISE"
      self.reqsendjson({'type': 'prepare', 
                          'key': k,
                          'value': self.store[k],
                          'source': self.name,
                          'destination': self.peer_names,
                          'id': msg['id']})
      self.loop.add_timeout(time.time() + .5, lambda: self.collectPromise(msg.copy())) 

  def handle(self, msg_frames):
    assert len(msg_frames) == 3
    assert msg_frames[0] == self.name
    # Second field is the empty delimiter
    msg = json.loads(msg_frames[2])
    self.handle_message(msg.copy())
    
  def handle_message(self, msg):
    if 'id' not in msg.keys():
        msg['id'] = 0
    if self.seen_id > int(msg['id']):
        return
    self.seen_id = max(self.seen_id, int(msg['id']))

    if msg['type'] in ['propose', 'promise', 'nopromise', 'accepted', 'rejected', 'accept', 'prepare', 'consensus']:
      self.handle_paxos(msg.copy())
      return
    elif msg['type'] in ['leaderElec', 'leaderVeto', 'leaderVote','leaderAccept']:
      self.handle_leader_elect(msg.copy())
      return
    elif msg['type'] == 'ack':
      if (msg['source'],msg['id']) in self.outstanding_acks:
        orig = self.outstanding_acks[:]
        self.outstanding_acks[:] = [i for i in self.outstanding_acks if i != (msg['source'],msg['id'])]
      return
    elif msg['type'] == 'hello':
      # should be the very first message we see
      if not self.registered:
        self.reqsendjson({'type': 'hello', 'id': 0, 'source': self.name})
        self.registered = True
      return
   
    # Messages which require acks
    
    msg['key'] = int(msg['key'])
    k = msg['key']
    if 'value' in msg.keys():
      v = msg['value'] 
    
    if 'source' not in msg.keys():
      msg['source'] = self.name
    if 'origin' not in msg.keys():
      msg['origin'] = self.name
      self.waiting = True
    
    if msg['type'] == 'get':
      if k in self.keyrange:
        v = self.store[k][1]
      else:
        v = (0, '')
      
      if 'source' not in msg.keys():
        msg['source'] = self.name
      if 'origin' not in msg.keys():
        msg['origin'] = self.name
        self.waiting = True
      if not self.forward(msg.copy()):
        self.consistentGet(k, msg)
      self.collectReply(msg.copy())
    elif msg['type'] == 'set':
      if 'source' not in msg.keys():
        msg['source'] = self.name
      if 'origin' not in msg.keys():
        msg['origin'] = self.name
        self.waiting = True
      if not self.forward(msg.copy()):
        self.consistentSet(k, v, msg)
    elif msg['type'] == 'nodeset':
      if k in self.keyrange:
        self.store[k] = (msg['id'], v)
    elif msg['type'] == 'getReply':
      self.reply(msg.copy())
    elif msg['type'] == 'setReply':
      self.reply(msg.copy())
    else:
      self.reqsendjson({'type': 'log', 
                          'debug': {'event': 'unknown', 
                                    'prev_type': msg['type'],
                                    'node': self.name}})
    self.sendack(msg.copy())

  def sendack(self, msg2):
    msg = msg2.copy()
    if 'source' in msg.keys() and msg['source'] != self.name:
      msg['prevtype'] = msg['type']
      msg['type'] = 'ack'
      msg['destination'] = [msg['source']]
      msg['source'] = self.name
      self.reqsendjson(msg)

  def handle_paxos(self, msg):
    k = msg['key']
    if 'value' in msg.keys():
      v = msg['value'] 
    
    if msg['type'] == 'propose':
      if ((self.state == 'CONSENSUS') and (msg['id'] != self.store[k][0])) or (self.state == 'WAIT_PROPOSE'):
        self.state = 'WAIT_PROMISE'
        self.value = self.store[k]
        self.reqsendjson({'type': 'prepare', 
                            'key': k,
                            'value': self.store[k],
                            'source': self.name,
                            'destination': self.peer_names,
                            'id': msg['id']})
        self.loop.add_timeout(time.time() + 1.5, lambda: self.collectPromise(msg.copy()))
    elif msg['type'] == 'promise':
      if self.state == 'WAIT_PROMISE':
        self.promised.append(msg['source'])
    elif msg['type'] == 'nopromise':
      if self.state == 'WAIT_PROMISE':
        self.nopromised.append(msg['source'])
    elif msg['type'] == 'accepted':
      if self.state == 'WAIT_ACCEPTED':
        self.accepted.append(msg['source'])
    elif msg['type'] == 'rejected':
      if self.state == 'WAIT_ACCEPTED':
        self.rejected.append(msg['source'])
    elif msg['type'] == 'prepare':
      if msg['value'][0] >= self.value[0]:
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
      else:
        self.reqsendjson({'type': 'nopromise',
                            'source': self.name,
                            'destination': [msg['source']],
                            'id': msg['id'],
                            'key': k})
    elif msg['type'] == 'accept':
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
      self.state = 'CONSENSUS'
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
    if msg['key'] not in self.keyrange:
      self.group_tried = [False]*len(self.succ_group)
      msg['destination'] = self.forward_nodes 
      msg['source'] = self.name
      self.reqsendjson(msg) 
    elif not self.leader:
      msg['destination'] = [self.peer_leader]
      msg['source'] = self.name
      self.reqsendjson(msg) 
    else:
      return False
    return True

  def reply(self, msg):
    if msg['origin'] != self.name:
      if msg['origin'] in self.peer_names:
        msg[u'destination'] = [msg['origin']]
      else:
        # Else change to next_names
        msg['destination'] = self.forward_nodes
      msg['type'] = msg['type'][:3] + 'Reply'
      msg['source'] = self.name
      self.reqsendjson(msg)
    else:
      if self.sent_id >= int(msg['id']):
        return
      msg['type'] = msg['type'][:3] + 'Response'
      self.sent_id = int(msg['id'])
      self.reqsendjson(msg)
      self.waiting = False

  def consistentSet(self, k, v, msg):
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

    self.loop.add_timeout(time.time() + 4, lambda: self.checkConsensus(msg.copy()))

  def checkConsensus(self, msg):
    if self.consensus:
      self.store[msg['key']] = self.value
      msg['value'] = self.value[1]
      msg['type'] = 'get'
      self.reply(msg.copy())
    else:
      
      self.reply({'type': msg['type'] + 'Response', 'key': msg['key'], 'id': msg['id'], 'value': 'No consensus reached', 'origin': msg['origin']})

  def handle_leader_elect(self, msg):
    if msg['type'] == 'leaderElec' and msg['origin'] != self.name:
      if self.leaderPromiseID >= msg['leadid'] or (self.leaderPromiseID == msg['leadid'] and self.leaderPromise != msg['origin']):
        self.req.send_json({'type': 'leaderVeto', 'leadid': self.leaderPromiseID, 'id': msg['id'], 'origin': msg['origin'], 'source': self.name, 'destination': [msg['origin']] })
        self.leaderUpdateTime = self.loop.time()
      elif (self.leaderPromiseID < msg['leadid']):
		    self.leaderPromiseID = msg['leadid']
		    self.leaderPromise = msg['origin']
		    self.leaderUpdateTime = self.loop.time()
		    self.req.send_json({'type': 'leaderVote', 'leadid': self.leaderPromiseID, 'id': msg['id'], 'origin': msg['origin'], 'source': self.name, 'destination': [msg['origin']] })
    elif msg['type'] == 'leaderVeto':
      if (msg['leadid'] == self.leaderID):
        self.electionVetos = self.electionVetos + 1
        if self.vetoLeaderID < msg['leadid']:
          self.vetoLeaderID = msg['leadid']
    elif msg['type'] == 'leaderVote':
      if msg['leadid'] == self.leaderID:
        self.electionVotes = self.electionVotes + 1
    elif msg['type'] == 'leaderAccept':
			if ( msg['leadid'] > self.leaderID ):
				self.leaderID = self.leaderPromiseID
				self.old_peer_leader = self.peer_leader
				self.peer_leader = msg['source']
				self.leader = False
				self.old_leader_ref = None
				self.leaderUpdateTime = self.loop.time()
				self.outstanding_acks[:] = [ (i,j) for (i,j) in self.outstanding_acks if i != msg['source']]

				if (self.forwardMessage != None):
				  self.forwardMessage['destination'] = [self.peer_leader]
				  self.handle_message(self.forwardMessage)
				  self.forwardMessage = None
  
  def leaderElection(self,msg):
    if (self.peer_leader == self.old_leader_ref):
      if ( self.loop.time() - self.leaderUpdateTime ) > self.leaderUpdateTimeout:
        self.leaderID = self.leaderID + 1
        self.electionVotes = 0
        self.electionVetos = 0
        self.vetoLeaderID = 0
        
        self.req.send_json({'type': 'leaderElec', 'leadid': self.leaderID, 'id':msg['id'] , 'origin': self.name, 'source': self.name, 'destination': self.peer_names})
        self.loop.add_timeout(self.loop.time() + self.timeoutTime, lambda: self.tallyElection(msg.copy()) ) 
      else:
        self.loop.add_timeout(self.loop.time() + self.timeoutTime, lambda: self.leaderElection(msg.copy()) ) 

  def tallyElection(self,msg):
    if (self.peer_leader == self.old_leader_ref):
      if self.electionVotes > self.electionVetos:
        self.peer_leader = self.name
        self.old_leader_ref = None
        self.leader = True
        self.req.send_json({'type': 'leaderAccept', 'leadid': self.leaderID, 'id':msg['id'] , 'origin': self.name, 'source': self.name, 'destination': self.peer_names})
        if (self.forwardMessage != None):
          self.forwardMessage['destination'] = [self.peer_leader]
          self.handle_message(self.forwardMessage)
          self.forwardMessage = None
      else:
        self.leaderID = self.vetoLeaderID
        self.leaderElection(msg.copy())


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

