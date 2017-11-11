# Implements a core pBFT replica as a state machine. 
# Follows the formal specification at: 
# https://www.microsoft.com/en-us/research/wp-content/uploads/2017/01/tm590.pdf

from collections import defaultdict

NoneT = lambda: None



def _C(cond, msg):
    if not cond:
        print(msg)
    return cond

class replica(object):

    _PREPREPARE = 1000
    _PREPARE    = 1001
    _REPLY      = 1002
    _REQUEST    = 1003
    _COMMIT     = 1004

    def __init__(self,i, R):
        self.i = i
        self.R = R
        self.f = (R - 1) // 3
        self.vali = None # v_0
        self.view_i = 0
        self.in_i = set()
        self.out_i = set()
        self.last_rep_i = defaultdict(NoneT)
        self.last_rep_ti = defaultdict(int)
        self.seqno_i = 0
        self.last_exec_i = 0

        # Not quite the official state, but useful
        # to schedule internal things.
        self.mnv_store = {}

    # Utility functions

    def valid_sig(self, i, m):
        return True

    def primary(self, v=None):
        if v is None:
            v = self.view_i
        return v % self.R

    def in_v(self, v):
        return self.view_i == v

    def has_new_view(self, v):
        return v == 0 # TODO: new view messages

    def hash(self, m):
        return m

    def prepared(self, m, v, n, M=None):
        if M is None:
            M = self.in_i

        cond = (self._PREPREPARE, v, n, m, self.primary()) in M
        
        others = set()
        for mx in M: 
            if mx[:4] == (self._PREPARE, v, n, self.hash(m)):
                if mx[4] != self.primary():
                    others.add(mx[4])

        cond &= len(others) >= 2*self.f
        return cond

    def commited(self, m, v, n, M=None):
        if M is None:
            M = self.in_i

        cond = False
        for mx in M:
            if mx[0] == self._PREPREPARE:
                (_, vp, np, mp, jp) = mx
                cond |= (np, mp) == (n, m) and (jp == self.primary(vp))
        cond |= m in M
        
        others = set()
        for mx in M: 
            if mx[:4] == (self._COMMIT, v, n, self.hash(m)):
                others.add(mx[4])

        cond &= len(others) >= 2*self.f + 1
        return cond


    # Input transactions

    def receive_request(self, msg):
        (_, o, t, c) = msg

        # We have already replied to the message
        if t == self.last_rep_i[c]:
            new_reply = (self._REPLY, self.view_i, t, c, self.i, last_rep_i[c])
            self.out_i.add( new_reply )
        else:
            self.in_i.add( msg )
            # If not the primary, send message to all.
            if self.primary() != self.i:
                self.out_i.add( msg )


    def receive_preprepare(self, msg):
        (_, v, n, m, j) = msg
        if j == self.i: return

        cond = (self.primary() == j)
        cond &= self.in_v(v)
        cond &= self.has_new_view(v)

        for mx in self.in_i:
            if mx[0] == self._PREPARE:
                (_, vp, np, dp, ip) = mx
                if (vp, np, ip) == (v, n, self.i):
                    cond &= (dp == self.hash(m))

        if cond:
            # Send a prepare message
            p = (self._PREPARE, v, n, self.hash(m), self.i)
            self.in_i |= set([p, msg])
            self.out_i.add(p)

            # Unofficial state
            self.mnv_store[(v,n)] = m
        else:
            # Add the request to the received messages
            self.in_i.add(m)

    def receive_prepare(self, msg):
        (_, v, n, d, j) = msg
        if j == self.i: return

        if j != self.primary() and self.in_v(v):
            self.in_i.add(msg)

    def receive_commit(self, msg):
        (_, v, n, d, j) = msg
        if j == self.i: return

        if self.view_i >= v:
            self.in_i.add(msg)


    # Internal transactions

    def send_preprepare(self, m, v, n):
        cond = (self.primary() == self.i)
        cond &= (n == self.seqno_i + 1)
        cond &= self.in_v(v)
        cond &= self.has_new_view(v)
        cond &= m in self.in_i

        # Ensure we only process once.
        cond &= m[0] == self._REQUEST
        for ms in self.in_i:
            if ms[0] == self._PREPREPARE:
                (_, vp, np, mp, ip) = ms
                cond &= ((vp, mp) != (v, m))

        if cond:
            self.seqno_i = self.seqno_i + 1
            p = (self._PREPREPARE, v, n, m, self.i)
            self.out_i.add(p)
            self.in_i.add(p)

            # Unofficial state
            self.mnv_store[(v,n)] = m
            return True
        else:
            return False

    def send_commit(self, m, v, n):
        c = (self._COMMIT, v, n, self.hash(m), self.i)
        if self.prepared(m,v,n) and c not in self.in_i:
            self.out_i.add(c)
            self.in_i.add(c)
            return True
        else:
            return False

    def execute(self, m, v, n):
        if n == self.last_exec_i + 1 and self.commited(m, v, n):
            self.last_exec_i = n
            if m != None: # TODO: check null representation
                (_, o, t, c) = m
                if t >= self.last_rep_ti[c]:
                    if t > self.last_rep_ti[c]:
                        self.last_rep_ti[c] = t
                        self.last_rep_i[c], self.vali = None, None # EXEC
                    rep = (self._REPLY, self.view_i, t, c, self.i, self.last_rep_i[c])
                    self.out_i.add(rep)
            self.in_i.discard(m)
            return True
        else:
            return False



    # System's calls

    def route_receive(self, msg):
        xtype = msg[0]
        xlen = len(msg)
        if xtype == self._REQUEST and xlen == 4:
            self.receive_request(msg)
            self.send_preprepare(msg, self.view_i, self.seqno_i+1)

        elif xtype == self._PREPREPARE and xlen == 5:
            self.receive_preprepare(msg)

        elif xtype == self._PREPARE and xlen == 5:
            self.receive_prepare(msg)
            
        elif xtype == self._COMMIT and xlen == 5:
            self.receive_commit(msg)

        else:
            pass

        # Make as much progress as possible
        n = self.last_exec_i + 1
        v = self.view_i
        while (v,n) in self.mnv_store:
            m = self.mnv_store[(v,n)]
            cond = True
            if cond:
                cond = self.send_commit(m,v,n)
            if cond:
                cond = self.execute(m,v,n)
            n += 1

    def action_send(self, msg):
        pass
