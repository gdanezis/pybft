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
        for mx in self.in_i:
            if mx[0] == self._PREPREPARE:
                (_, vp, np, mp, jp) = mx
                cond |= (np, mp) == (n, m) and (jp == self.primary(vp))
        cond |= m in self.in_i
        
        others = set()
        for mx in M: 
            if mx[:4] == ("COMMIT", v, n, self.hash(m)):
                if mx[4] != self.primary():
                    others.add(mx[4])

        cond &= len(others) >= 2*self.f + 1
        return cond


    # Input transactions

    def receive_request(self, msg):
        (_, o, t, c) = msg

        # We have already replied to the message
        if t == self.last_rep_i[c]:
            new_reply = ("REPLY", self.view_i, t, c, self.i, last_rep_i[c])
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

        for m in self.in_i:
            if m[0] == self._PREPARE:
                (_, vp, np, dp, ip) = m
                if (vp, np, ip) == (v, n, self.i):
                    cond &= (dp == self.hash(m))

        if cond:
            # Send a prepare message
            p = (self._PREPARE, v, n, self.hash(m), self.i)
            self.in_i |= set([p, msg])
            self.out_i.add(p)
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
        cond &= m[0] == "REQUEST"
        for ms in self.in_i:
            if ms[0] == self._PREPREPARE:
                (_, vp, np, mp, ip) = ms
                cond &= ((vp, mp) != (v, m))

        if cond:
            self.seqno_i = self.seqno_i + 1
            p = (self._PREPREPARE, v, n, m, self.i)
            self.out_i.add(p)
            self.in_i.add(p)

    def send_commit(self, m, v, n):
        c = ("COMMIT", v, n, self.hash(m), self.i)
        if self.prepared(m,v,n) and c not in self.in_i:
            self.out_i.add(c)
            self.in_i.add(c)

    def execute(self, m, v, n):
        if n == self.last_exec_i + 1 and self.commited(m, v, n):
            self.last_exec_i = n
            if m != None: # TODO: check null representation
                (_, o, t, c) = m
                if t >= self.last_rep_ti[c]:
                    if t > self.last_rep_ti[c]:
                        self.last_rep_ti[c] = t
                        self.last_rep_i[c], self.vali = None, None # EXEC
                    rep = ("REPLY", self.view_i, t, c, self.i, self.last_rep_i[c])
                    self.out_i.add(rep)
            self.in_i.remove(m)

