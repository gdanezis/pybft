# Implements a core pBFT replica as a state machine. 
# Follows the formal specification at: 
# https://www.microsoft.com/en-us/research/wp-content/uploads/2017/01/tm590.pdf

from collections import defaultdict
from hashlib import sha256

NoneT = lambda: None



def _C(cond, msg):
    if not cond:
        print(msg)
    return cond

class replica(object):

    _PREPREPARE = "_PREPREPARE" # 1000
    _PREPARE    = "_PREPARE" # 1001
    _REPLY      = "_REPLY" # 1002
    _REQUEST    = "_REQUEST" # 1003
    _COMMIT     = "_COMMIT" # 1004
    _VIEWCHANGE = "_VIEWCHANGE" # 1005
    _NEWVIEW    = "_NEWVIEW"

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
        if v == 0:
            return True
        else:
            for msg in self.in_i:
                if msg[0] != self._NEWVIEW: continue
                if msg[1] == v:
                    return True
            return False

    def hash(self, m):
        t = ("%2.2f" % m[2]).encode("utf-8")
        bts = m[1] + b"||" + t + b"||" + m[3] # TODO: fix formatting
        return sha256(bts).hexdigest()

    def prepared(self, m, v, n, M=None):
        if M is None:
            M = self.in_i

        cond = (self._PREPREPARE, v, n, m, self.primary(v)) in M
        
        others = set()
        for mx in M: 
            if mx[:4] == (self._PREPARE, v, n, self.hash(m)):
                if mx[4] != self.primary(v):
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

    def correct_view_change(self, msg, v, j):
        (_, _, P, _) = msg
        return P == self.compute_P(v, P)


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

            ## Liveness hack. TODO: check it.
            else: 
                # If we are the primary, and have send a 
                # preprepare message for this request, send it
                # again here.

                for xmsg in self.in_i:
                    if xmsg[0] == self._PREPREPARE and \
                       xmsg[1] == self.view_i and \
                       xmsg[3] == self.hash(msg):

                       print("!!! FOUND PREPREPARE")
                       self.out_i.add(xmsg)


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
            if m != None:
                self.in_i.add(m)

    def receive_prepare(self, msg):
        (_, v, n, d, j) = msg
        if j == self.i: return

        if j != self.primary(v) and self.in_v(v):
            self.in_i.add(msg)

    def receive_commit(self, msg):
        (_, v, n, d, j) = msg
        if j == self.i: return

        if self.view_i >= v:
            self.in_i.add(msg)

    def receive_view_change(self, msg):
        (_, v, P, j) = msg
        if j == self.i: return

        if v >= self.view_i and self.correct_view_change(msg, v, j):
            self.in_i.add(msg)

    def receive_new_view(self, msg):
        (_, v, X, O, N, j) = msg
        if j == self.i: return False


        P = set()

        new_mvn = []
        for msgx in O | N:
            if msgx[0] != self._PREPREPARE: continue

            (_, vi, ni, mi, _) = msgx
            P.add( (self._PREPARE, v, ni, self.hash(mi), self.i) )
            new_mvn += [(mi, v, ni)]

        cond = v >= self.view_i and v > 0
        assert cond

        mergeP = set()
        for (_, _, P, _) in X:
            mergeP |= P

        # The set O contains fresh preprepares
        O2 = set()
        used_ns = set()
        for msgx in mergeP:
            if msgx[0] != self._PREPREPARE:
                continue
            (_, vi,ni, mi, _) = msgx
            new_prep = (self._PREPREPARE, v, ni, mi, self.primary(v))
            O2.add(new_prep)
            used_ns.add(ni)
        O2 = frozenset(O2)

        cond &= O == O2
        assert cond

        # The set N contrains nulls for the non-proposed slots
        N2 = set()

        minO, maxO = 0, 0
        if len(used_ns) > 0:
            minO, maxO = min(used_ns), max(used_ns) + 1

        for ni in range(minO, maxO):
            if ni not in used_ns:
                new_prep = (self._PREPREPARE, v, ni, None, self.primary(v))
                N.add(new_prep)
        N2 = frozenset(N2)

        cond &= N == N2
        assert cond

        cond &= not self.has_new_view(v)
        assert cond

        if cond:
            self.view_i = v
            self.in_i |= (O | N | P)
            self.in_i.add(msg)
            self.out_i |= P

            # Update unofficial
            for (mi, vi, ni) in new_mvn:
                self.mnv_store[(vi, ni)] = mi

            return True
        else:
            return False


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

    def compute_P(self, v, M=None):
        if M is None:
            M = self.in_i

        by_ni = {}
        for prep in M:
            if prep[0] != self._PREPREPARE:
                continue
            (_, vi,ni, mi, _) = prep

            if self.prepared(mi, vi, ni, M):
                if ni not in by_ni:
                    by_ni[ni] = prep
                else:
                    if by_ni[1] < vi:
                        by_ni[n1] = prep

        P = set()
        for prep in by_ni.values():
            P.add(prep)
            (_, vi2,ni2, mi2, _) = prep

            for mx in self.in_i: 
                if mx[:4] == (self._PREPARE, vi2, ni2, self.hash(mi2)):
                    if mx[4] != self.primary(vi2):
                        P.add(mx)

        return frozenset(P)


    def send_viewchange(self, v):
        if v == self.view_i + 1:
            self.view_i = v

            P = self.compute_P(v)

            msg = (self._VIEWCHANGE, v, P, self.i)
            self.out_i.add(msg)
            self.in_i.add(msg)
            return True
        else:
            return False

    def send_newview(self, v, V):
        cond = (self.primary(v) == self.i)
        cond &= (v >= self.view_i and v > 0)
        for Vi in V:
            cond &= (Vi in self.in_i)
        cond &= len(V) == 2 * self.f + 1
        cond &= not self.has_new_view(v)
        
        who = set()
        for Vi in V:
            cond &= Vi[:2] == (self._VIEWCHANGE, v)
            who.add(Vi[3])
        
        cond &= (len(who) == (2 * self.f + 1))

        if cond:
            mergeP = set()
            for (_, _, P, _) in V:
                mergeP |= P

            # The set O contains fresh preprepares
            O = set()
            used_ns = set()
            for msg in mergeP:
                if msg[0] != self._PREPREPARE:
                    continue
                (_, vi,ni, mi, _) = msg
                new_prep = (self._PREPREPARE, v, ni, mi, self.i)
                O.add(new_prep)
                used_ns.add(ni)
            O = frozenset(O)

            # The set N contrains nulls for the non-proposed slots
            N = set()

            minO, maxO = 0, 0
            if len(used_ns) > 0:
                minO, maxO = min(used_ns), max(used_ns) + 1

            for ni in range(minO, maxO):
                if ni not in used_ns:
                    new_prep = (self._PREPREPARE, v, ni, None, self.i)
                    N.add(new_prep)
            N = frozenset(N)

            m = (self._NEWVIEW, v, frozenset(V), O, N, self.i)
            self.seqno_i = max(used_ns) if len(used_ns) > 0 else self.seqno_i
            self.in_i.add(m)
            self.in_i |= O
            self.in_i |= N
            self.out_i.add(m) # TODO clear out_i

            return True
        else:
            return False


    # System's calls

    def route_receive(self, msg):
        xtype = msg[0]
        xlen = len(msg)
        if xtype == self._REQUEST and xlen == 4:
            self.receive_request(msg)
            ret = self.send_preprepare(msg, self.view_i, self.seqno_i+1)
            
        elif xtype == self._PREPREPARE and xlen == 5:
            self.receive_preprepare(msg)

        elif xtype == self._PREPARE and xlen == 5:
            self.receive_prepare(msg)
            
        elif xtype == self._COMMIT and xlen == 5:
            self.receive_commit(msg)

        elif xtype == self._VIEWCHANGE and xlen == 4:
            self.receive_view_change(msg)

            # Gather related view changes
            V = set()
            for vc_msg in self.in_i:
                if vc_msg[0] == self._VIEWCHANGE and vc_msg[1] == msg[1]:
                    V.add(vc_msg) 
            self.send_newview(msg[1], V)

        elif xtype == self._NEWVIEW and xlen == 6:
            ret = self.receive_new_view(msg)
            
            # Process again any 'hanging' requests
            for xmsg in self.in_i:
                if xmsg[0] != self._REQUEST: continue
                self.route_receive(xmsg)

        else:
            print("UNKNOWN: ", msg)
            print("UNKNOWN LEN: ", len(msg))
            assert False

        # Make as much progress as possible
        all_preps = []
        for prep in list(self.in_i):
            if prep[0] != self._PREPREPARE: continue
            if not (prep[1] >= self.view_i and prep[2] >= self.last_exec_i + 1): continue
            all_preps += [ prep ]

        all_preps = sorted(all_preps, key=lambda xmsg: xmsg[2])

        for (_, vx, nx, mx, _) in all_preps:
            # v,n,m = prep[1:4]
            self.send_commit(mx,vx,nx)
            self.execute(mx,vx,nx)
            # n += 1

    def _debug_status(self, request):
        # First check out if the request has been received:
        print("\nPeer %s (view: %s) REQ: %s" % (self.i, self.view_i, str(request)))
        accounted = set()
        for msg in self.in_i:
            if msg[0] == self._PREPREPARE and msg[3] == request:
                accounted.add( (msg[1], msg[2]) )
                print("** %s" % (str(msg)))
                if self.prepared(request, msg[1], msg[2]):
                    print("        ** PREPARED **")

                # How many prepeared do we have
                for Pmsg in self.in_i:
                    if Pmsg[0] == self._PREPARE and Pmsg[1:3] == msg[1:3]:
                        print("        %s" % str(Pmsg))

                commited = False
                if self.commited(request, msg[1], msg[2]):
                    print("        ** COMMITED **")
                    commited = True

                # How many prepeared do we have
                for Pmsg in self.in_i:
                    if Pmsg[0] == self._COMMIT and Pmsg[1:3] == msg[1:3]:
                        print("        %s" % str(Pmsg))

                if commited:
                    if self.last_exec_i >= msg[2]:
                        print("        ** EXECUTED (%s) **" % self.last_exec_i)
                    else:
                        print("        ** NOT EXECUTED (%s) **" % self.last_exec_i)
            
        # How many prepeared do we have
        for Pmsg in self.in_i:
            if Pmsg[0] == self._PREPARE and Pmsg[3] == self.hash(request):
                if Pmsg[1:3] not in accounted:
                    print("STRAY: %s" % str(Pmsg))