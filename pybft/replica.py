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

    # Checkpoint messages
    _CHECKPOINT = "_CHECKPOINT"

    def filter_type(self, xtype, M=None):
        if M is None:
            M = self.in_i

        for msg in M:
            if msg[0] == xtype:
                yield msg


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

        # Initialize checkpoints
        initial_checkpoint = self.to_checkpoint(self.vali, self.last_rep_i, self.last_rep_ti)

        self.checkpts_i = set([(0, initial_checkpoint)])
        for i in range(self.R):
            self.in_i.add( (self._CHECKPOINT, self.view_i, self.last_exec_i, initial_checkpoint, i) )


        # Utility functions

        # Consts
        self.max_out = 100
        self.chkpt_int = 50
        assert self.chkpt_int < self.max_out

    def to_checkpoint(self, vi, rep, rep_t):
        rep_ser = tuple(sorted(rep.items()))
        rep_t_ser = tuple(sorted(rep_t.items()))
        return (vi, rep_ser, rep_t_ser)

    def from_checkpoint(self, chkpt):
        vali, rep_s, rep_t_s = chkpt
        last_rep_i = defaultdict(NoneT).update(rep_s)
        last_rep_ti = defaultdict(int).update(rep_t_s)

        return (vali, last_rep_i, last_rep_ti)

    def valid_sig(self, i, m):
        return True


    def primary(self, v=None):
        if v is None:
            v = self.view_i
        return v % self.R


    def in_v(self, v):
        return self.view_i == v

    def in_w(self, n):
        return 0 < n - self.stable_n() < self.max_out

    def in_wv(self, v, n):
        return self.in_v(v) and self.in_w(n)

    def stable_n(self):
        return min(n for n,_ in self.checkpts_i)

    def stable_chkpt(self):
        vx = min((n,v) for (n,v) in self.checkpts_i)[1]
        assert len(vx) == 3
        return vx


    def has_new_view(self, v):
        if v == 0:
            return True
        else:
            for msg in self.filter_type(self._NEWVIEW):
                if msg[1] == v:
                    return True
            return False

    def take_chkpt(self, n):
        return (n % self.chkpt_int) == 0


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
        for mx in self.filter_type(self._PREPREPARE, M):
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
        (_, _, n, s, C, P, xj) = msg
        # TODO: Check correctness (suspect missing cases)

        ret = True
        ret &= j == xj
        ret &= C == self.compute_C(n, s, C)
        ret &= len(C) > self.f
        ret &= P == self.compute_P(v, P)
        print(P)
        ret &= all(np - n <= self.max_out for (_, _, np, _, _) in P )

        return ret


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
                       xmsg[3] == msg:

                       # print("!!! FOUND PREPREPARE")
                       self.out_i.add(xmsg)


    def receive_preprepare(self, msg):
        (_, v, n, m, j) = msg
        if j == self.i: return

        cond = (self.primary() == j)
        cond &= self.in_wv(v, n)
        cond &= self.has_new_view(v)

        for mx in self.filter_type(self._PREPARE):
            (_, vp, np, dp, ip) = mx
            if (vp, np, ip) == (v, n, self.i):
                cond &= (dp == self.hash(m))

        if cond:
            # Send a prepare message
            p = (self._PREPARE, v, n, self.hash(m), self.i)
            self.in_i |= set([p, msg])
            self.out_i.add(p)

        else:
            # Add the request to the received messages
            if m != None:
                self.in_i.add(m)


    def receive_prepare(self, msg):
        (_, v, n, d, j) = msg
        if j == self.i: return

        if j != self.primary(v) and self.in_wv(v, n):
            self.in_i.add(msg)


    def receive_commit(self, msg):
        (_, v, n, d, j) = msg
        if j == self.i: return

        if self.view_i >= v and self.in_w(n):
            self.in_i.add(msg)


    def receive_checkpoint(self, msg):
        (_, v, n, d, j) = msg
        if j == self.i: return

        if self.view_i >= v and self.in_w(n):
            self.in_i.add(msg)


    def receive_view_change(self, msg):
        (_, v, n, s, C, P, j) = msg
        if j == self.i: return

        ret = self.correct_view_change(msg, v, j)
        assert ret
        if v >= self.view_i and ret:
            self.in_i.add(msg)


    def receive_new_view(self, msg):
        (_, v, X, O, N, j) = msg
        if j == self.i: return False

        
        cond = v >= self.view_i and v > 0

        senders = set()
        for x in X:
            snd = x[-1]
            cond &= self.correct_view_change(x, v, snd)
            senders.add(snd)

        cond &= len(senders) >= 2 * self.f + 1 
        O2, N2, maxV, maxO, used_ns = self.compute_new_view_sets(v, X)
        cond &= N == N2
        cond &= O == O2
        cond &= not self.has_new_view(v)
        

        if cond:

            P = set()
            for msgx in self.filter_type(self._PREPREPARE, O | N):
                (_, vi, ni, mi, _) = msgx
                P.add( (self._PREPARE, v, ni, self.hash(mi), self.i) )

            self.view_i = v
            self.in_i |= (O | N | P)
            self.in_i.add(msg)
            self.out_i |= P
            return True
        else:
            return False


    # Internal transactions

    def send_preprepare(self, m, v, n):
        cond = (self.primary() == self.i)
        cond &= (n == self.seqno_i + 1)
        cond &= self.in_wv(v, n)
        cond &= self.has_new_view(v)
        cond &= m in self.in_i

        # Ensure we only process once.
        cond &= m[0] == self._REQUEST
        for ms in self.filter_type(self._PREPREPARE):
            (_, vp, np, mp, ip) = ms
            cond &= ((vp, mp) != (v, m))

        if cond:
            self.seqno_i = self.seqno_i + 1
            p = (self._PREPREPARE, v, n, m, self.i)
            self.out_i.add(p)
            self.in_i.add(p)

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
                        self.last_rep_i[c], self.vali = None, None # TODO: EXEC
                    rep = (self._REPLY, self.view_i, t, c, self.i, self.last_rep_i[c])
                    self.out_i.add(rep)
            self.in_i.discard(m)
            if self.take_chkpt(n):
                new_chkpt = self.to_checkpoint(self.vali, self.last_rep_i, self.last_rep_ti)
                m = (self._CHECKPOINT, self.view_i, n, new_chkpt, self.i)
                self.in_i.add(m)
                self.out_i.add(m)
                self.checkpts_i.add((n, new_chkpt))

            return True
        else:
            return False


    def compute_P(self, v, M=None):
        if M is None:
            M = self.in_i

        by_ni = {}
        for prep in self.filter_type(self._PREPREPARE, M):
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
                    print(mx[0])
                    if mx[4] != self.primary(vi2):
                        P.add(mx)

        return frozenset(P)

    def compute_C(self, n=None, s=None, M=None):
        if M is None:
            M = self.in_i

        if n is None or s is None:
            n, s = self.stable_n(), self.stable_chkpt(),

        C = set()
        for m in self.filter_type(self._CHECKPOINT, M):
            (_, v, np, dp, j) = m
            if np == n and s == dp:
                C.add(m)
        return frozenset(C)



    def send_viewchange(self, v):
        if v == self.view_i + 1:
            self.view_i = v

            P = self.compute_P(v)
            C = self.compute_C()

            sn, shkpt = self.stable_n(), self.stable_chkpt(),
            msg = (self._VIEWCHANGE, v, sn, shkpt, C, P, self.i)
            self.out_i.add(msg)
            self.in_i.add(msg)
            return True
        else:
            return False


    def compute_new_view_sets(self, v, V):
        mergeP = set()
        maxV = 0
        for (_, _, n, s, C, P, _) in V:
            mergeP |= P
            maxV = max(maxV, n)

        # The set O contains fresh preprepares
        O = set()
        used_ns = set()
        for msg in self.filter_type(self._PREPREPARE, mergeP):
            (_, vi,ni, mi, _) = msg
            if ni > maxV:
                new_prep = (self._PREPREPARE, v, ni, mi, self.primary(v))
                O.add(new_prep)
                used_ns.add(ni)
        O = frozenset(O)

        # The set N contrains nulls for the non-proposed slots
        N = set()

        maxO = 0
        if len(used_ns) > 0:
            maxO =max(used_ns)

        for ni in range(maxV+1, maxO+1):
            if ni not in used_ns:
                new_prep = (self._PREPREPARE, v, ni, None, self.primary(v))
                N.add(new_prep)
        N = frozenset(N)

        return O, N, maxV, maxO, used_ns


    def send_newview(self, v, V):
        cond = (self.primary(v) == self.i)
        cond &= (v >= self.view_i and v > 0)
        for Vi in V:
            cond &= (Vi in self.in_i)
        cond &= len(V) == 2 * self.f + 1
        cond &= not self.has_new_view(v)
        
        who = set()
        same = None
        for Vi in V:
            (xtype, xv, xn, xs, xC, xP, peer_k) = Vi
            cond &= (xtype, xv) == (self._VIEWCHANGE, v)
            cond &= same == None or (xn, xs, xC, xP) == same
            same = (xn, xs, xC, xP)
            who.add(peer_k)
        
        cond &= (len(who) == (2 * self.f + 1))

        if cond:
            (O, N, maxV, maxO, used_ns) = self.compute_new_view_sets(v, V)

            m = (self._NEWVIEW, v, frozenset(V), O, N, self.i)
            self.seqno_i = maxO if maxO > 0 else self.seqno_i
            self.in_i.add(m)
            self.in_i |= O
            self.in_i |= N
            self.out_i.add(m) # TODO clear out_i

            self.update_state_nv(v, V, m, maxV)

            # TODO: Clear all old requests
            #for req in list(self.filter_type(self._REQUEST)):
            #    (_, o, t, c) = req
            #    if t <= self.last_rep_ti[c]:
            #        self.in_i.remove(req)

            return True
        else:
            return False

    def update_state_nv(self, v, V, m, maxV):
        if maxV > self.stable_n():
            (_, _, xn, xs, C, _, _) = V[0]
            if xn == maxV:
                self.in_i |= C

            own_chkpt = (self._CHECKPOINT, v, xn, xs, i)
            if own_chkpt not in self.in_i:
                self.in_i.add(own_chkpt)
                self.out_i.add(own_chkpt)

            for chk in list(self.checkpts_i):
                ni, si = chk
                if ni < maxV:
                    self.checkpts_i.remove(chk)

            if maxV > self.last_exec_i:
                self.checkpts_i.add( (maxV, s) )

            vx = self.stable_chkpt
            self.vali, self.last_rep_i, self.last_rep_ti = from_checkpoint(vx)
            self.last_exec_i = maxV


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

        elif xtype == self._VIEWCHANGE and xlen == 4 + 3:
            self.receive_view_change(msg)

            # Gather related view changes
            V = set()
            for vc_msg in self.in_i:
                if vc_msg[0] == self._VIEWCHANGE and vc_msg[1] == msg[1]:
                    V.add(vc_msg) 
            ret = self.send_newview(msg[1], V)
            if ret:
                # Process hanging requests

                for xmsg in list(self.in_i):
                    if xmsg[0] != self._REQUEST: continue
                    # print("!!! RESEND REQ: %s" % str(xmsg))
                    self.route_receive(xmsg)


        elif xtype == self._NEWVIEW and xlen == 6:
            self.receive_new_view(msg)
            ret = self.has_new_view(self.view_i)
            
            if ret:
                # Process again any 'hanging' requests
                for xmsg in self.filter_type(self._REQUEST):
                    self.route_receive(xmsg)

        else:
            raise Exception("UNKNOWN type: ", msg)

        # Make as much progress as possible
        all_preps = []
        for prep in list(self.in_i):
            if prep[0] != self._PREPREPARE: continue
            if not (prep[1] >= self.view_i and prep[2] >= self.last_exec_i + 1): continue
            all_preps += [ prep ]

        all_preps = sorted(all_preps, key=lambda xmsg: xmsg[2])

        for (_, vx, nx, mx, _) in all_preps:
            self.send_commit(mx,vx,nx)
            self.execute(mx,vx,nx)

            
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