# Tests

import sys
sys.path += ["."]

from pybft.replica import replica

def test_replica_init():
    r = replica(0, 4)
    
def test_replica_request():
    r = replica(0, 4)

    request = (r._REQUEST, b"message", 0, b"100")
    r.receive_request(request)

def test_replica_preprepare():
    r = replica(0, 4)

    request = (r._REQUEST, b"message", 0, b"100")
    request2 = (r._REQUEST, b"message2", 0, b"101")

    r.receive_request(request)
    L0 = len(r.out_i)

    # Make progress
    r.send_preprepare(request, 0, 1)
    L1 = len(r.out_i)
    assert L1 > L0

    # Cannot re-use the same seq number.
    r.send_preprepare(request, 0, 1)
    L2 = len(r.out_i)
    assert L2 == L1

    # Cannot re-issue the same req in the same view
    r.send_preprepare(request, 0, 2)
    L2 = len(r.out_i)
    assert L2 == L1

    # Cannot re-use the same seq number.
    r.send_preprepare(request2, 0, 1)
    L2 = len(r.out_i)
    assert L2 == L1

    # Has not received request
    r.send_preprepare(request2, 0, 2)
    L2 = len(r.out_i)
    assert L2 == L1
    
    # Register request
    r.receive_request(request2)
    L0 = len(r.out_i)

    # make progress
    r.send_preprepare(request2, 0, 2)
    L2 = len(r.out_i)
    assert L2 > L0

def test_replica_prepare():
    r1 = replica(1, 4)

    request = (r1._REQUEST, b"message", 0, b"100")
    request2 = (r1._REQUEST, b"message2", 0, b"101")

    prepr = (r1._PREPREPARE, 0, 1, request, 0)
    prepr2 = (r1._PREPREPARE, 0, 1, request2, 0)

    # prepare the first time
    L0 = len(r1.out_i)
    r1.receive_preprepare(prepr)
    L1 = len(r1.out_i)
    assert L1 > L0

    # Do not prepare twice
    L0 = len(r1.out_i)
    r1.receive_preprepare(prepr)
    L1 = len(r1.out_i)
    assert L1 == L0

    # Do not prepare another req at the same position
    L0 = len(r1.out_i)
    r1.receive_preprepare(prepr2)
    L1 = len(r1.out_i)
    assert L1 == L0

def test_proposed():
    r = replica(0, 4)

    request = (r._REQUEST, b"message", 0, b"100")
    prepr = (r._PREPREPARE, 0, 1, request, 0)
    p1 = (r._PREPARE, 0, 1, r.hash(request), 1)    

    M = set([prepr, p1])
    assert not r.prepared(request, 0, 1, M)
    
    p2 = (r._PREPARE, 0, 1, r.hash(request), 2)
    M.add(  p2 )
    assert r.prepared(request, 0, 1, M)

    p3 = (r._PREPARE, 0, 1, r.hash(request), 3)
    M.add(  p3 )
    assert r.prepared(request, 0, 1, M)

    M = set([p1, p2, p3])
    assert not r.prepared(request, 0, 1, M)

    p0 = (r._PREPARE, 0, 1, r.hash(request), 0)    
    M = set([prepr, p0, p1])
    assert not r.prepared(request, 0, 1, M)

def test_committed():
    r = replica(0, 4)

    request = (r._REQUEST, b"message", 0, b"100")
    prepr = (r._PREPREPARE, 0, 1, request, 0)
    p0 = (r._COMMIT, 0, 1, r.hash(request), 0)    
    p1 = (r._COMMIT, 0, 1, r.hash(request), 1)    
    p2 = (r._COMMIT, 0, 1, r.hash(request), 2)    

    M = set([prepr, p0, p1, p2])
    assert r.commited(request, 0, 1, M)

    M = set([p0, p1, p2])
    assert not r.commited(request, 0, 1, M)

    M = set([request, p0, p1, p2])
    assert r.commited(request, 0, 1, M)

    M = set([prepr, p0, p1])
    assert not r.commited(request, 0, 1, M)

def test_all_transitions():
    for _ in range(100):
        replicas = [ replica(i, 4) for i in range(4) ]

        mvns = {}

        # This is a massive hack: we set all the out buffers to the same
        # one and take control of the scheduling of messages
        global_out = set()
        for r in replicas:
            r.out_i = global_out

        request = (r._REQUEST, b"message", 0, b"100")

        import random
        r = random.choice(replicas)
        r.route_receive(request)
        

        seen_replies = set()

        def route_to(msgs):
            rx = replica(0,4)
            Dest = []
            for m in msgs:
                if m[0] == rx._REQUEST:
                    Dest += [(replicas[0], m)]
                elif m[0] == rx._REPLY:
                    seen_replies.add(m[1:4])
                elif m[0] == rx._PREPREPARE:
                    Dest += [(r, m) for r in replicas[1:]]
                else:
                    Dest += [(r, m) for r in replicas if r.i != m[-1]]
            msgs.clear()
            return Dest

        D = route_to(global_out)
        while len(D) > 0:
            #print("Message volume: ", len(D))
            dest, msg = random.choice(D)
            #print("%s -> %s" % (dest.i, str(msg)))
            dest.route_receive(msg)
            
            D.remove((dest, msg))
            D += route_to(global_out)
        assert len(seen_replies) == 1

def test_all_transitions_for_two():
    for _ in range(100):
        replicas = [ replica(i, 4) for i in range(4) ]

        mvns = {}

        # This is a massive hack: we set all the out buffers to the same
        # one and take control of the scheduling of messages
        global_out = set()
        for r in replicas:
            r.out_i = global_out

        request1 = (r._REQUEST, b"message1", 0, b"100")
        request2 = (r._REQUEST, b"message2", 0.5, b"101")

        import random
        rand = random.choice(replicas)
        rand.route_receive(request1)
        rand.route_receive(request2)
        

        seen_replies = set()

        def route_to(msgs):
            rx = replica(0,4)
            Dest = []
            for m in msgs:
                if m[0] == rx._REQUEST:
                    Dest += [(replicas[0], m)]
                elif m[0] == rx._REPLY:
                    seen_replies.add(m[1:4])
                elif m[0] == rx._PREPREPARE:
                    Dest += [(r, m) for r in replicas[1:]]
                else:
                    Dest += [(r, m) for r in replicas if r.i != m[-1]]
            msgs.clear()
            return Dest

        D = route_to(global_out)
        while len(D) > 0:
            #print("Message volume: ", len(D))
            dest, msg = random.choice(D)
            #print("%s -> %s" % (dest.i, str(msg)))
            dest.route_receive(msg)
            
            D.remove((dest, msg))
            D += route_to(global_out)
        # print(seen_replies)
        assert len(seen_replies) == 2

def test_view_change():
    replicas = [ replica(i, 4) for i in range(4) ]

    mvns = {}

    # This is a massive hack: we set all the out buffers to the same
    # one and take control of the scheduling of messages
    global_out = set()
    for r in replicas:
        r.out_i = global_out

    request1 = (r._REQUEST, b"message1", 0, b"100")
    request2 = (r._REQUEST, b"message2", 0.5, b"101")

    import random
    rand = random.choice(replicas)
    rand.route_receive(request1)
    rand.route_receive(request2)
    

    seen_replies = set()

    def route_to(msgs):
        rx = replica(0,4)
        Dest = []
        for m in msgs:
            if m[0] == rx._REQUEST:
                Dest += [(replicas[0], m)]
            elif m[0] == rx._REPLY:
                seen_replies.add(m[1:4])
            elif m[0] == rx._PREPREPARE:
                Dest += [(r, m) for r in replicas[1:]]
            else:
                Dest += [(r, m) for r in replicas if r.i != m[-1]]
        msgs.clear()
        return Dest

    D = route_to(global_out)
    while len(D) > 0:
        #print("Message volume: ", len(D))
        dest, msg = random.choice(D)
        #print("%s -> %s" % (dest.i, str(msg)))
        dest.route_receive(msg)
        
        D.remove((dest, msg))
        D += route_to(global_out)
    # print(seen_replies)
    assert len(seen_replies) == 2

    RT = replicas[1]
    RT.send_viewchange(1)
    assert len(RT.out_i) == 1

    msg = list(RT.out_i)[0]
    mxs = [mx for mx in msg[2] if mx[0] == RT._PREPREPARE]
    assert len(mxs) == 2

    for (_, vi, ni, mi, _) in mxs:
        assert RT.prepared(mi, vi, ni, msg[2])

    RT2 = replicas[2]
    L0 = len(RT2.in_i)
    RT2.receive_view_change(msg)
    L1 = len(RT2.in_i)
    assert L1 > L0

    L0 = len(RT.in_i)
    RT.receive_view_change(msg)
    L1 = len(RT.in_i)
    assert L1 == L0


    RT2.send_viewchange(1)
    RT3 = replicas[3]
    RT3.send_viewchange(1)
    
    
    V = frozenset(RT3.out_i)
    assert len(V) == 3

    for msg in V:
        RT.receive_view_change(msg)


    global_out.clear()
    RT.send_newview(1, V)

    assert len(RT.out_i) == 1
    newview = RT.out_i.pop()

    assert RT2.receive_new_view(newview)

def test_view_change_full():
    for _ in range(10):
        replicas = [ replica(i, 4) for i in range(4) ]
        mvns = {}

        # This is a massive hack: we set all the out buffers to the same
        # one and take control of the scheduling of messages
        global_outs = [set() for _ in replicas]

        for r, oi in zip(replicas,global_outs):
            r.out_i = oi

        request1 = (r._REQUEST, b"message1", 0, b"100")
        request2 = (r._REQUEST, b"message2", 0.5, b"101")

        import random
        rand = random.choice(replicas)
        rand.route_receive(request1)
        rand.route_receive(request2)

        for i in range(1,4):
            replicas[i].send_viewchange(1)    

        seen_replies = set()

        def route_to(msgs, i):
            # rx = replica(0,4)
            Dest = []
            for m in msgs:
                if m[0] == replica._REQUEST:
                    primary = replicas[i].primary()
                    Dest += [(replicas[primary], m)]
                elif m[0] == replica._REPLY:
                    seen_replies.add(m[1:4])
                else:
                    Dest += [(r, m) for r in replicas if r.i != m[-1]]
            msgs.clear()
            return Dest

        D = sum([route_to(oi,i) for i,oi in enumerate(global_outs)],[]) # route_to(global_out)
        LOG = []
        while len(D) > 0:
            #print("Message volume: ", len(D))
            dest, msg = random.choice(D)
            dest.route_receive(msg)
            LOG += [("%s -> %s" % ( str(msg), dest.i))]
            LOG += [(["V%d:%d" % (j, rep.view_i) for j,rep in enumerate(replicas)])]

            D.remove((dest, msg))
            D += sum([route_to(oi,i) for i,oi in enumerate(global_outs)],[]) # route_to(global_out)

        # print(seen_replies)

        if not len(seen_replies) == 2:
            for line in LOG:
                print(line)

            for req in [request1, request2]:
                print("------" * 5)
                for R in replicas:
                    R._debug_status(req)

        assert len(seen_replies) == 2

from collections import defaultdict
import random

class driver():
    def __init__(self, f=1):
        n = 3*f+1
        self.replicas = [replica(i, n) for i in range(n)]

        self.global_outs = [r.out_i for r in self.replicas]

        self.seen_replies = set()
        self.message_numbers = defaultdict(int)

        self.D = []
        self.LOG = []


    def route_to(self):
        # rx = replica(0,4)
        for i, msgs in enumerate(self.global_outs):
            Ds = []
            for m in msgs:
                if m[0] == replica._REQUEST:
                    primary = self.replicas[i].primary()
                    Ds += [(self.replicas[primary], m)]
                elif m[0] == replica._REPLY:
                    self.seen_replies.add(m[1:4])
                else:
                    Ds += [(r, m) for r in self.replicas if r.i != m[-1]]
            self.message_numbers[i] += len(Ds)
            self.D += Ds
            msgs.clear()

    def execute(self):
        self.route_to()
        while len(self.D) > 0:
            #print("Message volume: ", len(D))
            dest, msg = random.choice(self.D)
            dest.route_receive(msg)
            self.LOG += [("%s -> %s" % ( str(msg), dest.i))]
            self.LOG += [(["V%d:%d" % (j, rep.view_i) for j,rep in enumerate(self.replicas)])]

            self.D.remove((dest, msg))
            self.route_to()


def test_driver_for_f3():
    dvr = driver(3)    

    request1 = (replica._REQUEST, b"message1", 0, b"100")
    request2 = (replica._REQUEST, b"message2", 0.5, b"101")

    rand = random.choice(dvr.replicas)
    rand.route_receive(request1)
    rand.route_receive(request2)

    dvr.execute()

    assert len(dvr.seen_replies) == 2
    # print(dvr.message_numbers)


def test_view_change_cost():
    for _ in range(10):
        replicas = [ replica(i, 4) for i in range(4) ]
        mvns = {}

        # This is a massive hack: we set all the out buffers to the same
        # one and take control of the scheduling of messages
        global_outs = [r.out_i for r in replicas]

        request1 = (replica._REQUEST, b"message1", 0, b"100")
        request2 = (replica._REQUEST, b"message2", 0.5, b"101")

        import random
        rand = random.choice(replicas)
        rand.route_receive(request1)
        rand.route_receive(request2)

        for i in range(1,4):
            replicas[i].send_viewchange(1)    

        seen_replies = set()
        message_numbers = defaultdict(int)        

        def route_to(msgs, i):
            # rx = replica(0,4)
            Dest = []
            for m in msgs:
                if m[0] == replica._REQUEST:
                    primary = replicas[i].primary()
                    Dest += [(replicas[primary], m)]
                elif m[0] == replica._REPLY:
                    seen_replies.add(m[1:4])
                else:
                    Dest += [(r, m) for r in replicas if r.i != m[-1]]
            message_numbers[i] += len(Dest)
            msgs.clear()
            return Dest

        D = sum([route_to(oi,i) for i,oi in enumerate(global_outs)],[]) # route_to(global_out)
        LOG = []
        while len(D) > 0:
            #print("Message volume: ", len(D))
            dest, msg = random.choice(D)
            dest.route_receive(msg)
            LOG += [("%s -> %s" % ( str(msg), dest.i))]
            LOG += [(["V%d:%d" % (j, rep.view_i) for j,rep in enumerate(replicas)])]

            D.remove((dest, msg))
            D += sum([route_to(oi,i) for i,oi in enumerate(global_outs)],[]) # route_to(global_out)

        # print(seen_replies)

        if not len(seen_replies) == 2:
            for line in LOG:
                print(line)

            for req in [request1, request2]:
                print("------" * 5)
                for R in replicas:
                    R._debug_status(req)

        assert len(seen_replies) == 2
        # print(message_numbers)