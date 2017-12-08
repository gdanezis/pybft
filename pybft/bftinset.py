""" This is a specialized set abstraction that makes the handling of the `in' set
efficient for pBFT processing"""

from collections import defaultdict

class inset():

    def __init__(self, items=None):
        self.sets = defaultdict(set)
        self.types = set()
        if items is not None:
            for i in items:
                self.add(i)

            self.types |= set(i[0] for i in self.items)

    def reset_hints(self):
        self.types = set()

    def hint(self, xtype):
        return xtype in self.types

    def add(self, item):
        self.sets[item[0]].add(item)
        self.types.add(item[0])


    def __contains__(self, item):
        return self.sets[item[0]].__contains__(item)

    def __ior__(self, other):
        for o in other:
            self.add(o)
        return self

    def discard(self, item):
        self.sets[item[0]].discard(item)

    def __isub__(self, other):
        for o in other:
            self.sets[o[0]].discard(o)
        return self

    def __iter__(self):
        for s in self.sets:
            for x in self.sets[s]:
                yield x

    def __len__(self):
        return sum(len(s) for s in self.sets.values())






def test_init():
    s = inset()
    s.add(("Hello", 1))
    s.add(("Hello", 2))
    s.add(("World", 2))

    assert ("World", 2) in s
    assert ("World", 3) not in s

    s2 = inset([("xx", 1),("yy", 2) ])
    assert ("xx", 1) in s2
    assert ("zz", 1) not in s2

    s2 |= [("aa", 45), ("bb", 43)]
    assert ("aa", 45) in s2
    s2 -= [("aa", 45)]
    assert ("aa", 45) not in s2
    assert ("bb", 43) in s2

    assert set(s2) == set([("xx", 1),("yy", 2),("bb", 43)])

