# Implements a core pBFT replica as a state machine. 
# Follows the formal specification at: 
# https://www.microsoft.com/en-us/research/wp-content/uploads/2017/01/tm590.pdf

from collections import defaultdict

NoneT = lambda: None

class replica(object):

	def __init__(self):
		self.vali = None # v_0
		self.view_i = 0
		self.in_i = set()
		self.out_i = set()
		self.last_rep_i = defaultdict(NoneT)
		self.last_rep_ti = defaultdict(int)
		self.seqno_i = 0
		self.last_exec_i = 0


def test_replica_init():
	r = replica()
