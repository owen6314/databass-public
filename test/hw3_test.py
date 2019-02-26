"""
HW3 Unit Test
Test correct implementation of query compilation for
(Limit, Projection with aggregation, Hash join)
"""
import unittest
from databass import *
from databass.tables import InMemoryTable


class TestUnits(unittest.TestCase):
  def setUp(self):
    self.db = Database.db()
    self.opt = Optimizer()

  def test_groupby(self):
    qs = ["SELECT count(b) FROM data GROUP BY c",
          """SELECT d1.a, count(d2.b), sum(d2.b)
          FROM data as d1, data as d2
          WHERE d1.a = d2.b 
          GROUP BY d1.a"""]
    for q in qs:
        self.run_query(q, False)
  
  def test_limit(self):
    qs = ["SELECT * FROM data LIMIT 5",
          """SELECT DISTINCT d1.a FROM data as d1, data as d2
          WHERE d1.a = d2.b LIMIT 2"""]
    for q in qs:
        self.run_query(q, False)
  
  def test_hashjoin(self):
    hashjoin = HashJoin(Scan('data', 'd1'), Scan('data', "d2"),
            map(cond_to_func, ["d1.a", "d2.c"]))
    proj = Project(hashjoin, [Star()])
    q = Yield(proj)

    # initialize operators' schema recursively
    self.opt.initialize_plan(q)
    print "Evaluate:\n%s" %(q.pretty_print())
    self.eval_query_plan(q, False)
  
  def parse(self, s):
    q = parse(s)
    q = Yield(q)
    q = self.opt(q)
    return q

  def compile(self, q):
    ctx = Context()
    q.produce(ctx)
    code = ctx.compiler.compile_to_func("compiled_q")
    code_w_comments = """
'''
%s
'''

%s
    """ % (q.pretty_print(), code)
    print code_w_comments
    exec(code)
    compiled_q.code = code
    return compiled_q
  
  def eval_query_str(self, s, ordered):
    q = self.parse(s)
    self.eval_query_plan(q, ordered)

  def eval_query_plan(self, q, ordered):
    # run iterator version
    res1 = []
    for row in q:
      res1.append(str(row))

    # run compiled version
    cq = self.compile(q)
    res2 = []
    for row in cq():
      res2.append(str(row))

    if not ordered:
      res1.sort()
      res2.sort()
    print "iterpreted: ", res1
    print "compiled: ", res2
    self.assertEqual(len(res1), len(res2))
    for row1, row2 in zip(res1, res2):
      self.assertEqual(row1, row2)
  
  def run_query(self, query, ordered):
    print "Test Query: ", query
    self.eval_query_str(query, ordered)

