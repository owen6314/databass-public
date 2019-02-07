"""
HW2 Unit Test
Test correct implementation of query compilation for
(Order by, Projection without aggregation, Nested loops join)
"""
import unittest
from databass import *
from databass.tables import InMemoryTable


class TestUnits(unittest.TestCase):
  def setUp(self):
    self.db = Database.db()
    self.opt = Optimizer()

  def test_project(self):
    qs = ["SELECT a+c, b FROM data",
         "SELECT 1"]
    for q in qs:
        self.run_query(q, False)
  
  def test_orderby(self):
    qs = "SELECT a+b AS x, ((a*c)+2) AS y FROM (SELECT a, b, c FROM data ORDER BY c) ORDER BY y, x DESC"
    self.run_query(qs, True)
  
  def test_thetajoin(self):
    qs = """SELECT DISTINCT d1.a
      FROM data as d1,
           data as d2,
           (SELECT * FROM data) as d3
      WHERE d1.a = d2.b and d2.b = d3.a and d1.a = d3.c"""
    self.run_query(qs, False)
  
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

