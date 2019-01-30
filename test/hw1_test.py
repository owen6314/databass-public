"""
HW1 Unit Test
"""
import unittest
from databass import *
from databass.tables import InMemoryTable
import random


class TestUnits(unittest.TestCase):
  def setUp(self):
    self.db = Database.db()
    self.opt = Optimizer()
    self.init_test_table()

  def init_test_table(self):
    tablename = "tdata"
    attr_name = "a"
    typ = "num"
    schema = Schema([Attr(attr_name, typ, tablename)])
    data = [[random.randint(0, 100)] for i in xrange(1000)]
    table = InMemoryTable(schema, data)
    self.db.register_table(tablename, schema, table)
    self.ttable = dict(tablename=tablename, schema=schema, data=data)

  def eval_query_plan(self, q):
    """
    run iterator version __iter__
    """
    res = []
    for row in q:
      res.append(str(row))
    return res

  def limit_data(self, data, offset):
    temp = data[:offset]
    return ["(%s)"%x[0] for x in temp]

  def distinct_data(self, data):
    seen = set()
    return ["(%s)"%x[0] for x in data if x[0] not in seen and not seen.add(x[0])]

  def orderby_data(self, data, idx, asc="asc"):
    f = lambda x: x[idx]
    temp = sorted(data, key=f, reverse=asc!="asc")
    return ["(%s)"%x[0] for x in temp]

  def test_limit(self):
    """
    Limit
    """
    tablename = self.ttable["tablename"]
    data = self.ttable["data"]
    schema = self.ttable["schema"]

    base_op = Scan(tablename)
    base_op.init_schema()

    # Limit iter()
    offset = 2
    op = Limit(base_op, cond_to_func(str(offset)))
    print "Evaluate:\n%s" %(op.pretty_print())
    ret1 = self.eval_query_plan(op)
    ret2 = self.limit_data(data, offset)
    ret1.sort()
    ret2.sort()
    self.assertEqual(ret1, ret2)

  def test_distinct(self):
    """
    Distinct
    """
    tablename = self.ttable["tablename"]
    data = self.ttable["data"]
    schema = self.ttable["schema"]

    base_op = Scan(tablename)
    base_op.init_schema()

    # Distinct iter()
    op = Distinct(base_op)
    print "Evaluate:\n%s" %(op.pretty_print())
    ret1 = self.eval_query_plan(op)
    ret2 = self.distinct_data(data)
    self.assertEqual(ret1, ret2)

  def test_orderby(self):
    """
    OrderBy
    """
    tablename = self.ttable["tablename"]
    data = self.ttable["data"]
    schema = self.ttable["schema"]

    base_op = Scan(tablename)
    base_op.init_schema()

    # OrderBy iter()
    exp = Attr("a", "num", tablename)
    exp.idx = schema.idx(exp)
    op = OrderBy(base_op, [exp], ["asc"])
    print "Evaluate:\n%s" %(op.pretty_print())
    ret1 = self.eval_query_plan(op)
    ret2 = self.orderby_data(data, exp.idx, "asc")
    self.assertEqual(ret1, ret2)

  def test_phase1(self):
    """
    Complete iter():
      a) Limit
      b) Distinct
      c) OrderBy
    """
    self.test_limit()
    self.test_distinct()
    self.test_orderby()

  def eval_query_str(self, s):
    q = parse(s)
    q = Yield(q)
    # initialize operators' schema recursively
    q = self.opt(q)
    return (q, self.eval_query_plan(q))

  def check_schema(self, schema1, schema2):
    for i, attr in enumerate(schema1.attrs):
      self.assertEqual(attr.aname, schema2.attrs[i].aname)
      self.assertEqual(attr.typ, schema2.attrs[i].typ)

  def test_project_init_schema(self):
    """
    """
    scan = Scan('data', 'd1')
    proj = Project(scan, map(cond_to_func, ["a", "b", "c"]))
    proj = Project(
            proj,
            map(cond_to_func, ["a + b", "a"]),
            ['x'])
    # initialize operators' schema recursively
    self.opt.initialize_plan(proj)
    self.opt.disambiguate_attrs(proj)
    print "Evaluate:\n%s" %(proj.pretty_print())
    
    schema = Schema([Attr("x", "num"), Attr("a", "?")])
    self.check_schema(schema, proj.schema)

    aliases_res = ['x', 'a']
    self.assertEqual(proj.aliases, aliases_res)
  
  def test_project_expand_stars(self):
    scan = Scan('data', 'd1')
    proj = Project(scan, [Star()])
    self.opt.initialize_plan(proj)
    self.opt.disambiguate_attrs(proj)
    print "Evaluate:\n%s" %(proj.pretty_print())
    schema = Schema([
      Attr("a", "num"),
      Attr("b", "num"),
      Attr("c", "num"),
      Attr("d", "num"),
      Attr("e", "str"),
      Attr("f", "num"),
      Attr("g", "str"),
      ])
    self.check_schema(schema, proj.schema)

    aliases_res = ['a', 'b', 'c', 'd', 'e', 'f', 'g']
    self.assertEqual(proj.aliases, aliases_res)

  def test_project_iter(self):
    q_res = [
            ['(1.0)'],
            ['(0, 0.0)', '(5, 0.0)', '(10, 0.0)', '(15, 0.0)', '(2, 9.0)', '(7, 9.0)', '(12, 9.0)', '(17, 9.0)', '(4, 18.0)', '(9, 18.0)', '(14, 18.0)', '(19, 18.0)', '(6, 27.0)', '(11, 27.0)', '(16, 27.0)', '(21, 27.0)', '(8, 36.0)', '(13, 36.0)', '(18, 36.0)', '(23, 36.0)'],
            ['(0)', '(1)'],
            ['(0, 0, 0, 0, a, 2, c)', '(1, 1, 1, 0, b, 4, d)']
    ]

    queries = [
      ("SELECT 1", q_res[0], False),
      ("SELECT DISTINCT a+b as a1, 9 * b as b1 FROM data ORDER BY b1", q_res[1], True),
      ("SELECT b FROM data LIMIT 2", q_res[2], False),
      ("SELECT * FROM data ORDER BY f LIMIT 2", q_res[3], True),
    ]

    for q, q_res, ordered in queries:
      (qplan, ret) = self.eval_query_str(q)
      print "Evaluate: %s\n %s" %(q, qplan.pretty_print())
      self.check_results(q_res, ret, ordered)

  def check_results(self, ret1, ret2, ordered=False):
    if ordered == False:
      ret1.sort()
      ret2.sort()
    self.assertEqual(len(ret1), len(ret2))
    for row1, row2 in zip(ret1, ret2):
      self.assertEqual(row1, row2)

  def test_phase2(self):
    """
    Complete:
      a) Project.init_schema()
      b) Project.__iter__()
    """

    self.test_project_init_schema()
    self.test_project_expand_stars()
    self.test_project_iter()

  def test_subquery(self):
    q_res = ['(0, 0)', '(2, 1)', '(4, 2)', '(6, 3)', '(8, 4)', '(5, 5)', '(7, 6)', '(9, 7)', '(11, 8)', '(13, 9)', '(10, 10)', '(12, 11)', '(14, 12)', '(16, 13)', '(18, 14)', '(15, 15)', '(17, 16)', '(19, 17)', '(21, 18)', '(23, 19)']
    proj = Project(Scan('data', 'd1'), map(cond_to_func, ["a + b", "a"]))
    q = SubQuerySource(proj, 'd2')
    # initialize operators' schema recursively
    self.opt.initialize_plan(q)
    self.opt.disambiguate_attrs(q)
    print "Evaluate:\n%s" %(q.pretty_print())
    res = self.eval_query_plan(q)

    self.check_schema(q.schema, proj.schema)
    self.check_results(q_res, res)

  def test_hashjoin(self):
    q_res = ['(0, 0, 0, 0, a, 2, c, 19, 4, 1, 0, b, 40, cde)', '(0, 0, 0, 0, a, 2, c, 19, 4, 1, 0, b, 40, cde)', '(0, 0, 0, 0, a, 2, c, 19, 4, 1, 0, b, 40, cde)', '(0, 0, 0, 0, a, 2, c, 19, 4, 1, 0, b, 40, cde)', '(0, 0, 0, 0, a, 2, c, 19, 4, 1, 0, b, 40, cde)', '(0, 0, 0, 0, a, 2, c, 19, 4, 1, 0, b, 40, cde)', '(0, 0, 0, 0, a, 2, c, 19, 4, 1, 0, b, 40, cde)', '(0, 0, 0, 0, a, 2, c, 19, 4, 1, 0, b, 40, cde)', '(0, 0, 0, 0, a, 2, c, 19, 4, 1, 0, b, 40, cde)', '(0, 0, 0, 0, a, 2, c, 19, 4, 1, 0, b, 40, cde)', '(1, 1, 1, 0, b, 4, d, 19, 4, 1, 0, b, 40, cde)', '(1, 1, 1, 0, b, 4, d, 19, 4, 1, 0, b, 40, cde)', '(1, 1, 1, 0, b, 4, d, 19, 4, 1, 0, b, 40, cde)', '(1, 1, 1, 0, b, 4, d, 19, 4, 1, 0, b, 40, cde)', '(1, 1, 1, 0, b, 4, d, 19, 4, 1, 0, b, 40, cde)', '(1, 1, 1, 0, b, 4, d, 19, 4, 1, 0, b, 40, cde)', '(1, 1, 1, 0, b, 4, d, 19, 4, 1, 0, b, 40, cde)', '(1, 1, 1, 0, b, 4, d, 19, 4, 1, 0, b, 40, cde)', '(1, 1, 1, 0, b, 4, d, 19, 4, 1, 0, b, 40, cde)', '(1, 1, 1, 0, b, 4, d, 19, 4, 1, 0, b, 40, cde)']
    q = HashJoin(Scan('data', 'd1'), Scan('data', "d2"),
            map(cond_to_func, ["d1.a", "d2.c"]))

    # initialize operators' schema recursively
    self.opt.initialize_plan(q)
    self.opt.disambiguate_attrs(q)
    print "Evaluate:\n%s" %(q.pretty_print())
    res = self.eval_query_plan(q)
    self.check_results(q_res, res)

  def test_groupby(self):
    gby = GroupBy(Scan("data", "d"), map(cond_to_func, ["c"]))
    q = Project(gby, map(cond_to_func, ["c+2", "sum(f)", "count(a)"]))
    self.opt.initialize_plan(q)
    self.opt.disambiguate_attrs(q)

    q_res = ['(2.0, 200, 10)', '(3.0, 220, 10)']

    print "Evaluate:\n%s" %(q.pretty_print())
    # check for init schema
    schema = Schema([
      Attr("c", "num"),
      Attr("__key__", "str"),
      Attr("__group__")
      ])
    for i, attr in enumerate(schema.attrs):
      self.assertEqual(attr.aname, gby.schema.attrs[i].aname)

    res = self.eval_query_plan(q)
    self.check_results(q_res, res)

  def testall(self):
    q_res = ['(0)', '(5)', '(10)', '(15)']
    q = """SELECT d2.x
      FROM (SELECT a AS x, sum(b) AS z
            FROM data GROUP BY a) AS d2,
           (SELECT d AS y, sum(b) AS z
            FROM data GROUP BY d+1) AS d3
      WHERE d2.z = d3.y ORDER BY x"""
    self.eval_query_str(q)
    (qplan, ret) = self.eval_query_str(q)
    print "Evaluate:\n%s" %(qplan.pretty_print())
    self.check_results(q_res, ret, True)

  def test_phase3(self):
    """
    Complete:
      a) SubQuerySource.iter() & init__schema()
      c) HashJoin
      d) GroupBy.iter() && init_schema
    """
    self.test_subquery()
    self.test_hashjoin()
    self.test_groupby()
    self.testall()
