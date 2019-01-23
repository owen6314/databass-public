"""
HW0 Unit Test
"""
import unittest
from databass import *
from databass.tables import InMemoryTable


class TestUnits(unittest.TestCase):
  def compile_simple_query(self, q):
    ctx = Context()
    ctx.add_io_vars("irow", "outrow")
    q.compile(ctx)
    code = ctx.compiler.compile_to_code()
    exec(code)
    return outrow

  def test_interpreted_exprs(self):
    """
    Edit the code in `exprs.py` to support the unary operations "-", and "not",
    in addition to multiple  binary operations "/", "and", ">=", "<=", and "==".
    """
    row = ListTuple(Schema([]))
    exprs = [
      ("not(true)", False),
      ("-1", -1),
      ("(((4 * 3) * 10) <= 120) and true", True),
      ("((4 * 3) * 10) == 120", True),
      ("((4 * 3) * 10) >= 120", True),
      ("(1 / 2)", 0.5),
    ]

    for s, res in exprs:
      e = cond_to_func(s)
      val_interpreted = e(row)
      self.assertEqual(val_interpreted, res,
          "%s: %s != %s" % (s, val_interpreted, res))

    # invalid expressions that must raise an exeption
    invalid_exprs = [
      "'1' + 2",
      "2 + '2'",
      "'1' / '9'",
      "(ao"
    ]
    for s in invalid_exprs:
      with self.assertRaises(Exception):
        e = cond_to_func(s)
        e(row)

  def test_compiled_exprs(self):
    """
    Write the compiled version for expression evaluation by completing Expr.compile().
    compile() turn an expression into a python code that can be evaluated and write them to `Context`.
    """
    row = ListTuple(Schema([]))
    exprs = [
      ("'and the name'", 'and the name'),
      ("'1'", '1'),
      ("1 + 2", 3),
      ("1 and (2 > 3)", False),
      ("1 and (2 < 3)", True),
      ("true and (2 + 3)", 5),
      ("1 + (2 > 3)", 1),
      ("1 / 2.0", 0.5),
      ("((4 * 3) * 10) = 120", True),
      ("((4 * 3) * 10) <> 120", False),
      ("((4 * 3) * 10) > 120", False),
    ]


    # compare interpreted with compiled expressions
    for s, res in exprs:
      e = cond_to_func(s)
      val_interpreted = e(row)
      val_compiled = self.compile_simple_query(e)
      self.assertEqual(val_interpreted, res,
          "%s: %s != %s" % (s, val_interpreted, res))
      self.assertEqual(val_compiled, res,
          "%s: %s != %s" % (s, val_compiled, res))
