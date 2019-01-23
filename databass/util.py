import numbers
from functools import partial

def pickone(l, attr):
  return [(i and getattr(i, attr) or None) for i in l]

def flatten(list_of_lists):
  ret = []
  map(ret.extend, list_of_lists)
  return ret

def cond_to_func(expr_or_func):
  """
  Helper function to help automatically interpret string expressions 
  when you manually construct a query plan.
  """
  from parse_expr import parse

  # if it's a function already, then we're good
  if hasattr(expr_or_func, "__call__"):
    return expr_or_func
  # otherwise, parse it as a string
  if isinstance(expr_or_func, str):
    return parse(expr_or_func)
  raise Exception("Can't interpret as expression: %s" % expr_or_func)

def cache(func):
  class Cache(object):
    def __init__(self):
      self.cache = None
  _cache = Cache()
  def wrapper(*args, **kwargs):
    if _cache.cache is None:
      _cache.cache = func(*args, **kwargs)
    return _cache.cache
  return wrapper


def guess_type(v):
  if v is not None and isinstance(v, numbers.Number):
    return "num"
  return "str"

def print_qplan_pointers(q):
  queue = [q]
  while queue:
    op = queue.pop(0)
    print "%d: %s" % (op.id, op)
    for cop in op.children():
      print "\t%d\t->\t%d" % (op.id, cop.id)
    queue.extend(op.children())


