"""

  The following are operators for simple Expressions 
  used within relational algebra operators

  e.g.,
     f() 
     1+2
     T.a + 2 / T.b

"""
from baseops import *
from util import guess_type


def unary(op, v):
  """
  interpretor for executing unary operator expressions
  """
  if op == "+":
    return v
  if op == "-":
    return -v
  if op.lower() == "not":
    return not(v)
  raise Exception("unary op not implemented")

def binary(op, l, r):
  """
  interpretor for executing binary operator expressions
  """
  if op == "+": return l + r
  if op == "*": return l * r
  if op == "-": return l - r
  if op == "=": return l == r
  if op == "<>": return l != r
  if op == "!=": return l != r
  if op == "or": return l or r
  if op == "<": return l < r
  if op == ">": return l > r
  if op == "/": return l / r
  if op == "and": return l and r
  if op == "==": return l == r
  if op == "<=": return l <= r
  if op == ">=": return l >= r
  raise Exception("binary op not implemented")

class ExprBase(Op):

  def get_type(self):
    raise Exception("ExprBase.get_type() not implemented")

  def compile(self, ctx):
    """
    @ctx contains the input and output variable that expression
         will read and write to:
          
            ctx.in_cur_var
            ctx.out_cur_var
    """
    raise Exception("ExprBase.compile() not implemented")

  def __str__(self):
    raise Exception("ExprBase.__str__() not implemented")

  def to_str(self, ctx):
    ctx.add_line(str(self))

class Expr(ExprBase):
  def __init__(self, op, l, r=None):
    self.op = op
    self.l = l
    self.r = r

  def get_type(self):
    boolean_ops = ["and", "or"]
    numeric_ops = ["+", "/", "*", "-", "<", ">", "<=", ">="]
    if self.op in boolean_ops:
      return "num"
    if self.op in numeric_ops:
      return "num"
    return "str"

  def operand_to_str(self, operand):
    """
    Helper for __str__ that adds parentheses around operands in a smarter way
    """
    s = str(operand)
    if s.startswith('(') and s.endswith(')'):
      return s
    if (isinstance(operand, Literal) or
        isinstance(operand, Attr) or 
        isinstance(operand, Star)):
      return s
    return "(%s)" % s

  def __str__(self):
    op = self.op
    if op == "=": op = "=="
    l = self.operand_to_str(self.l)
    if self.r:
      r = self.operand_to_str(self.r)
      return "%s %s %s" % (l, op, r)
    return "%s%s" % (self.op, l)

  def compile(self, ctx):
    """
    Compile expression into python code and write them to Context.
    For instance, "a + (b*2)" is compiled into a sequence of assignment expressions.  
    The following is pseudocode, with comments added for clarity

        # this block is code for compiling subexpression "a"
        v_l = v_in['a']
        # this block is code from compiling subexpression "(b*2)"
        v_rl = v_in['b']
        v_rr = 2
        v_r  = v_rl * v_rr
        # end of subexpression block
        v_out = v_l + v_r

    """
    v_in, v_out = ctx.pop_io_vars()
    v_l = ctx.new_var("expr")
    v_r = ctx.new_var("expr")
    op = self.op
    if op == "=":
      op = "=="

    # evaluate subexpressions
    ctx.add_io_vars(v_in, v_l)
    self.l.compile(ctx)
    if self.r:
      ctx.add_io_vars(v_in, v_r)
      self.r.compile(ctx)

    # write expression result to output variable
    if self.r:
      line = "%s = (%s) %s (%s)" % (v_out, v_l, op, v_r)
    else:
      line = "%s = %s(%s)" % (v_out, self.op, v_l)
    ctx.add_line(line)

  def __call__(self, row):
    l = self.l(row)
    if self.r is None:
      return unary(self.op, l)
    r = self.r(row)
    return binary(self.op, l, r)

class Paren(ExprBase):
  def __init__(self, c):
    self.c = c

  def get_type(self):
    return self.c.get_type()

  def __str__(self):
    return "(%s)" % self.c

  def compile(self, ctx):
    self.c.compile(ctx)

  def __call__(self, tup):
    return self.c(tup)


class Between(ExprBase):
  def __init__(self, expr, lower, upper):
    """
    expr BETWEEN lower AND upper
    """
    self.expr = expr
    self.lower = lower
    self.upper = upper

  def get_type(self):
    return "num"

  def __str__(self):
    return "(%s) BETWEEN (%s) AND (%s)" % (
        self.expr, self.lower, self.upper)

  def compile(self, ctx):
    v_in, v_out = ctx.pop_io_vars()

    v_e = ctx.new_var("tmp")
    v_l = ctx.new_var("tmp")
    v_u = ctx.new_var("tmp")

    ctx.add_io_vars(v_in, v_e)
    self.expr.compile(ctx)
    ctx.add_io_vars(v_in, v_l)
    self.lower.compile(ctx)
    ctx.add_io_vars(v_in, v_u)
    self.upper.compile(ctx)
    line = "%s = (%s) > (%s) && (%s) <= (%s)" % (
        v_out, v_e, v_l, v_e, v_u)
    ctx.add_line(line)

  def __call__(self, tup, tup2=None):
    e = self.expr(tup, tup2)
    l = self.lower(tup, tup2)
    u = self.upper(tup, tup2)
    return e >= l and e <= u

class AggFunc(ExprBase):
  """
  Expression Wrapper around an AggUDF instance
  """
  def __init__(self, f, args):
    self.name = f.name
    self.args = args
    self.f = f
    self.group_attr = Attr("__group__")

    # set the Attr references in the arguments to be array typed.
    for arg in self.args:
      for attr in arg.collect(Attr):
        attr.barraytyp = True

  def get_type(self):
    return "num"

  def __call__(self, row):
    idx = self.group_attr.idx
    args = []
    for grow in row[idx]:
      args.append([arg(grow) for arg in self.args])

    # make the arguments columnar:
    #   [ (a,a,a,a), (b,b,b,b) ]
    args = zip(*args)
    return self.f(*args)

  def compile(self, ctx):
    """
    Compiles and writes Python code that reads one input row, which should contains a 
    __group__ attribute, computes the arguments for the aggregation function, and calls it.

    @ctx Context object that contains appropriately set (v_in, v_out) variable names.
         v_in points to a single input row
         See Context.pop_io_vars() for details
    """
    v_in, v_out = ctx.pop_io_vars()
    v_g = ctx.new_var("tmp")
    v_irow = ctx.new_var("irow")
    v_args = ctx.new_var("args")

    # put row.__group__ into v_g
    ctx.add_io_vars(v_in, v_g)
    self.group_attr.compile(ctx)
    ctx.add_line("%s = [[] for i in range(%d)]" % (
      v_args, len(self.args)))

    # Iterate through the rows in the group to evaluate each argument
    line = "for %s in %s: " % (v_irow, v_g)
    with ctx.compiler.indent(line):
      for idx, arg in enumerate(self.args):
        v_arg = ctx.new_var("tmp")
        ctx.add_io_vars(v_irow, v_arg)
        arg.compile(ctx)
        ctx.add_line("%s[%d].append(%s)" % (v_args, idx, v_arg))

    # now, call the UDF with the prepared argument lists
    line = "%s = UDFRegistry.registry()['%s'](*%s)" % (v_out, self.name, v_args)
    ctx.add_line(line)

  def __str__(self):
    args = ",".join(map(str, self.args))
    return "%s(%s)" % (self.name, args)


class ScalarFunc(ExprBase): 
  def __init__(self, f, args):
    self.name = f.name
    self.args = args
    self.f = f

  def get_type(self):
    return "str"

  def __call__(self, ctx):
    args = [arg(row) for arg in self.args]
    return f(*args)

  def compile(self, ctx):
    """
    Writes Python code that evaluates each expresion in self.args over an input row and 
    passes their results to the UDF function.

    @ctx Context object that contains appropriately set (v_in, v_out) variable names.
         v_in points to a single input row
         See Context.pop_io_vars() for details
    """
    v_in, v_out = ctx.pop_io_vars()

    vlist = []
    for arg in self.args:
      v_arg = ctx.new_var("tmp")
      ctx.add_io_vars(v_in, v_arg)
      arg.compile(ctx)
      vlist.append(v_arg)
    vlist = ", ".join(vlist)

    line = "%s = UDFRegistry.registry()['%s'](%s)" % (v_out, self.name, ", ".join(vlist))
    ctx.add_line(line)

  def __str__(self):
    args = ",".join(map(str, self.args))
    return "%s(%s)" % (self.name, args)


class Literal(ExprBase):
  def __init__(self, v):
    self.v = v

  def __call__(self, row):
    return self.v

  def get_type(self):
    return guess_type(self.v)

  def __str__(self):
    if isinstance(self.v, basestring):
      return "'%s'" % self.v
    return str(self.v)

  def compile(self, ctx):
    v_in, v_out = ctx.pop_io_vars()
    line = "%s = %s" % (v_out, self)
    ctx.add_line(line)

class Bool(Literal):
  def __init__(self, v):
    super(Bool, self).__init__(v)

class Attr(ExprBase):
  """
  This class incorporates all uses and representatinos of attribute references in DataBass.
  At its core, it is represented by the tablename, attribute name, and attribute type.

  Attrs is used in two main capacities
  1. Schema definition: defines the table name, attribute name, and type ofr attrs in schema
     In this case, Attr should have all three field filled after init_schema() is called on 
     the operator (optimizer.initialize_plan()).
  2. Column Ref in an Expression: a reference to some tuple's attribute in the query that
     may need to be disambiguated.  In this case, Attr may only contain the attribute 
     name, and the tablename and type needs to be inferred during the optimizer's
     disambiguation process (optimizer.disambiguate_attrs())

  See constructor for additional fields for special cases.
  """

  NUM = "num"
  STR = "str"
  id = 0

  def __init__(self, aname, typ=None, tablename=None, 
      var=None, group_schema=None, idx=None, gidx=None):
    self.aname = aname
    self.typ = typ
    self.tablename = tablename

    # is Attr referenced in an aggregation function?
    self.barraytyp = False   

    # The GroupBy operator outputs a special schema with the
    # __group__ attribute that represents a table of rows in the
    # group's bucket.  
    # 
    # For this case, if this Attr instance represents __group__,
    # this field contains the schema of the group's relation.  
    #
    # It should be initialized in GroupBy.init_schema()
    self.group_schema = group_schema

    # If the child operator is GroupBy, and Attr is used as an expression,
    # then Attr may refer to an attribute within a group.  In that case we
    # need to index into the input row # to retrieve __group__, and then 
    # index into the group's rows:
    #
    #       # input_row.__group__.attr
    #       input_row[gidx][idx]
    #
    # * gidx is the index in child operator's schema that contains the __group__ attr.
    # * idx stores the index within the __group__ attr's schema
    #
    # It should be initialized in optimizer.disambiguate_attrs()
    self.gidx = None


    # Index for accessing attribute value in ListTuple instances.
    # Populated for Attr instances used as expressions.
    #
    # For joins, it is the idx of the left or right tuple that
    # contains the Attribute value.  The join operator will
    # ensure that the Attr gets the appropriate left or right tuple
    #
    # It should be initialized in optimizer.disambiguate_attrs()
    self.idx = idx

    self.id = "attr-%s" % Attr.id
    Attr.id += 1

  def get_type(self):
    return self.typ or '?'

  @property
  def is_agg(self):
    return self.barraytyp == True

  def copy(self):
    attr = Attr(self.aname)
    id = attr.id
    for key, val in self.__dict__.iteritems():
      attr.__dict__[key] = val
    attr.id = id
    return attr

  def matches(self, attr):
    """
    If self can satisfy the @attr argument, where @attr can be less specific 
    (e.g., tablename and typ are unbound)

    Typically, self will be a schema attribute, and @attr will be an attr reference
    from an expression.
    """
    if attr is None: 
      return False
    if attr.tablename and attr.tablename != self.tablename:
      return False
    if attr.typ and attr.typ != "?" and attr.typ != self.typ:
      return False
    return self.aname == attr.aname

  def __call__(self, row):
    return row[self.idx]

  def __hash__(self):
    return hash(self.id)

  def compile(self, ctx):
    """
    Read the input and output variable names from ctx, and write
    the python code to set the output variable to the input row's attribute val.

    @ctx Context object, where the top io variable pair (v_in, v_out) represents 
         the variable containing the input row, and output variable that stores
         the attribute value.
    """
    v_in, v_out = ctx.pop_io_vars()
    line = "%s = %s[%s]" % (v_out, v_in, self.idx)
    ctx.add_line(line)

  def compile_constructor(self):
    """
    return python string to reconstruct this Attr for query compilation.  
    For instance, the output may be the _string_:

        "Attr('a', 'data', 'num')"

    Used to generate code that initializes Schemas.
    """
    attrs_to_keep = ["typ", "tablename"]
    args = ["'%s'" % self.aname]
    for key in attrs_to_keep:
      val = self.__dict__[key]
      if isinstance(val, basestring):
        args.append("'%s'" % (val))
      else:
        args.append("%s" % (val))
    return "Attr(%s)" % ", ".join(args)

  def __str__(self):
    s = ".".join(filter(bool, [self.tablename, self.aname]))
    #return s
    return ":".join(filter(bool, [s, self.typ]))


class Star(ExprBase):
  def __init__(self, tablename=None):
    self.tablename = tablename

  def __call__(self, row):
    return row

  def __str__(self):
    if self.tablename:
      return "%s.*" % self.tablename
    return "*"

  def compile(self, ctx):
    raise Exception("I don't support turning SELECT * into python code")



