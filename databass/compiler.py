from contextlib import contextmanager
from collections import *
from udfs import *

class Indent(object):
  pass

class Unindent(object):
  pass

class Compiler(object):
  """
  Defines helper functions to construct code blocks
  """
  def __init__(self):
    self.var_ids = defaultdict(lambda: 0)
    self.lines = []

  def new_var(self, prefix="var"):
    """
    Allocate a new variable name for compmiler to use

    @prefix optionally provide a custom variable name prefix
    """
    var = "%s_%d" % (prefix, self.var_ids[prefix])
    self.var_ids[prefix] += 1
    return var

  def compile_to_code(self):
    """
    Generate the raw code from the ir nodes referenced by self.root
    """
    return self.compile()

  def compile_to_func(self, fname="f"):
    """
    Wrap the compiled query code with a function definition.
    """
    lines = list(self.lines)
    comp = Compiler()
    with comp.indent("def %s():" % fname):
      comp.add_lines(self.lines)
    return comp.compile()

  def compile(self):
    ret = []
    ind = 0
    for line in self.lines:
      if isinstance(line, basestring):
        ret.append(("  " * ind) + line)
      elif isinstance(line, Indent):
        ind += 1
      elif isinstance(line, Unindent):
        ind -= 1
    return "\n".join(ret)

  def add_line(self, line):
    self.lines.append(line)

  def add_lines(self, lines):
    self.lines.extend(lines)

  @contextmanager
  def indent(self, cond):
    """
    Helper that lets caller enter and exit indented code block using a "with" expression:

        with ctx.compiler.indent() as compiler:
          compiler.add_line(...)

    """
    self.lines.append(cond)
    self.lines.append(Indent())
    try:
      yield self
    finally:
      self.lines.append(Unindent())


class Context(object):
  """
  The context object used throughout query compilation, and is the main 
  communication channel between operators as it goes through the produce->consume
  compilation phases.

  It serves three main roles:
  1. reference to Compiler object which is used to actually write compiled code
  2. manage i/o variables for compiling expressions.  The caller of an Expr instance's
     compile() method should specify the variable name of the input tuple that the 
     expression is evaluated over, and the output variable name that the expression result
     should be written to.
  3. pass variable requests and definitions between relational algebra operators in the
     query plan.  For instance, Source operators define range variables that iterate
     over input relations, and the names of those variables need to be passed to 
     later operators read these range variables' attribute values.

  """
  def __init__(self):
    # stack of operators populated during produce phase
    self.compiler = Compiler()

    # Input and output variable names for compiling exprs.  
    # Exprs are special because the reference to the input
    # row is passed from parent to child
    self.io_vars = []


    # Requested vars during qplan produce/consume phase.
    # Source operators define variables that range over the input relation
    # Non-source operators request variables that they will process.
    # 
    # This is needed because the produce phase happens top-down, thus a 
    # Project operator's produce is called before the range variables is allocated
    # by its child Scan operator.  Thus, during produce, operators add requests to
    # ops_var, and during consume, operators fulfill variable requests.
    #
    # Initialized with a dummy dict
    self.op_vars = [dict()]


  def add_line(self, line):
    self.compiler.add_line(line)

  def add_lines(self, lines):
    self.compiler.add_lines(lines)

  def new_var(self, *args, **kwargs):
    """
    Wrapper to create a new variable name
    """
    return self.compiler.new_var(*args, **kwargs)

  def add_io_vars(self, in_var, out_var):
    """
    Add an io variable request for expression compilation.
    @in_var name of variable in compiled program that contains expression's input row
    @out_var name of variable in compiled program that expression result should write to
    """
    self.io_vars.append((in_var, out_var))

  def pop_io_vars(self):
    """
    After an expression has used its input and output variables for compilation, the 
    pair should be popped from the stack.
    """
    return self.io_vars.pop()

  def request_vars(self, d):
    """
    @d a dictionary with keys that the operator requests, whose values are None and
       will be filled in by the child operators.  The main requested key is "row"
    """
    self.op_vars.append(d)

  def pop_vars(self):
    return self.op_vars.pop()

  def __getitem__(self, name):
    """
    Get the value of the most recently requested variable.
    @name name of variable to get
    """
    return self.op_vars[-1].get(name, None)

  def __setitem__(self, name, val):
    """
    Set the value of the most recently requested variable
    @name name of variable to set
    @val  value of variable
    """
    self.op_vars[-1][name] = val


if __name__ == "__main__":
  c = Compiler()
  c.add_line("a = 1")
  c.add_line("b = 2")
  with c.indent("for t in dataset:"):
    with c.indent("for s in dataset:"):
      c.add_line("t['a'] += 1")
    c.add_line("t['a'] -= 1")
  c.add_line("tup = {}")

  print c.compile()



