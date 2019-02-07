"""
Implementation of logical and physical relational operators
"""
from baseops import *
from exprs import *
from db import Database
from schema import *
from tuples import *
from util import cache
from itertools import chain


########################################################
#
# Source Operators
#
########################################################


class Source(UnaryOp):
  pass

class SubQuerySource(Source):
  """
  Allows subqueries in the FROM clause of a query
  Mainly responsible for giving the subquery an alias
  """
  def __init__(self, c, alias=None):
    super(SubQuerySource, self).__init__(c)
    self.alias = alias 

  def __iter__(self):
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def init_schema(self):
    """
    A source operator's schema should be initialized with the same 
    tablename as the operator's alias
    """
    # TODO: IMPLEMENT THIS
    self.schema = Schema([])
    return self.schema

  def produce(self, ctx):
    self.c.produce(ctx)

  def consume(self, ctx):
    self.consume_parent(ctx)

class Scan(Source):
  """
  A scan operator over a table in the Database singleton.
  """
  def __init__(self, tablename, alias=None):
    super(Scan, self).__init__()
    self.tablename = tablename
    self.alias = alias or tablename

  def init_schema(self):
    """
    A source operator's schema should be initialized with the same 
    tablename as the operator's alias
    """
    db = Database.db()
    self.schema = db.schema(self.tablename).copy()
    self.schema.set_tablename(self.alias)
    return self.schema

  def __iter__(self):
    # initialize a single intermediate tuple
    irow = ListTuple(self.schema, [])

    for row in Database.db()[self.tablename]:
      irow.row = row.row
      yield irow

  def produce(self, ctx):
    v_row = ctx.new_var("scan_row")

    cond = "for %s in Database.db()['%s']:" % (v_row, self.tablename)
    with ctx.compiler.indent(cond):
      # give variable name for the scan row to parent operator
      ctx["row"] = v_row
      self.consume_parent(ctx)

  def __str__(self):
    return "Scan(%s AS %s)" % (self.tablename, self.alias)

class TableFunctionSource(UnaryOp):
  """
  Scaffold for a table UDF function that outputs a relation.
  Not implemented.
  """
  def __init__(self, function, alias=None):
    super(TableFunctionSource, self).__init__(function)
    self.function = function
    self.alias = alias 

  def __iter__(self):
    raise Exception("TableFunctionSource: Not implemented")

  def __str__(self):
    return "TableFunctionSource(%s)" % self.alias


########################################################
#
# Join Operators
#
########################################################


class From(NaryOp):
  """
  Logical FROM operator. 
  Optimizer will expand it into a join tree
  """
  def to_str(self, ctx):
    with ctx.compiler.indent("From"):
      for c in self.cs:
        c.to_str(ctx)

class Join(BinaryOp):
  pass

class ThetaJoin(Join):
  """
  Theta Join is tuple-nested loops join
  """
  def __init__(self, l, r, cond=Literal(True)):
    """
    @l    left (outer) subplan of the join
    @r    right (inner) subplan of the join
    @cond an Expr object whose output will be interpreted
          as a boolean
    """
    super(ThetaJoin, self).__init__(l, r)
    self.cond = cond
    self.state = 0

    # Variables allocated during compilation that are shared
    # between the produce and consume phases.
    self.v_irow = None  # intermediate row emitted to parent op
    self.v_lrow = None  # variable to contain left row
    self.v_rrow = None  # variable to contain right row

  def __iter__(self):
    # initialize a single intermediate tuple
    irow = ListTuple(self.schema, [])

    for lrow in self.l:
      for rrow in self.r:
        # populate intermediate tuple with values
        irow.row[:len(lrow.row)] = lrow.row
        irow.row[len(lrow.row):] = rrow.row

        if self.cond(irow):
          yield irow

  def produce(self, ctx):
    """
    Produce's job is to 
    1. request the var name for the left input row
    2. allocate a variable and Tuple for the intermediate row
    3. call produce on left child
    """
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def consume(self, ctx):
    """
    Consume will be called twice, 
    * first by the left child's consume phase, 
    * second by the right child's consume phase.

    It will help to keep state to know which time this 
    function is being called, so that you run the correct logic
    """
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def consume_left(self, ctx):
    """
    Consume for the left subplan.  
    Retreive the variable allocated by the left subplan and
    setup context for right subplan
    """
    # TODO: IMPLEMENT THIS
    pass

  def consume_right(self, ctx):
    """
    This writes the inner loop logic for the nested loops join.
    To do so, retreive variable allocated by right subplan, and write the 
    compiled python code to create the intermediate row, call the 
    join condition expression, and pass control to parent's consume.

    Make sure to pass the variable name of output row for the parent operator.
    """
    # TODO: IMPLEMENT THIS
    pass

  def __str__(self):
    return "THETAJOIN(ON %s)" % (str(self.cond))

    
class HashJoin(Join):
  """
  Hash Join
  """
  def __init__(self, l, r, join_attrs):
    """
    @l    left table of the join
    @r    right table of the join
    @join_attrs two attributes to join on, hash join checks if the 
                attribute values from the left and right tables are
                the same.  Suppose:
                
                  l = iowa, r = iowa, join_attrs = ["STORE", "storee"]

                then we return all pairs of (l, r) where 
                l.STORE = r.storee
    """
    super(HashJoin, self).__init__(l, r)
    self.join_attrs = join_attrs

    self.state = 0

    # allocate variables for all state shared between produce/consume
    self.v_ht = None   # hashtable
    self.v_irow = None # intermediate row
    self.v_lrow = None # left row allocated by left subplan
    self.v_rrow = None # right row allocated by right subplan
    self.v_lkey = None # left row's join key
    self.v_rkey = None # right row's join key

  def __iter__(self):
    """
    Build an index on the inner (right) source, then probe the index
    for each row in the outer (left) source.  
    
    Yields each join result
    """
    # initialize intermediate row to populate and pass to parent operators
    irow = ListTuple(self.schema)

    # TODO: IMPLEMENT THIS
    # XXX: Uncomment the following line of code after you complete the function.
    # index = self.build_hash_index(self.r, ridx)
    raise Exception("Not implemented")

  def build_hash_index(self, child_iter, idx):
    """
    @child_iter tuple iterator to construct an index over
    @attr attribute name to build index on

    Loops through a tuple iterator and creates an index based on
    the attr value
    """
    # defaultdict will initialize a hash entry to a new list if
    # the entry is not found
    index = defaultdict(list)
    # TODO: IMPLEMENT THIS
    return index

  def produce(self, ctx):
    """
    Produce's job is to 
    1. allocate variable names and create hash table
    2. create hash table
    2. call left's produce to populate hash table
    3. call right's produce to probe hash table 
    """
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def consume(self, ctx):
    """
    Consume will be called twice, once by the left child's
    consume phase, and once by the right childe's consume phase.

    Need to internally keep track of which time it is being called
    """
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def consume_left(self, ctx):
    """
    Given variable name for left row, compute left key to populate hash table
    """
    # TODO: IMPLEMENT THIS
    pass

  def consume_right(self, ctx):
    """
    Given variable name for right row, 
    1. compute right key, 
    2. probe hash table, 
    3. create intermediate row to pass to parent's consume
    """
    # TODO: IMPLEMENT THIS
    pass


########################################################
#
# Aggregation Operators
#
########################################################



class GroupBy(UnaryOp):
  def __init__(self, c, group_exprs):
    """
    @c           child operator
    @group_exprs list of Expression objects
    """
    super(GroupBy, self).__init__(c)
    self.group_exprs = group_exprs

    # Attrs referenced in group_exprs. 
    # They will be copied into the output row so Project can compute arbitrary 
    # expressions over any of the referenced Attrs.  e.g.,
    #   
    #    SELECT a+b, sum(c)
    #    ..
    #    GROUP BY a * b
    #
    # self.group_attrs contains the Attr expressions for a, b
    # 
    # During execution, the values of a, b will be held in compiler 
    # var self.v_attrvals below
    #
    self.group_attrs = []

    # Compiler variables
    self.v_ht = None       # hash table
    self.v_bucket = None   # value (bucket) in v_ht
    self.v_grp = None      # holds a group's rows
    self.v_key = None      # a group's key
    self.v_attrvals = None # holds values of Attrs referenced in # grouping expression.
                           # See self.group_attrs
    self.v_irow = None     # intermediate row
    self.v_in = None       # input row from child subplan


  def init_schema(self):
    """
    Find and copy attributes referenced in the group expressions to the output schema. 

    The output schema should contain (in the following order):

    * the attributes (Attr expressions) referenced in the grouping expressions
    * a special __key__ attribute that contains a string representation of the
      group's key
    * a special __group__ attribute that contains a list of tuples in the group.
      Note that this Attr's group_schema should be set to the input relation's
      schema

    For instance, if the query is:

        SELECT a-b, count(c)
        FROM data
        GROUP BY a+b

    The output schema should be:

        (a, b, __key__, __group__)

    The values of a, b can be set to those of the LAST tuple added to the group.
    """
    # TODO: IMPLEMENT THIS
    self.schema = Schema([])
    return self.schema

  def __iter__(self):
    """
    GroupBy works as follows:
    
    * Contruct and populate hash table 
      * key is defined by the group_exprs expressions  
      * Track the values of the attributes referenced in the grouping expressions
      * Track the tuples in each bucket
    * Iterate through each bucket, compose and populate a tuple that conforms to 
      this operator's output schema (see self.init_schema)
    """

    hashtable = defaultdict(lambda: [None, None, []])

    # This initializes the intermediate row that you will populate and pass 
    # to parent operators
    irow = ListTuple(self.schema, [])

    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def produce(self, ctx):
    """
    Produce sets up the variables and hash table so that they can be populated by
    calling child's produce (which eventually calls self.consume). 
    
    Once the hash table is populated, it should then loop through the hash table
    and emit output records that adhere to the output schema
    """
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def consume(self, ctx):
    """
    Emits code that takes as input the child operator's row and adds it to the hash table.
    Note that this should NOT call the parent's consume, since this doesn't actually
    compute output records.
    """
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def __str__(self):
    s = "GROUPBY(%s)" % ", ".join(map(str, self.group_exprs))
    return s

  def to_str(self, ctx):
    with ctx.compiler.indent(str(self)):
      self.c.to_str(ctx)


########################################################
#
# Projection Operator
#
########################################################

class Project(UnaryOp):
  def __init__(self, c, exprs, aliases=[]):
    """
    @p            parent operator
    @exprs        list of Expr objects
    @aliases      name of the fields defined by the above exprs
    """
    super(Project, self).__init__(c)
    self.exprs = exprs
    self.aliases = list(aliases) or []

  def set_default_aliases(self):
    """
    Provide generic attr1 style names for unaliased expressions.
    """
    for i, expr in enumerate(self.exprs):
      if i >= len(self.aliases):
        self.aliases.append(None)
      alias = self.aliases[i]
      if not alias:
        if isinstance(expr, Star): 
          continue
        if isinstance(expr, Attr):
          self.aliases[i] = expr.aname
        else:
          self.aliases[i] = "attr%s" % i

  def expand_stars(self):
    """
    Updates self.exprs and self.aliases to replace instances of * with all attributes
    from child schema.  Alias should simply be the Attr's name.

    For instance, assume data(a,b,c).  Then the following query:

            SELECT a, b, * FROM data

    Should be expanded into

            SELECT a, b, a, b, c FROM data

    And thus self.exprs should contain
        
            [Attr(a), Attr(b), Attr(a), Attr(b), Attr(c)]

    And self.aliases should contain

            ["a", "b", "a", "b", "c"]

    Note that if a parent operator references "a", the reference will be caught
    as ambiguous by Optimizer.disambiguate_attrs().

    """
    if not self.c:
      if self.collect(Star):
        raise Exception("Cannot use * when Project has no source relations.")
      return

    newexprs = []
    newaliases = []

    # TODO: IMPLEMENT THIS
    self.exprs = newexprs
    self.aliases = newaliases

  def init_schema(self):
    # TODO: IMPLEMENT THIS
    self.set_default_aliases()
    # XXX: Uncomment the following line of code after you complete expand_stars()
    # self.expand_stars()
    self.schema = Schema([])
    return self.schema

  def __iter__(self):
    child_iter = self.c
    # initialize single intermediate tuple to populate and pass to parent
    irow = ListTuple(self.schema, [])

    # TODO: IMPLEMENT THIS
    # if the query doesn't have a FROM clause (SELECT 1), pass up an empty tuple
    raise Exception("Not implemented")

  def produce(self, ctx):
    """
    Setup output tuple variable and generate code to initialize it 
    as an empty tuple with the correct schema..  

    There is a special case when if there is no child operator, such as
    
            SELECT 1
            
    where produce should pretend it is an access method that emits a 
    single empty tuple to its own consume method.
    """
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def consume(self, ctx):
    # TODO: IMPLEMENT THIS
    # In the consume method, you can use the `self.compile_exprs()`
    # method defined in `baseops.py` to execute an array of expressions
    # and retrive the temporary values their results are stored in.
    raise Exception("Not implemented")

  def __str__(self):
    args = ", ".join(["%s AS %s" % (e, a) 
      for (e, a) in  zip(self.exprs, self.aliases)])
    return "Project(%s)" % args




########################################################
#
# Other Operators
#
########################################################


class OrderBy(UnaryOp):
  """
  XXX:  There is a slight bug with this implementation, which is that
        ORDERBY is placed after project, and does not have visibility
        to the underlying row attributes that are not explicitly projected
        by the Project operator.  Thus the following will result in an error:

          SELECT a+b FROM data ORDER BY a
  """

  def __init__(self, c, order_exprs, ascdescs="asc"):
    """
    @c            child operator
    @order_exprs  ordered list of Expression objects
    """
    super(OrderBy, self).__init__(c)
    self.order_exprs = order_exprs
    self.ascdescs = ascdescs
    self.normalize_ascdescs()

  def normalize_ascdescs(self):
    """
    make sure there is "asc" or "desc" for each element in order_exprs
    """
    if not isinstance(self.ascdescs, list):
      raise Exception("OrderBy: ascdescs should be a list type")

    for i in range(len(self.order_exprs)):
      if len(self.ascdescs) < i:
        self.ascdescs.append("asc")
      if self.ascdescs[i] is None:
        self.ascdescs[i] = "asc"

      if self.ascdescs[i] == "desc":
        raise Exception("OrderBy: does not support DESC")

  def __iter__(self):
    """
    OrderBy is a blocking operator that needs to accumulate all of its child
    operator's outputs before sorting by the order expressions.

    Note: each row from the child operator may be the _same_ ListTuple
    """
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def produce(self, ctx):
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def consume(self, ctx):
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def __str__(self):
    args = ", ".join(["%s %s" % (e, ad) 
      for (e, ad) in  zip(self.order_exprs, self.ascdescs)])
    return "ORDERBY(%s)" % args

class Filter(UnaryOp):
  def __init__(self, c, cond):
    """
    @c            child operator
    @cond         boolean Expression 
    """
    super(Filter, self).__init__(c)
    self.cond = cond

  def __iter__(self):
    for row in self.c:
      if self.cond(row):
        yield row

  def produce(self, ctx):
    self.c.produce(ctx)

  def consume(self, ctx):
    v_in = ctx['row']
    v_cond = ctx.new_var("fil_cond")

    ctx.add_line("# if %s" % str(self.cond))
    ctx.add_io_vars(v_in, v_cond)
    self.cond.compile(ctx)

    cond = "if (%s):" % v_cond
    with ctx.compiler.indent(cond):
      self.consume_parent(ctx)

class Limit(UnaryOp):
  def __init__(self, c, limit, offset=0):
    """
    @c            child operator
    @limit        number of tuples to return
    """
    super(Limit, self).__init__(c)
    self.limit = limit
    if isinstance(self.limit, numbers.Number):
      self.limit = Literal(self.limit)

    self._limit =  int(self.limit(None))
    if self._limit < 0:
      raise Exception("LIMIT must not be negative: %d" % l)

    self.offset = offset or 0
    if isinstance(self.offset, numbers.Number):
      self.offset = Literal(self.offset)

    self._offset = int(self.offset(None))
    if self._offset < 0:
      raise Exception("OFFSET must not be negative: %d" % o)


  def __iter__(self):
    """
    LIMIT should skip <offset> number of rows, and yield at most <limit>
    number of rows
    """
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def produce(self, ctx):
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def consume(self, ctx):
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def __str__(self):
    return "LIMIT(%s OFFSET %s)" % (self.limit, self.offset)

class Distinct(UnaryOp):
  def __iter__(self):
    """
    It is OK to use hash(row) to check for equivalence between rows
    """
    # TODO: IMPLEMENT THIS
    raise Exception("Not implemented")

  def produce(self, ctx):
    self.v_seen = ctx.new_var("distinct_seen")
    ctx.add_line("%s = set()" % self.v_seen)

    ctx.request_vars(dict(row=None))
    self.c.produce(ctx)

  def consume(self, ctx):
    v_in = ctx['row']
    ctx.pop_vars()

    v_key = ctx.new_var("distinct_key")
    lines = [
      "%s = hash(%s)" % (v_key, v_in),
      "if %s in %s: continue" % (v_key, self.v_seen),
      "%s.add(%s)" % (self.v_seen, v_key)]
    ctx.add_lines(lines)

    ctx['row'] = v_in
    self.consume_parent(ctx)


class Yield(UnaryOp):
  def init_schema(self):
    self.schema = self.c.schema
    return self.schema

  def __iter__(self):
    return iter(self.c)

  def produce(self, ctx):
    self.c.produce(ctx)

  def consume(self, ctx):
    v_in = ctx['row']
    ctx.add_line("yield %s" % v_in)
    self.consume_parent(ctx)

class Print(UnaryOp):
  def init_schema(self):
    self.schema = self.c.schema
    return self.schema

  def __iter__(self):
    for row in self.c:
      print row
    yield 

  def produce(self, ctx):
    self.c.produce(ctx)

  def consume(self, ctx):
    v_in = ctx['row']
    ctx.add_line("print %s" % v_in)
    self.consume_parent(ctx)




