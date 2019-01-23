from ops import *
from db import Database
from util import *
from itertools import *
from collections import *


class Optimizer(object):
  """
  The optimizer takes as input a query plan, and provides three functionalities:

  1. Initialize the plan by called init_schema() on the operators bottom up
  2. Once the plan is initialized, disambiguate all attribute references in
     expressions so that their tablenames, and idx/gidx fields are populated
  3. Optimize the query plan by replacing the N-ary From operator with a join 
     plan.  This involves performing Selinger-style join ordering 
     Note that this step may create new operators, so the new physical plan needs
     to be initialized and disambiguated again.
  """

  def __init__(self):
    self.db = Database.db()

  def __call__(self, op):
    if not op: return None

    self.initialize_plan(op)
    self.disambiguate_attrs(op)

    # If there's a From operator in the tree, 
    # then replace with join tree
    while op.collectone("From"):
      op = self.expand_from_op(op)

    self.initialize_plan(op)
    self.disambiguate_attrs(op)
    return op

  def initialize_plan(self, op):
    """
    Traverse bottom up from Scan operators and initialize operator schemas

    This method should be idempotent: running it again should result in the same
    schemas
    """
    # clear all schemas
    for cop in op.collect(Op):
      if cop.is_type(ExprBase): continue
      cop.schema = None

    leaves = []
    def f(cur, path):
      if cur.is_type(ExprBase): return False
      if not cur.children():
        leaves.append(cur)
    op.traverse(f)

    queue = leaves
    niters = 0
    while queue:
      niters += 1
      if niters > 10000:
        raise Exception("Cycle in plan.  Cannot initialize")

      op = queue.pop(0)
      # wait until op's children are all initialized
      if not all(c.schema for c in op.children()):
        queue.append(op)
        continue

      op.init_schema()
      if op.p:
        queue.append(op.p)

  def attrs_from_nonsource_op(self, op):
    """
    Find all instances of Attrs used in op's expressions
    """
    attrs = []
    if op.is_type(ThetaJoin):
      attrs = op.cond.collect(Attr)
    elif op.is_type(HashJoin):
      attrs = op.join_attrs
    elif op.is_type(GroupBy):
      attrs = []
      for expr in chain(op.group_exprs, op.group_attrs):
        attrs.extend(expr.collect(Attr))
    elif op.is_type(OrderBy):
      attrs = []
      for expr in op.order_exprs:
        attrs.extend(expr.collect(Attr))
    elif op.is_type(Filter):
      attrs = op.cond.collect(Attr)
    elif op.is_type(Project):
      attrs = []
      for expr in op.exprs:
        attrs.extend(expr.collect(Attr))

    return attrs

  def disambiguate_op_attrs(self, op):
    """
    Disambiguate a single operator's Attr references using its child operators' schemas.  
    On return, each Attr reference should know its tablename, its type, and how to index
    into an input tuple to retrieve the referenced Attr value.

    To do so, need to take into account child GroupBy operators, which may contain 
    __group__ attribute that is a list of rows as its value rather than a single scalar value.
    """
    attrs = self.attrs_from_nonsource_op(op)
    if not attrs: 
      return 

    # For each attr reference in attrs, find all matching schema attributes
    # from the child operators.
    matches = defaultdict(list)
    for cop in op.children():
      for attr in attrs:
        # if attr referenced in an aggregate UDF, look for __group__ in child's schema.
        # See AggFunc.__init__
        if attr.barraytyp: 
          gidx = cop.schema.idx(Attr("__group__"))
          gschema = cop.schema.attrs[gidx].group_schema
          for idx, gattr in enumerate(gschema):
            if gattr.matches(attr):
              matches[attr].append(dict(
                is_agg=True, 
                attr=gattr, 
                op=cop,
                gidx=gidx,
                idx=idx))
        else:
          for idx, cattr in enumerate(cop.schema):
            if cattr.matches(attr):
              matches[attr].append(dict(
                is_agg=False,
                attr=cattr,
                op=cop,
                idx=idx))

    # Make sure that each attribute reference matches at most 1 unique schema attribute
    # and set the fields in the reference appropriately
    for attr, mattrs in matches.iteritems():
      if len(mattrs) == 0: continue
      if len(mattrs) > 1:
        err = ", ".join([str(mattr['attr']) for mattr in mattrs])
        raise Exception("Attr %s in %s is ambiguous, matched: %s" % (attr, op, err))

      is_agg = mattrs[0]['is_agg']
      mattr = mattrs[0]['attr']
      mop = mattrs[0]['op']
      if attr.tablename and attr.tablename != mattr.tablename:
        msg = "Attr %s in %s already bound to %s != %s" % (
          attr, op, attr.tablename, mattr.tablename)
        raise Exception(msg)

      attr.tablename = mattr.tablename
      attr.typ = mattr.typ
      attr.idx = mattrs[0]['idx']
      if is_agg:
        attr.gidx = mattrs[0]['gidx']

  def disambiguate_attrs(self, root):
    """
    Go through each operator and disambiguate the table and types
    for its attribute references.  Each Attr in the query plan
    should know which operator will provide its attribute value.

    This method should be idempotent, 
    and should be run after initializing operator schemas
    """
    ops = root.collect(Op)
    for i, op in enumerate(ops):
      if op.is_type(Source): continue
      self.disambiguate_op_attrs(op)
    self.verify_attr_refs(root)

  def verify_attr_refs(self, root):
    # Verify that all attributes are bound
    for attr in root.collect(Attr):
      if attr.idx is None:
        raise Exception("Attr %s not within scope" % attr)

  def expand_from_op(self, op):
    """
    Replace the first From operator under op with a join tree
    The algorithm is as follows

    0. Find first From operator F
    1. Find all binary expressions in any Where clause (Filter operator)
       that is an ancestor of F
    2. Keep the equality join predicates that only reference tables in
       the operator F
    3. Pick a join order 
    """

    # pick the first From clause to replace with join operators
    fromop = op.collectone("From")
    sources = fromop.cs
    sourcealiases = [s.alias for s in sources]

    # get all equi-join predicates 
    filters = op.collect("Filter")
    preds = []
    for f in filters:
      if fromop.is_ancestor(f):
        for e in f.collect(Expr):
          if self.valid_join_expr(e):
            preds.append(e)

    join_tree = None
    for source in sources:
      if join_tree is None:
        join_tree = source
      else:
        join_tree = ThetaJoin(join_tree, source, Bool(True))
    
    
    # XXX: Uncomment the following two lines of code and run this file to
    #      try out our naive Selinger implementation.  Note that the current
    #      implementation does NOT generate good plans because you need to implement
    #      cost and cardinality estimates first!
    # opt = SelingerOpt(self.db)
    # join_tree = opt(preds, sources)

    fromop.replace(join_tree)
    return op

  def valid_join_expr(self, expr):
    """
    @expr     candidate join expression
              assumes attrs have been disambiguated and have 
              tablenames attached

    Checks that expression is a valid join expression.
    A valid join expression is a = operation that compares
    an attribute in each table

    e.g.,
         T.a = S.b            -- is valid
         T.a = T.b            -- not valid
         T.a = S.b + 1        -- not valid
    """

    if expr.op != "=":
      return False

    l, r = expr.l, expr.r

    if not (isinstance(l, Attr) and isinstance(r, Attr)):
        return False
    if l is None or r is None:
      return False
    if l.tablename is None:
      raise Exception("Left side of join condition needs to be disambiguated: %s" % expr)
    if r.tablename is None:
      raise Exception("Right side of join condition needs to be disambiguated: %s" % expr)
    return l.tablename != r.tablename


class SelingerOpt(object):
  def __init__(self, db):
    self.db = db
    self.costs = dict()
    self.cards = dict()

    self.DEFAULT_SELECTIVITY = 0.05

  def __call__(self, preds, sources):
    self.sources = sources
    self.preds = preds
    self.pred_index = self.build_predicate_index(preds)
    self.plans_tested = 0

    # This is an exhaustive algorithm that uses recursion
    # You will implement a faster bottom-up algorithm based on Selinger
    plan = self.best_plan_exhaustive(sources)
    
    # XXX: Uncomment the following once you have implemented best_plan()
    # plan = self.best_plan(sources)


    # print "# plans tested: ", self.plans_tested
    return plan


  def build_predicate_index(self, preds):
    """
    @preds list of join predicates to index

    Build index to map a pair of tablenames to their join predicates
    e.g., 
    
      SELECT * FROM A,B WHERE A.a = B.b 
   
    creates the lookup table:
   
      A,B --> "A.a = B.b"
      B,A --> "A.a = B.b"
   """
    pred_index = defaultdict(list)
    for pred in preds:
      lname = pred.l.tablename
      rname = pred.r.tablename
      pred_index[(lname,rname)] = pred
      pred_index[(rname,lname)] = pred
    return pred_index

  def get_join_pred(self, l, r):
    """
    @l left subplan
    @r right Scan operator

    This method looks for any predicate that involves a table in the left
    subplan and right Scan operator.  If it can't find a predicate, then it 
    returns the predicate True
    """
    if l.is_type(Scan):
      key = (l.alias, r.alias)
      return self.pred_index.get(key, Bool(True))
    for lsource in l.collect("Scan"):
      key = (lsource.alias, r.alias)
      if key in self.pred_index:
        return self.pred_index[key]
    return Bool(True)


  def best_plan(self, sources):
    """
    @sources list of tables that we will build a join plan for

    This implements a Selinger-based Bottom-up join optimization
    and returns a left-deep ThetaJoin plan.  The algorithm 

    1. picks the best 2-table join plan
    2. then iteratively picks the next table to join based on
       the cost model that you will implement.  

    """
    # make a copy of sources 
    sources = list(sources)

    # No need for optimizer if only one table in the FROM clause
    if len(sources) == 1:
      return sources[0]

    best_plan = self.best_initial_join(sources)
    sources.remove(best_plan.l)
    sources.remove(best_plan.r)

    # each iteration of this while loop adds the best table to join
    # with current best plan
    while sources:
      best_cand = None
      best_cost = float("inf")
      for r in sources:
        self.plans_tested += 1
        pred = self.get_join_pred(best_plan, r)

        
        # XXX: Write code to construct a candidate plan with r as the
        # inner table, and compute its cost using self.cost()
        #
        # Keep the lowest cost candidate plan in best_cand
        #      Make sure to track the child operators' 
        #      parent pointers, as described in best_plan_exhaustive()
        pass

      best_plan = best_cand
      sources.remove(best_plan.r)

    return best_plan


  def best_initial_join(self, sources):
    """
    @sources base taobles

    Try all 2-table join candidates and return the one with
    the lowest cost, as defined by self.cost()
    """
    best_plan = None
    best_cost = float("inf")

    for (l, r) in product(sources, sources):
      if l == r: continue
      self.plans_tested += 1
      pred = self.get_join_pred(l, r)

      # XXX: Write your code here
      #      Make sure to track the child operators' 
      #      parent pointers, as described in best_plan_exhaustive()

    return best_plan

  def best_plan_exhaustive(self, sources):
    """
    @sources list of tables that we will build a join plan for
    @return A left-deep ThetaJoin plan

    This is an example implementation of a exhaustive plan optimizer.
    It is slower than the bottom-up Selinnger approach
    that you will implement because it ends up checking the same candidate
    plans multiple times.  

    This code is provided to give you hints about how to use the class 
    methods and implement the bottom-up approach
    """
    if len(sources) == 1: return sources[0]

    best_plan = None
    best_cost = float("inf")
    for i, r in enumerate(sources):
      rest = sources[:i] + sources[i+1:]
      rest_plan = self.best_plan_exhaustive(rest)
      if rest_plan is None:
        continue

      self.plans_tested += 1
      pred = self.get_join_pred(rest_plan, r)
      plan = self.create_new_join_plan(ThetaJoin, rest_plan, r, pred)
      cost = self.cost(plan)

      if cost <= best_cost:
        plan.init_schema()
        plan.l.p = plan
        plan.r.p = plan
        best_plan, best_cost = plan, cost

    return best_plan

  def create_new_join_plan(self, join_klass, l, r, pred):
    """
    When an operator is initialized, the constructor
    modifies the child operators' parent pointers
    
    During join optimization, costs are computed top down,
    so once we create a new join operator, we need to make sure
    the left and right child operators preserve their original parents.
    """
    lp = l.p
    rp = r.p
    plan = join_klass(l, r, pred)
    l.p = lp
    r.p = rp
    return plan


  def cost(self, join):
    """
    @join a left-deep join subplan
    @returns join cost estimate

    Estimate the cost to execute this join subplan
    """
    # internally cache cost estimates so they don't need to be recomputed
    if join in self.costs:
      return self.costs[join]

    if join.is_type(Scan):
      # XXX: Implement the cost to scan this Scan operator
      # Take a look at db.py:Stats, which provides some database statistics.
      # To use its functionality, you may need to implement parts of db.py
      cost = 0
    elif join.is_type(Join):
      # XXX: Compute the cost of the tuple-based nested loops join operation
      # in terms of the cost to compute the outer (left) subplan and the number
      # of tuples we need to examine from the inner (right) table.
      #
      # Hint: You may want to compute the cost recursively.
      cost = 0

      # We penalize high cardinality joins a little bit
      cost += 0.1 * self.card(join)
    elif join.is_type(SubQuerySource):
      cost = self.cost(join.c)
    else:
      cost = self.card(join)

    # save estimate in the cache
    self.costs[join] = cost
    return cost

  def card(self, join):
    """
    @join join subplan 
    @returns join cardinality estimate

    Compute the cardinality estimate of the join subplan
    """
    # We cache the cardinality estimates
    if join in self.cards:
      return self.cards[join]

    if join.is_type(Scan):
      # XXX: Compute the cardinality of the join if it is a Scan operator
      # Similar to self.cost() above, take a look at db.py:Stats.
      card = 1
    elif join.is_type(Join):
      # XXX: Compute the cardinality of the join subplan as described in lecture.
      # Hint: You may want to compute the cardinality recursively
      card = 1
    elif join.is_type(SubQuerySource):
      card = self.card(join.c)
    else:
      card = 0.05

    # Save estimate in the cache
    self.cards[join] = card
    return card

  def selectivity(self, join):
    """
    @join join subplan

    Computes the selectivity of the join depending on the number of
    tables, the predicate, and the selectivities of the join attributes
    """
    
    if join.is_type(Scan):
      return self.DEFAULT_SELECTIVITY

    # if the predicate is a boolean, then the selectivity
    # is 1 if True (cross-product), or 0 if False
    if join.cond.is_type(Bool):
      return join.cond(None) * 1.0

    lsel = self.selectivity_attr(join.l, join.cond.l)
    rsel = self.selectivity_attr(join.r, join.cond.r)
    return min(lsel, rsel)

  def selectivity_attr(self, source, attr):
    """
    @source the left or right subplan
    @attr  the attribute in the subplan used in the equijoin

    Estimate the selectivity of a join attribute.  
    We make the following assumptions:

    * if the source is not a base table, then the selectivity is 1
    * if the attribute is numeric then we assume the attribute values are
      uniformly distributed between the min and max values.
    * if the attribute is non-numeric, we assume the values are 
      uniformly distributed across the distinct attribute values
    """
    if not source.is_type(Scan):
      return 1.0

    table = self.db[source.tablename]
    stat = table.stats[attr]
    typ = table.schema.get_type(attr)
    if typ == "num":
      # XXX: Write code to estimate the selectivity of the numeric attribute.
      # You can add 1 to the denominator to avoid divide by 0 errors
      sel = 1.0
    elif typ == "str":
      # XXX: Write code to estimaote the selectivity of the non-numeric attribute
      sel = 1.0
    else:
      sel = 0.05
    return sel
