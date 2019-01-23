class Stats(object):
  
  # XXX: Edit this to compute the table cardinality
  def __init__(self, table):
    self.table = table
    self.card = 10

  # XXX: edit this to return the domain of the attr
  def __getitem__(self, attr):
    return [0, 1]


