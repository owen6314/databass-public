"""
Define the structure of aggregate and scalar UDFs, and the UDF registry.
"""
import numpy as np

class UDF(object):
  """
  Wrapper for registering metadata about UDFs.

  TODO: add setup function/dependencies so that compiler can generate the
        appropriate import statements and definitions.
  """
  def __init__(self, name, nargs):
    self.name = name
    self.nargs = nargs
    self.f = None

  @property
  def is_agg(self):
    return False

class AggUDF(UDF):
  def __init__(self, name, nargs, f=None):
    UDF.__init__(self, name, nargs)
    self.f = f

  @property
  def is_agg(self):
    return True

  def __call__(self, *args):
    if len(args) != self.nargs:
      raise Exception("Number of arguments did not match expected number.  %s != %s" % (len(args), self.nargs))
    if not all(isinstance(arg, list) or isinstance(arg, tuple) for arg in args):
      print args
      raise Exception("AggUDF expects each argument to be a column.")
    return self.f(*args)

class ScalarUDF(UDF):
  def __init__(self, name, nargs, f=None):
    UDF.__init__(self, name, nargs)
    self.f = f

  def __call__(self, *args):
    if len(args) != self.nargs:
      raise Exception("Number of arguments did not match expected number.  %s != %s" % (len(args), self.nargs))
    return self.f(*args)

class UDFRegistry(object):
  """
  Global singleton object for managing registered UDFs
  """
  _registry = None

  def __init__(self):
    self.scalar_udfs = {}
    self.agg_udfs = {}

  @staticmethod
  def registry():
    if not UDFRegistry._registry:
      UDFRegistry._registry = UDFRegistry()
    return UDFRegistry._registry

  def add(self, udf):
    if isinstance(udf, AggUDF):
      if udf.name in self.scalar_udfs:
        raise Exception("A Scalar UDF with same name already exists %s" % udf.name)
      self.agg_udfs[udf.name] = udf

    elif isinstance(udf, ScalarUDF):
      if udf.name in self.agg_udfs:
        raise Exception("A Agg UDF with same name already exists %s" % udf.name)
      self.scalar_udfs[udf.name] = udf

  def __getitem__(self, name):
    if name in self.scalar_udfs:
      return self.scalar_udfs[name]
    if name in self.agg_udfs:
      return self.agg_udfs[name]
    raise Exception("Could not find UDF named %s" % name)


# Prepopulate registry with simple functions
registry = UDFRegistry.registry()
registry.add(ScalarUDF("lower", 1, lambda s: str(s).lower()))
registry.add(AggUDF("avg", 1, np.mean))
registry.add(AggUDF("count", 1, len))
registry.add(AggUDF("sum", 1, np.sum))
registry.add(AggUDF("std", 1, np.std))
registry.add(AggUDF("stddev", 1, np.std))


if __name__ == "__main__":

  udf = registry["sum"]
  print udf.f([1,2,3])
