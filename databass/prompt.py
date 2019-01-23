import time
import traceback
import readline
import click
from . import *

WELCOMETEXT = """Welcome to DataBass.  
Type "help" for help, and "q" to exit"""


HELPTEXT = """
List of commands

<query>                           runs query string
COMPILE [AND RUN] <query>         compile and optionally run query string
PARSE [query or expression str]   parse and print AST for expression or query
TRACE                             print stack trace of last error
SHOW TABLES                       print list of database tables
SHOW <tablename>                  print schema for <tablename>
"""

def compile_and_write(plan, fname="./_code.py", funcname="compiled_q"):
  ctx = Context()
  plan.produce(ctx)
  code = ctx.compiler.compile_to_func(funcname)
  code_w_comments = """'''\n%s\n'''\n\n%s\n""" % (
      plan.pretty_print(), code)

  header = """
from databass import *
import time
db = Database.db()"""
  footer = """
if __name__ == "__main__":
  start = time.clock()
  for row in compiled_q(): 
    print row
  end = time.clock()
  print "Compiled query took %f seconds" % (end - start)
"""

  with file(fname, "w") as out:
    out.write(header)
    out.write("\n")
    out.write(code)
    out.write("\n")
    out.write(footer)
  return code

def parse_and_optimize(qstr):
  plan = parse(qstr)
  plan = Yield(plan)
  opt = Optimizer()
  opt.initialize_plan(plan)
  opt.disambiguate_attrs(plan)
  plan = opt(plan)
  opt.initialize_plan(plan)
  opt.disambiguate_attrs(plan)
  return plan

if __name__ == "__main__":

  @click.command()
  def main():
    print(WELCOMETEXT)
    service_inputs()

  def service_inputs():
    cmd = raw_input("> ").strip()

    _db = Database.db()

    if cmd == "q":
      return

    elif cmd == "":
      pass

    elif cmd.startswith("help"):
      print(HELPTEXT)

    elif cmd.upper().startswith("TRACE"):
      traceback.print_exc()

    elif cmd.upper().startswith("PARSE"):
      q = cmd[len("PARSE"):]
      ast = None
      try:
        ast = cond_to_func(q)
      except Exception as err_expr:
        try:
          ast = parse(q)
        except Exception as err:
          print("ERROR:", err)

      if ast:
        print(ast.pretty_print())


    elif cmd.upper().startswith("SHOW TABLES"):
      for tablename in _db.tablenames:
        print tablename
      
    elif cmd.upper().startswith("SHOW "):
      tname = cmd[len("SHOW "):].strip()
      if tname in _db:
        print "Schema for %s" % tname
        for attr in _db[tname].schema:
          print attr.aname, "\t", attr.typ
      else:
          print "%s not in database" % tname

    elif cmd.upper().startswith("COMPILE "):
      cmd = cmd[len("COMPILE "):].strip()
      b_run = False
      if cmd.upper().startswith("AND RUN "):
        b_run = True
        cmd = cmd[len("AND RUN "):].strip()

      try:
        plan = parse_and_optimize(cmd)
        print plan.pretty_print()
        code = compile_and_write(plan, "./_code.py", "compiled_q")
        print
        print code
        print
        print "wrote compiled query to ./_code.py.  Type `python _code.py` to run it."

        if b_run:
          print "Running compiled query"
          exec(code)
          start = time.clock()
          for row in compiled_q():
            print row
          end = time.clock()
          print "Compiled query took %f seconds" % (end - start)

      except Exception as err:
        print("ERROR:", err)

    else:
      try:
        plan = parse_and_optimize(cmd)
        print plan.pretty_print()
        start = time.clock()
        for row in plan:
          print row
        end = time.clock()
        print "Interpreted query took %f seconds" % (end - start)
      except Exception as err:
        print("ERROR:", err)

    del _db
    service_inputs()


  main()
