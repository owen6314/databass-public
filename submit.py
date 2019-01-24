import click
import subprocess
import shutil

assignments = ['hw%s' % i for i in range(0,4)]
digits = "1234567890"
check_bad_uni = lambda uni: uni is None or any(c not in digits for c in uni[-4:])


def validate_uni(ctx, param, uni):
  if check_bad_uni(uni):
    if param == "-u2" and uni == 'NONE': 
      return
    msg = "UNI should be in the format of AA1234.\nYou submitted: %s" % uni
    raise click.BadParameter(msg)
  return uni

@click.command()
@click.option("-u1", 
  prompt="Your UNI in the format of AA1234", 
  help="UNI in AA1234 format",
  callback=validate_uni)
@click.option('-a', prompt="The assignment you are submitting", type=click.Choice(assignments))
@click.option('-q', is_flag=True, help="Submit without prompting")
def main(u1, a, q):
  """
  Script to package up your DataBass submission.  You should run this in a UNIX-based environment.
  """
  uni1 = u1
  assignment = a

  if check_bad_uni(uni1):
    print("Your UNI should be in the format of AA1234.")
    print("You submitted: %s" % uni1)
    return 

  if assignment == None:
    print("Choose an assignment.  Use --help to see options")
    return

  if not q:
    cmd = raw_input("You submitted %s and %s.  Type Y to submit: " % (uni1, assignment))
    if cmd.lower() != "y":
      return

  # Package and check the code
  fname = "%s_%s" % (assignment, uni1)
  shutil.make_archive(fname, 'zip', "databass", ".")


if __name__ == "__main__":
  main()
