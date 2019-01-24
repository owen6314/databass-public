<img src="./docs/databass-small.png" width="600"></img>


# DataBass: Not Quite a DataBase

This isn't your average database. This will be the base of operations for exanding your data processing knowledge!  The base of your data exploration in this class!  It will cover the bassics of query execution that you will learn in class!

We present to you....  the **DATABASS..Bass...bass**.  


## Getting Started

This is a simple Python-based analytical database for instructional purposes.  See the [system design](./docs/design.md) for details.

Installation

    git clone git@github.com:w6113/databass-public.git

    # turn on virtualenv

    pip install click pandas numpy parsimonious readline nose


If you are a Columbia student and [have a clic account](https://www.cs.columbia.edu/~crf/accounts/cs.html), you can install and edit databass on clic.  That way you can minimize computer environment issues:

    # ssh into clic
    ssh <your user name>@clic.cs.columbia.edu

    # create virtual environment and enable it
    mkvirtualenv test
    workon test

    git clone git@github.com:w6113/databass-public.git
    pip install click pandas numpy parsimonious readline nose


### Take DataBass for a Spin.  

Do the following to run the DataBass console:

    python -m databass.prompt

Below is an example session using the prompt.  The user input is the text after the `> ` character.


	Welcome to DataBass.
	Type "help" for help, and "q" to exit
	> help

	List of commands

    <query>                           runs query string
    COMPILE [AND RUN] <query>         compile and optionally run query string
    PARSE [query or expression str]   parse and print AST for expression or query
    TRACE                             print stack trace of last error
    SHOW TABLES                       print list of database tables
    SHOW <tablename>                  print schema for <tablename>


You can see how simple expressions are parsed.  Note that operator precedence needs to be specified explicitly using parens:

	> parse 1=2 and a=b
    1.0 == (2.0 and (a == b))

    > parse (1=2) and (a=b)
    (1.0 == 2.0) and (a == b)

	> parse (1+2*a) / 10
	(1.0 + (2.0 * a)) / 10.0

Or the parsed query plan of a SQL query

	> parse SELECT 1+2*a AS a FROM data WHERE a > 1
    Project(1.0 + (2.0 * a) AS a)
      Filter(a > 1.0)
        From
          Scan(data AS data)

When the program starts, DataBass automatically crawls all subdirectories and loads any CSV files that it finds into memory.  In our example, [databass/data](./databass/data) contains two CSV files: [data.csv](./databass/data/data.csv) and [iowa-liquor-sample.csv](./databass/data/iowa-liquor-sample.csv).

	> show tables
    data_orig
    data
    iowa-liquor-sample
    data2

	> show data
	Schema for data
    a       num
    b       num
    c       num
    d       num
    e       str
    f       num
    g       str

You can execute a simple query, and it will print the query plan and then the result rows.  

	> SELECT 1
    Yield()
      Project(1.0 AS attr0)
    (1.0)
    Interpreted query took 0.000019 seconds

	> SELECT * FROM data LIMIT 2
	Yield()
	  LIMIT(2.0 OFFSET 0)
		Project(data.a AS a, data.b AS b, data.c AS c, data.d AS d, data.e AS e, data.f AS f, data.g AS g)
		  Scan(data AS data)
	(0, 0, 0, 0, a, 2, c)
	(1, 1, 1, 0, b, 4, d)
	Interpreted query took 0.000053 seconds


Finally, some of the assignments will involve query compilation.  To compile a query, using the `COMPILE` command.  It will print the query plan, the compiled code as a function called `compiled_q()`, and also write it out to a python file that you can run.

    > COMPILE SELECT 1

	Yield()
	  Project(1.0 AS attr0)

	def compiled_q():
	  proj_row_0 = ListTuple(Schema([Attr('attr0', 'num', None)]))
	  tmp_0 = 1.0
	  proj_row_0.row[:] = [tmp_0]
	  yield proj_row_0

	wrote compiled query to ./_code.py

You can run the compiled query with the command `COMPILE AND RUN`: 

	> COMPILE AND RUN SELECT 1                                                                  
	Yield()
	  Project(1.0 AS attr0)

	def compiled_q():
	  proj_row_0 = ListTuple(Schema([Attr('attr0', 'num', None)]))
	  tmp_0 = 1.0
	  proj_row_0.row[:] = [tmp_0]
	  yield proj_row_0

	wrote compiled query to ./_code.py
	Running compiled query
	(1.0)
	Compiled query took 0.000032 seconds

### Run Tests

To run tests, use the `nose` python test framework by specifying which tests in the `test/` directory to run:

    nosetests test/exprs.py

    # run all tests
    nosetests test/*.py

For each assignment, we will provide a test of the form `test/hwX.py` that you can run.  We will a set of private tests to more comprehensively evaluate your code.

    nosetests test/hw1.py

    # run tests for homework X
    nosetests test/hwx.py

