---
layout: page
---


## HW2: Query Compilation 

* Released: 2/07 12PM
* Due: 2/26 11:59PM**
* Max team size: 2


In this assignment, you will implement query compilation for some basic relational operators, by using the produce consume model described in the Neumann paper.  To make your life easier, we have included compilation implementations for SubQuerySource, Scan, Filter, Distinct, Yield, and Print.  

You will implement:

* Projection (without aggregation)
* Order by
* Nested loops join

We recommend implementing them in that order, since the difficulty gets progressively harder.


For instance, the following query:


        SELECT a + 1 as z, b * a as b FROM data ORDER BY z

Will be compiled into a program such as:

		def compiled_q():
		  ord_schema_0 = Schema([Attr('z', 'num', None),Attr('b', 'num', None)])
		  ord_rows_0 = []
		  def ord_keyf_0(row):
			tmp_0 = row[0]
			return (tmp_0,)
		  proj_row_0 = ListTuple(Schema([Attr('z', 'num', None),Attr('b', 'num', None)]))
		  for scan_row_0 in Database.db()['data']:
			expr_0 = scan_row_0[0]
			expr_1 = 1.0
			tmp_1 = (expr_0) + (expr_1)
			expr_2 = scan_row_0[1]
			expr_3 = scan_row_0[0]
			tmp_2 = (expr_2) * (expr_3)
			proj_row_0.row[:] = [tmp_1, tmp_2]
			ord_rows_0.append(ListTuple(ord_schema_0, list(proj_row_0.row)))
		  ord_rows_0.sort(key=ord_keyf_0)
		  for ord_irow_0 in ord_rows_0:
			yield ord_irow_0


## Compiling Queries

Our approach to query compilation follows the producer-consumer approach outlined in [Efficiently Compiling Efficient Query Plans for Modern Hardware](https://w6113.github.io/files/papers/p539-neumann.pdf).


#### 

The same idea as expression compilation applies to query plans.  Rather than using the Iterator model to interpret the query plan, we would like to generate raw Python code to run.  For example, the following query

        SELECT a + b AS val
        FROM data
        WHERE a > b

would ideally be compiled into the following program, where  `db` is a Database object that contains the table `data`.

        def q(db):
          for row in db['data']:
            if row['a'] > row['b']:
              val = row['a'] + row['b']
              yield dict(val=val)

The challenge is we can't just perform compilation in the same way we evaluate a query plan using the pull-based iterator model.  Take a look at the query plan for the above query:

            Project(a + b AS val)
                   |
              Filter(a > b)
                   |
              Scan(data)

Notice that project is at the _top_ of the query plan, whereas it is in the innermost block in the compiled program above.  In contrast, the Scan operator is at the _bottom_ of the query plan, even the for loop to scan the table is the first line of the compiled function.  If we asked Project to generate its code, and then called its child to generate the Filter code, we would have generated code in the opposite order:

        val = row['a'] + row['b']
        yield dict(val = val)
        if row['a'] > row['b']:
        ...

This is why the [Generating code for holistic query evaluation](https://w6113.github.io/files/papers/krikellas-icde2010.pdf) generates its code by first topologically sorting the query plan from the bottom operators (access methods) to the root operator.  

The produce-consumer model is one way to address this issue.  

#### The Producer Consumer Model

The main idea is that we want Scan to generate its code first, and then Filter, and finally Project.  To do so, split compilation into two phases. The _produce_ phase that follows the ↓ arrows, and its purpose is to initialize the required state for each operator (setup hash tables, temporary variables, etc), and to allocate variables to hold te tuples read by the access methods (e.g., Scan).  The _consume_ phase follows the ↑ arrows to use the populated variables actual data processing.  

                Project(a + b AS val)
                     |    ↑  
           produce   |    |    consume
                     ↓    |  
                  Filter(a > b)
                     |    ↑ 
           produce   |    |    consume
                     ↓    |  
                   Scan(data) 
     

This is implemented by adding `produce()` and `consume()` methods to each query operator:

          class Op(object):

            def produce(self):
              # call child operator's produce()

            def consume(self):
              # generate Python code with proper indentation
              # then call parent operator's consume()



There are a few things to keep in mind when doing this assignment.

1. As control flows down along the produce calls, operators can register variables they want the descendant operators to help populate.  For instance, the Filter operator expects the child operator to give it the name of the variable for the tuple it will evaluate the predicate expression over.  Thus it needs to request a "row" variable by calling `ctx.request_vars(dict(row=None))`.   Its child operator is responsible for assigning this value using `ctx['row'] = <row variable name>`.  Finally, once control passes back to `Filter.consume()`, it figures out the variable name for its input row by accessing `ctx['row']`.
2. The Context object is passed between operators as an argument in the produce/consume calls.   It provides a way to allocate new variables by calling `ctx.new_var("prefix")`, which allocates a new var `"prefix_10"`, if its the 10th variable allocated with the same prefix.  
3. You saw the use of `ctx.add_io_vars` when compiling expressions in HW0.  When you compile expressions in this assignment's operators, you want to make sure to use it to pass in the variable names of the input row the expression will be evaluated on, and possible the output variable name that the expression result is written to.  

Finally, it will help to look at the following code/comments:

* See the provided produce/consume implementations for Scan, Filter, Distinct for examples.
* See `Context` in `complier.py` to understand working with compiler variables in more detail.
* If you look at the appendix of the [Neumann paper](https://w6113.github.io/files/papers/p539-neumann.pdf), it goes into more details about their context information, which passes down to the access methods the columns of interest, so it doesn't read more columns than needed.  Ours is a stripped down version.    The appendix also goes into more details with code snippets for different operator implementations.

## The Assignment


#### Part 1: Projection (without aggregation)

Implement the Projection operator.  Since Project will manufactor new tuples, we want it to initialize a single Tuple up front (in produce) that it will populate for each record it sends to its parent.  You can think of this as allocating memory for a tuple, and filling it in, rather than allocating new memory for each input tuple.  Since we are generating Python code, it probably doesn't matter, but we do it for good hygiene.

Take a look at the comments to note a special case when running Project.  It's basically for the query

        SELECT 1

This project doesn't have a subplan to generate rows.  In this case, project should supply a dummy tuple that it then tries to populate using its expression list.  

In the consume method, you can use the `self.compile_exprs()` method defined in `baseops.py` to execute an array of expressions and retrive the temporary values their results are stored in.

Run the following as a sanity check that your implementation works.  Note that these tests are not exhaustive:

        python test/hw2_test.py:TestUnits.test_project

#### Part 2: Order By

Test your code by running:

        python test/hw2_test.py:TestUnits.test_orderby


#### Part 3: Theta Join

Finally, you will implement nested loops join.  As a recap, given the left and right childs, you will end up generating code similar to the following _pseudocode_:

        for lrow in leftchild
          for rrow in rightchild
            irow = lrow + rrow
            cond = predicate_expression(irow)
            if cond
              call parent consume

The tricky part is join is a binary operator.  Logically, you will want the control flow to flow down to the left child, which will flow back up to the join, and then flow down to the right child, and then back up to the join.  

                        join
                      / /  \ \
                     ↓ ↑    ↓ ↑ 
                    left    right

Thus `join.consume()` will be called twice: once by the left produce call, and once for the right produce call.  It will
be important for you to keep track of whether `join.consume()` is being colled for the first or second time, since each call
is responsible for different logic.

Test your code by running:

        $ python test/hw2_test.py:TestUnits.test_thetajoin

### Submission


Use the submit.py script to package your code. Run it from inside the main databass folder.  When prompted, give your UNI first, then your partner's.  If you did not work with a partner, put NONE in all caps.

        python submit.py --help

The assignment prefix is `hw2`.

Check the file to make sure no extra files were added.

Finaly, submit the file through this [link](https://www.dropbox.com/request/oEz89evE6zXk0LJrf8Vz).

