---
layout: page
---

# HW1 Iterator Model

* Released: 1/29 12PM
* Due: 2/07 11:59PM**

In this assignment, you will implement the iterator model for the DataBass query execution engine.  This will get you familiarized with how a class row-oriented engine might run.    To do so, you will need to implement two main things: how to infer and initialize the output schema for a subset of the operators, and the `__iter__()` method for most operators.  We have provided default implementations for the Scan and Filter operators.

The primary file to modify is [databass/ops.py](../databass/ops.py).

We have provided implementations of the following operators:

* Scan
* Filter
* ThetaJoin (aka nested loops join)

You will implement the following operators: 

* SubQuerySource
* Hashjoin
* GroupBy
* Project
* OrderBy
* Limit
* Distinct

## init_schema

The purpose of `init_schema` is to infer the output schema for an operator, given the schema of its child operators.  After a query plan is constructed, DataBass performs a bottom-up traversal of the plan (starting from leaf operators) and calls `init_schema`.   You can see this in `optimizer.py:initialize_plan`.   We have implemented `init_schema` for Scan, Filter, and in the base operators (baseops.py).

The reason why initializing the schemas is important is because each Attr reference in an operator's expressions (say in a project clause or predicate) needs to know  how to index into a tuple's list of values in order to retreive the appropriate value.   Rather than doing this dynamically, we want to figure out the index during query parsing/initialization so that later lookups are faster.  You will see how the code in `optimizer.py:disambiguate_op_attrs()` goes through each Attr reference and tries to disambiguate it and figure out the index value (stored in Attr.idx).

This is particularly useful in the next homeworks, which perform actual query compilation of the query plan into raw Python loops.

## iterator

Each `__iter__` method is a [Python generator](https://wiki.python.org/moin/Generators) that is implemented by calling `yield` on each output row.  This lets you loop through an operator, which you can see in our implementation of Filter and ThetaJoin.  

Typically, query engines try to avoid allocating new memory for every intermediate row by pre-allocating a "tuple slot" whose pointer is simply passed around.  Thus each operator's job is to populate the contents of this single slot that the parent operator will read.  Even though DataBass is implemented in Python, we still implement a similar strategy.


## Tasks

We have split HW1 into three phases, from simpler operators to more challenging operators.   

#### Phase 1:

You will implement

* `Distinct.__iter__()`
* `Limit.__iter__()`
* `OrderBy.__iter__()`

Test your code by running:

        $ nosetests test/hw1_test.py:TestUnits.test_phase1

#### Phase 2: 

You will implement

* `Project.__iter__()`
* `Project.init_schema()`
* `Project.expand_stars()`

Test your code by running:

        $ nosetests test/hw1_test.py:TestUnits.test_phase2

### Phase 3:

Finally, you will implement:

* `SubQuerySource.__iter__()` 
* `SubQuerySource.init_schema()`
* `HashJoin.__iter__()`
* `GroupBy.__iter__()`
* `GroupBy.init_schema()`

Test your code by running:

        $ nosetests test/hw1_test.py:TestUnits.test_phase3

### Submission

Use the submit.py script to package your code. Run it from inside the main databass folder.  When prompted, give your UNI first, then your partner's.  If you did not work with a partner, put NONE in all caps.

        python submit.py --help

The assignment prefix is `hw1`.

Check the zip file to make sure no extra files were added.

No points will be awarded to you if only your partner submits to Courseworks and
you do not.

