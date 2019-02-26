---
layout: page
---


## HW3: Query Compilation ctd.

* Released: 2/26 12PM
* Due: 3/14 11:59PM**
* Max team size: 2


In this assignment, you will implement query compilation for some more relational operators.

You will implement:

* Hash join
* Aggregation + Projection
* Limit


See [hw2](./hw2.md) for background on query compilation.

## The Assignment


#### Hash Join

The trick here will be to allocate and maintain the hash table in the appropriate places.  

Test your code by running:

        python test/hw3_test.py:TestUnits.test_hashjoin

#### Aggregation + Projection

In addition to implementing GroupBy compilation, you may (or may not) need to ensure that project is capable of executing expressions that are aggregation functions.

Test your code by running:

        python test/hw3_test.py:TestUnits.test_groupby



#### Limit

Note that when you implement limit, it should work correctly even within subqueries or multi-join queries.  Implementing LIMIT to be efficient (e.g., the cost is on the order of the LIMIT clause rather than the query complexity) requires some tricky bookkeeping and will receive a small reward.

Test your code by running:

        $ python test/hw3_test.py:TestUnits.test_limit

### Submission

Use the submit.py script to package your code. Run it from inside the main databass folder.  When prompted, give your UNI first, then your partner's.  If you did not work with a partner, put NONE in all caps.

        python submit.py --help

The assignment prefix is `hw3`.

Check the file to make sure no extra files were added.

Finaly, submit the file through this [link](https://www.dropbox.com/request/g7jRP9c0UPCQEd4TTRMx).
