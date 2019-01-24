---
layout: page
---

# HW0 Expression Evaluation

* Released: 1/23 12PM
* Due: 1/27 12PM**

## Overview:

The goal of this homework is to get you familar with DataBass and muck around in parts of the codebase.
Take a look at the [README](../README.md) in this repo to get an overview of the system and how to set it up.
We have written a short description of the [system design](./design.md). 
Then read these instructions. If you get need any help, please reach us on Piazza.

## Background

Let's say we have the following expression in a SQL query

        a + (1 * 9)

This is parsed into an expression tree of the form:

            +
           / \
          a   *
             / \
            1   9

In a typical database, the expression is evaluated by interpreting this tree.
Each node in the tree is an Operator object.
The root of the expression is actually a binary operator whose operator is `+`,
and the left and right children are `a` and the subtree for `*`.
The expression is evaluated by recursively evaluating the children, getting their value, looking up the function to add the two values, and then returning:

      def eval():
        left_val = left_child.eval()
        right_val = right_child.eval()
        if op == "=":
          return left_val == right_val
        if op == "+":
          return left_val + right_val

This incurs the overhead of function calls, context switches, if/else statements, etc.
In contrast, if we know that the tuple is a dictionary called `row`,
then we could magically compile the tree into the following Python statement that would run much much faster:

        row['a'] + (1 * 9)

## Tasks

#### Task 1: Interpreted Expressions

As a warmup, you will add more functionality to expressions.
Edit the code in `exprs.py` to support the unary operations "-", and "not",
in addition to the binary operations "/", "and", ">=", "<=", and "==".
"==" is simply a synonym for "=".
We have added comments to the places where you may need to edit in exprs.py file.

Run the following as a sanity check that your implementation works.  Note that these tests are not exhaustive:

        $ nosetests test/hw0_test.py:TestUnits.test_interpreted_exprs


#### Task 2: Compiled Expressions

Write the compiled version for expression evaluation by completing `Expr.compile()`.
`compile()` turns an Expr object into python code that can be evaluated.
Since the expression is a tree, the compilation process needs to stitch together the code generated from each operator in the tree.  
We provide the helper classes `Context` and `Compiler` to help you with generating the compiled Python code,
and manage the stack and other shared information during compilation.

Test your code by running:

        $ nosetests test/hw0_test.py:TestUnits.test_compiled_exprs

### Submission

Use the submit.py script to package your code. Run it from inside the main databass folder.
The assignment prefix is hw0.

Check the file to make sure no extra files were added.

Finaly, submit the file through this [link](https://www.dropbox.com/request/1LyaKG5BbALxc9p2RrOX).
