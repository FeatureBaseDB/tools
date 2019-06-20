# A sample schema

This is loosely inspired by things like the Star Schema Benchmark, with the
idea being to create a tool to let similar tests be run on different scales,
against an arbitrary install. This schema is not yet fully implemented;
what follows is some sketchy design notes.

## Indexes

All indexes used by this tool have names starting with `imaginary-`. (Or
another name specified with `--prefix`) The intent here is that it should be
safe to delete these indexes after tests.

## Sample Data

In the interests of visualization, we imagine a system which has data about
various entities, called `users`, and things that might happen involving
those users, called `events`. We also add another table, `supplemental`,
which has different data from `users` but the same column space, to facilitate
cross-index benchmarks. Benchmarks might well operate on only one of
these indexes, but having three of them lets us do interesting comparisons.

### Users

The main index is a table of information about users. Fields:
	age (int)
	income (int)
	favorite numbers (N rows, bits indicate "user likes number")
	least favorite numbers (N rows, bits indicate "user dislikes number")

The fields are obviously made-up, but they let us vary broadly in cardinality
without having to have a giant list of "ingredients one could have an allergy
to" or something. We will probably want/need other fields also.

We should probably have column keys, even if they're obviously-trivial (like
"the column ID expressed as a string with a prefix"), because that's a
use case we potentially care about.

### Events

The second table, `events`, has an int field which contains column IDs from the
user table. This allows comparisons to go either of two ways; you can perform
queries on this table, then create a new row which has bits set for all the
entries in the `userid` field for the returned columns, or you can perform
queries on `users`, then select entries from this table which have a value in
that set of columns present in their `userid` field. Events likewise have
other fields:
	timestamp of event
	int fields
	count-type fields
	fields which encourage union/intersection queries

### Supplemental

The third table, `supplemental`, contains additional details about users; the
column space for this table, and the column space for `users`, are the same.
This is useful in cases where there is a reason to have two indexes which
refer to the same columns, and not to combine those indexes. The likely join
use case is to perform queries on one, then use that in intersections or unions
with queries on the other. Fields:
	???
