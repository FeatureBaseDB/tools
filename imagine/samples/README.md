I'd like to see something like each of the following 6 cases with a single in
column order import followed by random updates.

- high cardinality (10 million?), long tail of values with very few bits set.
  Some columns have only a few (or zero) bits set, some have thousands.
  in column order import followed by random updates.

- medium/low cardinality set field (hundreds). zipfian distribution among
  values. 95% of columns have 1 value. 4% have more, 1% have none. (roughish numbers)

- low cardinality mutex field (3), even distribution, 99% of columns have values.

- 16 bit BSI field 99% of columns have a value. zipfian distribution within the 16 bit range
- 32 bit BSI field 99% of columns have a value. zipfian distribution within the 32 bit range
- 64 bit BSI field 99% of columns have a value. simulated high precision
  timestamp - each update increases slightly from the previous number.
