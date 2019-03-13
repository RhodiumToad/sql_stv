# sql_stv

Single Transferable Vote as a single SQL query.

This query attempts to implement STV using the algorithm specified in
the
[Scottish Local Government Elections Order 2007](http://www.legislation.gov.uk/ssi/2007/42/schedule/1/part/III/crossheading/counting-of-votes/made),
but it has not been validated by anyone or even tested all that much;
there are probably bugs.

The author makes NO WARRANTY OF CORRECTNESS whatsoever; use at own
risk.

The objective in this case is to produce a completely deterministic
query, i.e. when run on the same source dataset it should produce the
same result. But the specified algorithm calls for casting of lots at
various places to break ties; we resolve this conflict by requiring
that lots for each round are pre-cast.


