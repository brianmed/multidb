MultiDB
=======

**ALPHA** version of an embedabble, serverless SQL database that allows for multiple writers.

INSTALL
=======


```

$ git clone git@bitbucket.org:bpmedley/multidb.git 
$ cd multidb
$ cd src
$ make
...
$ cat ../t/create.sql 
CREATE TABLE site_key (
    id serial,
    site_key text,
    updated timestamp,
    inserted timestamp
);
$ cat ../t/insert.sql
INSERT INTO site_key (id, site_key, updated, inserted) VALUES (0, 'smtp_password', '2014-10-06T21:01', NULL);
$ cat ../t/select.sql
SELECT * FROM site_key;
$ ./cli_multidb --sql_create="$(cat ../t/create.sql)"
$ for i in $(seq 1 9); do ./cli_multidb --sql_insert="INSERT INTO site_key (id, site_key, updated, inserted) VALUES (0, 'smtp_password', '2014-10-06T21:01', NULL);"; done    
$ ./cli_multidb --sql_select="SELECT * FROM site_key WHERE (id > 3 AND id > 5);"
id      site_key        updated inserted
id	inserted	site_key	updated
6	NULL	'smtp_password'	'2014-10-06T21:01'
7	NULL	'smtp_password'	'2014-10-06T21:01'
8	NULL	'smtp_password'	'2014-10-06T21:01'
9	NULL	'smtp_password'	'2014-10-06T21:01'
$ ./cli_multidb --sql_delete="DELETE FROM site_key WHERE (id > 3 AND id > 5);"  
$ ./cli_multidb --sql_select="SELECT * FROM site_key WHERE (id > 3 AND id > 5);"
id	inserted	site_key	updated
$ ./cli_multidb --sql_insert="INSERT INTO site_key (id, site_key, updated, inserted) VALUES (0, 'smtp_password', '2014-10-06T21:01', NULL);"
$ ./cli_multidb --sql_select="SELECT * FROM site_key WHERE (id > 3 AND id > 5);"
id	inserted	site_key	updated
10	NULL	'smtp_password'	'2014-10-06T21:01'
$ ./cli_multidb --sql_update="UPDATE site_key SET inserted = '$(date +'%FT%T')' WHERE id = 10;" 
$ ./cli_multidb --sql_select="SELECT * FROM site_key WHERE (id > 3 AND id > 5);"
id	inserted	site_key	updated
10	'2014-10-14T18:54:28'	'smtp_password'	'2014-10-06T21:01'


```

LIMITATIONS
===========

SQL parsing is very basic.

COPYRIGHT AND LICENSE
=======================

Copyright (C) 2014, Brian Medley.

This program is free software, you can redistribute it and/or modify it under the terms of the Artistic License version 2.0.