# TiDB-Binlog

Forked From [https://github.com/pingcap/tidb-binlog](https://github.com/pingcap/tidb-binlog)

Add sql/flashback syncer for sql replay and flashback.

If used to flashback, you will need a script to generate a lines-reversed  file based on `dest-sql-file`, Because:


```sql
# sqls in dest-sql-file for sql syncer:
Start Transaction;
insert into test(id, name) values (1, 'a');
Commit;
Start Transaction;
insert into test(id, name) values (2, 'b');
update test set id = 1, name='A' where id = 1 and name = 'a';
Commit;

# sqls in dest-sql-file for flashback syncer:
Commit;
delete from test where id = 1 and name = 'a';
Start Transaction;
Commit;
delete from test where id = 2 and name = 'b';
update test set id = 1, name='a' where id = 1 and name = 'A';
Start Transaction;
```

Reverse lines order, and you will get a real flashback sql file, python seems good for this ops. 
