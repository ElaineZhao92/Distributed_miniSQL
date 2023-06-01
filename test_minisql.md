# Final

```mysql
# Client 1
create table stu (id int, name char(10), score float, primary key(id)) ;
create table a ( id int, primary key (id)) ;
create table b ( id int, primary key (id)) ;
create table c ( id int, primary key (id)) ;
```

```mysql
# Client 1
insert into stu values (1, 'zq', 100.0) ;
insert into stu values (2, 'sl', 69.0) ;
insert into stu values (3, 'ww', 30.0) ;
insert into b values (2) ;
insert into a values (3) ;
# Region挂一个
```

```mysql
# Client 2
select * from stu ;
select name, score from stu where score >= 80 ;
insert into b values (22) ;
insert into a values (34) ;
```

```mysql
# Client 2
delete from stu where score > 90 ;
delete from a where id > 2 ;
```

```mysql
# Client 2
drop table a ;
```

