-- 
-- @created 2009-01-27 14:00:00
-- @modified 2013-06-24 17:00:00
-- @tags ddl schema_topology

-- Drop table if exists

--Drop table 
DROP table if exists co_compr_part_1 cascade;
DROP table if exists exch_part2_compr_1 cascade;

--
-- Create table
--

CREATE TABLE co_compr_part_1
        (id SERIAL,a1 int,a2 char(5),a3 numeric,a4 boolean DEFAULT false ,a5 char DEFAULT 'd',a6 text,a7 timestamp,a8 character varying(705),a9 bigint,a10 date,a11 varchar(600),a12 text,a13 decimal,a14 real,a15 bigint,a16 int4 ,a17 bytea,a18 timestamp with time zone,a19 timetz,a20 path,a21 box,a22 macaddr,a23 interval,a24 character varying(800),a25 lseg,a26 point,a27 double precision,a28 circle,a29 int4,a30 numeric(8),a31 polygon,a32 date,a33 real,a34 money,a35 cidr,a36 inet,
        a37 time,a38 text,a39 bit,a40 bit varying(5),a41 smallint,a42 int )  WITH (appendonly=true, orientation=column) distributed randomly
 Partition by list(a2) Subpartition by range(a1) subpartition template (default subpartition df_sp, start(1)  end(5000) every(1000),
 COLUMN a2  ENCODING (compresstype=quicklz,compresslevel=1,blocksize=32768),
 COLUMN a1 encoding (compresstype = rle_type,compresslevel=1),
 COLUMN a3 encoding (compresstype = rle_type,compresslevel=3),
 COLUMN a5 ENCODING (compresstype=zlib,compresslevel=3, blocksize=32768),
 DEFAULT COLUMN ENCODING (compresstype=quicklz,compresslevel=1,blocksize=32768)) (partition p1 values('F'), partition p2 values ('M'));


-- 
-- Create Indexes # commenting create indexes due to MPP-14747 Restore fails with duplicate index errors for subpartition level indexes
--
-- CREATE INDEX co_compr_part_1_idx_bitmap ON co_compr_part_1 USING bitmap (a1);

-- CREATE INDEX co_compr_part_1_idx_btree ON co_compr_part_1(a9);

--
--Insert data to the table
--
INSERT INTO co_compr_part_1(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42) values(generate_series(1,20),'M',2011,'t','a','This is news of today: Deadlock between Republicans and Democrats over how best to reduce the U.S. deficit, and over what period, has blocked an agreement to allow the raising of the $14.3 trillion debt ceiling','2001-12-24 02:26:11','U.S. House of Representatives Speaker John Boehner, the top Republican in Congress who has put forward a deficit reduction plan to be voted on later on Thursday said he had no control over whether his bill would avert a credit downgrade.',generate_series(2490,2505),'2011-10-11','The Republican-controlled House is tentatively scheduled to vote on Boehner proposal this afternoon at around 6 p.m. EDT (2200 GMT). The main Republican vote counter in the House, Kevin McCarthy, would not say if there were enough votes to pass the bill.','WASHINGTON:House Speaker John Boehner says his plan mixing spending cuts in exchange for raising the nations $14.3 trillion debt limit is not perfect but is as large a step that a divided government can take that is doable and signable by President Barack Obama.The Ohio Republican says the measure is an honest and sincere attempt at compromise and was negotiated with Democrats last weekend and that passing it would end the ongoing debt crisis. The plan blends $900 billion-plus in spending cuts with a companion increase in the nations borrowing cap.','1234.56',323453,generate_series(3452,3462),7845,'0011','2005-07-16 01:51:15+1359','2001-12-13 01:51:15','((1,2),(0,3),(2,1))','((2,3)(4,5))','08:00:2b:01:02:03','1-2','Republicans had been working throughout the day Thursday to lock down support for their plan to raise the nations debt ceiling, even as Senate Democrats vowed to swiftly kill it if passed.','((2,3)(4,5))','(6,7)',11.222,'((4,5),7)',32,3214,'(1,0,2,3)','2010-02-21',43564,'$1,000.00','192.168.1','126.1.3.4','12:30:45','Johnson & Johnsons McNeil Consumer Healthcare announced the voluntary dosage reduction today. Labels will carry new dosing instructions this fall.The company says it will cut the maximum dosage of Regular Strength Tylenol and other acetaminophen-containing products in 2012.Acetaminophen is safe when used as directed, says Edwin Kuffner, MD, McNeil vice president of over-the-counter medical affairs. But, when too much is taken, it can cause liver damage.The action is intended to cut the risk of such accidental overdoses, the company says in a news release.','1','0',12,23);

INSERT INTO co_compr_part_1(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42) values(generate_series(500,510),'F',2010,'f','b','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child','2001-12-25 02:22:11','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child',generate_series(2500,2516),'2011-10-12','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child The type integer is the usual choice, as it offers the best balance between range, storage size, and performance The type integer is the usual choice, as it offers the best balance between range, storage size, and performanceThe type integer is the usual choice, as it offers the best balance between range, storage size, and performanceThe type integer is the usual choice, as it offers the best balance between range, storage size, and performanceThe type integer ','1134.26',311353,generate_series(3982,3992),7885,'0101','2002-02-12 01:31:14+1344','2003-11-14 01:41:15','((1,1),(0,1),(1,1))','((2,1)(1,5))','08:00:2b:01:01:03','1-3','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child The types smallint, integer, and bigint store whole numbers, that is, numbers without fractional components, of various ranges. The types smallint, integer, and bigint store whole numbers, that is, numbers without fractional components, of various ranges. Attempts to store values outside of the allowed range will result in an errorThe types smallint, integer, and bigint store whole numbers, that is, numbers without fractional components, of various ranges.','((6,5)(4,2))','(3,6)',12.233,'((5,4),2)',12,3114,'(1,1,0,3)','2010-03-21',43164,'$1,500.00','192.167.2','126.1.1.1','10:30:55','Parents and other family members are always welcome at Stratford. After the first two weeks ofschool','0','1',33,44);



--Alter table Add Partition 
ALTER TABLE co_compr_part_1 add partition new_p values('C') ;

ALTER TABLE co_compr_part_1 add default partition df_p ;

---- Create the tables to do the exchange partition--

CREATE TABLE exch_part2_compr_1 (id SERIAL,a1 int,a2 char(5),a3 numeric,a4 boolean DEFAULT false ,a5 char DEFAULT 'd',a6 text,a7 timestamp,a8 character varying(705),a9 bigint,a10 date,a11 varchar(600),a12 text,a13 decimal,a14 real,a15 bigint,a16 int4 ,a17 bytea,a18 timestamp with time zone,a19 timetz,a20 path,a21 box,a22 macaddr,a23 interval,a24 character varying(800),a25 lseg,a26 point,a27 double precision,a28 circle,a29 int4,a30 numeric(8),a31 polygon,a32 date,a33 real,a34 money,a35 cidr,a36 inet,a37 time,a38 text,a39 bit,a40 bit varying(5),a41 smallint,a42 int) WITH (appendonly=true, orientation=column) distributed randomly;

INSERT INTO  exch_part2_compr_1(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42) select a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42 from co_compr_part_1 where  a1=10;


--Alter table exchange partition
ALTER TABLE co_compr_part_1 alter partition p2 exchange partition FOR (RANK(1)) with table exch_part2_compr_1 ;

--Alter table split partition
ALTER TABLE co_compr_part_1 alter partition p1 split partition FOR (RANK(1)) at(505) into (partition splita_1,partition splitb_1) ;

--Alter table Drop partition
ALTER TABLE co_compr_part_1 drop partition new_p;
ALTER TABLE co_compr_part_1 drop default partition;

INSERT INTO co_compr_part_1(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42) select a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42 from co_compr_part_1 where  a1=500;

SELECT count(*) from co_compr_part_1;


-- Partition by range

--Drop table if exists

DROP table if exists co_compr_part_2 cascade;
DROP table if exists  exch_part2_compr_2 cascade;



--
-- Create table
--
CREATE TABLE co_compr_part_2
        (id SERIAL,a1 int,a2 char(5),a3 numeric,a4 boolean DEFAULT false ,a5 char DEFAULT 'd',a6 text,a7 timestamp,a8 character varying(705),a9 bigint,a10 date,a11 varchar(600),a12 text,a13 decimal,a14 real,a15 bigint,a16 int4 ,a17 bytea,a18 timestamp with time zone,a19 timetz,a20 path,a21 box,a22 macaddr,a23 interval,a24 character varying(800),a25 lseg,a26 point,a27 double precision,a28 circle,a29 int4,a30 numeric(8),a31 polygon,a32 date,a33 real,a34 money,a35 cidr,a36 inet,a37 time,a38 text,a39 bit,a40 bit varying(5),a41 smallint,a42 int )  WITH (appendonly=true, orientation=column) distributed randomly
 Partition by range(a1) Subpartition by list(a2) subpartition template ( default subpartition df_sp, subpartition sp1 values('M') , subpartition sp2 values('F')  ,
 COLUMN a2  ENCODING (compresstype=zlib,compresslevel=1,blocksize=32768),
 COLUMN a1 encoding (compresstype = rle_type,compresslevel=2),
 COLUMN a5 ENCODING (compresstype=quicklz,compresslevel=1, blocksize=32768),
 DEFAULT COLUMN ENCODING (compresstype=rle_type,compresslevel=4,blocksize=32768)) (start(1) end(5000) every(1000));

-- 
-- Create Indexes
--
-- CREATE INDEX co_compr_part_2_idx_bitmap ON co_compr_part_2 USING bitmap (a1);

-- CREATE INDEX co_compr_part_2_idx_btree ON co_compr_part_2(a9);


--
-- Insert data to the table
--
 INSERT INTO co_compr_part_2(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42) values(generate_series(1,20),'M',2011,'t','a','This is news of today: Deadlock between Republicans and Democrats over how best to reduce the U.S. deficit, and over what period, has blocked an agreement to allow the raising of the $14.3 trillion debt ceiling','2001-12-24 02:26:11','U.S. House of Representatives Speaker John Boehner, the top Republican in Congress who has put forward a deficit reduction plan to be voted on later on Thursday said he had no control over whether his bill would avert a credit downgrade.',generate_series(2490,2505),'2011-10-11','The Republican-controlled House is tentatively scheduled to vote on Boehner proposal this afternoon at around 6 p.m. EDT (2200 GMT). The main Republican vote counter in the House, Kevin McCarthy, would not say if there were enough votes to pass the bill.','WASHINGTON:House Speaker John Boehner says his plan mixing spending cuts in exchange for raising the nations $14.3 trillion debt limit is not perfect but is as large a step that a divided government can take that is doable and signable by President Barack Obama.The Ohio Republican says the measure is an honest and sincere attempt at compromise and was negotiated with Democrats last weekend and that passing it would end the ongoing debt crisis. The plan blends $900 billion-plus in spending cuts with a companion increase in the nations borrowing cap.','1234.56',323453,generate_series(3452,3462),7845,'0011','2005-07-16 01:51:15+1359','2001-12-13 01:51:15','((1,2),(0,3),(2,1))','((2,3)(4,5))','08:00:2b:01:02:03','1-2','Republicans had been working throughout the day Thursday to lock down support for their plan to raise the nations debt ceiling, even as Senate Democrats vowed to swiftly kill it if passed.','((2,3)(4,5))','(6,7)',11.222,'((4,5),7)',32,3214,'(1,0,2,3)','2010-02-21',43564,'$1,000.00','192.168.1','126.1.3.4','12:30:45','Johnson & Johnsons McNeil Consumer Healthcare announced the voluntary dosage reduction today. Labels will carry new dosing instructions this fall.The company says it will cut the maximum dosage of Regular Strength Tylenol and other acetaminophen-containing products in 2012.Acetaminophen is safe when used as directed, says Edwin Kuffner, MD, McNeil vice president of over-the-counter medical affairs. But, when too much is taken, it can cause liver damage.The action is intended to cut the risk of such accidental overdoses, the company says in a news release.','1','0',12,23);


 INSERT INTO co_compr_part_2(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42) values(generate_series(500,510),'F',2010,'f','b','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child','2001-12-25 02:22:11','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child',generate_series(2500,2516),'2011-10-12','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child The type integer is the usual choice, as it offers the best balance between range, storage size, and performance The type integer is the usual choice, as it offers the best balance between range, storage size, and performanceThe type integer is the usual choice, as it offers the best balance between range, storage size, and performanceThe type integer is the usual choice, as it offers the best balance between range, storage size, and performanceThe type integer ','1134.26',311353,generate_series(3982,3992),7885,'0101','2002-02-12 01:31:14+1344','2003-11-14 01:41:15','((1,1),(0,1),(1,1))','((2,1)(1,5))','08:00:2b:01:01:03','1-3','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child The types smallint, integer, and bigint store whole numbers, that is, numbers without fractional components, of various ranges. The types smallint, integer, and bigint store whole numbers, that is, numbers without fractional components, of various ranges. Attempts to store values outside of the allowed range will result in an errorThe types smallint, integer, and bigint store whole numbers, that is, numbers without fractional components, of various ranges.','((6,5)(4,2))','(3,6)',12.233,'((5,4),2)',12,3114,'(1,1,0,3)','2010-03-21',43164,'$1,500.00','192.167.2','126.1.1.1','10:30:55','Parents and other family members are always welcome at Stratford. After the first two weeks ofschool','0','1',33,44);



--Select from pg_attribute_encoding to see the table entry 
select parencattnum, parencattoptions from pg_partition_encoding e, pg_partition p, pg_class c  where c.relname = 'co_compr_part_2' and c.oid = p.parrelid and p.oid = e.parencoid order by parencattnum limit 3;


--Alter table Add Partition 
alter table co_compr_part_2 add partition new_p start(5050) end (5061);

--Validation with psql utility 

alter table co_compr_part_2 add default partition df_p ;



-- Insert data 
Insert into co_compr_part_2(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42) values(generate_series(5051,5060),'C',2011,'t','a','dfjjjjjj','2001-12-24 02:26:11','hghgh',333,'2011-10-11','Tddd','sss','1234.56',323453,4454,7845,'0011','2005-07-16 01:51:15+1359','2001-12-13 01:51:15','((1,2),(0,3),(2,1))','((2,3)(4,5))','08:00:2b:01:02:03','1-2','dfdf','((2,3)(4,5))','(6,7)',11.222,'((4,5),7)',32,3214,'(1,0,2,3)','2010-02-21',43564,'$1,000.00','192.168.1','126.1.3.4','12:30:45','ggg','1','0',12,23) ;


 CREATE TABLE exch_part2_compr_2 (id SERIAL,a1 int,a2 char(5),a3 numeric,a4 boolean DEFAULT false ,a5 char DEFAULT 'd',a6 text,a7 timestamp,a8 character varying(705),a9 bigint,a10 date,a11 varchar(600),a12 text,a13 decimal,a14 real,a15 bigint,a16 int4 ,a17 bytea,a18 timestamp with time zone,a19 timetz,a20 path,a21 box,a22 macaddr,a23 interval,a24 character varying(800),a25 lseg,a26 point,a27 double precision,a28 circle,a29 int4,a30 numeric(8),a31 polygon,a32 date,a33 real,a34 money,a35 cidr,a36 inet,a37 time,a38 text,a39 bit,a40 bit varying(5),a41 smallint,a42 int) WITH (appendonly=true, orientation=column) distributed randomly;


Insert into exch_part2_compr_2(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42) select a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42 from co_compr_part_2 where  a1=10;

Alter table co_compr_part_2 alter partition FOR (RANK(1)) exchange partition sp1 with table exch_part2_compr_2 ;


--Alter table Drop Partition 
alter table co_compr_part_2 drop partition new_p;

-- Drop the default partition 
alter table co_compr_part_2 drop default partition;


--Alter table alter type of a column 
Alter table co_compr_part_2 Alter column a3 TYPE int4;
--Insert data to the table, select count(*)
Insert into co_compr_part_2(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42) select a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42 from co_compr_part_2 where id =10;
Select count(*) from co_compr_part_2;

--Alter table drop a column 
Alter table co_compr_part_2 Drop column a12;
Insert into co_compr_part_2(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42) select a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42 from co_compr_part_2 where id =10;
Select count(*) from co_compr_part_2;

--Alter table rename a column 
Alter table co_compr_part_2 Rename column a13 TO after_rename_a13;
--Insert data to the table, select count(*)
Insert into co_compr_part_2(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,after_rename_a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42) select a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,after_rename_a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42 from co_compr_part_2 where id =10;
Select count(*) from co_compr_part_2;


