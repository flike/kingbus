use ks;
flush logs;

create table stu (
	id int(11) NOT NULL,
	name varchar(40) NOT NULL,
	age int(11) NOT NULL,
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 1
begin;
insert into stu(id,name,age) values(10,"hello1",101);
insert into stu(id,name,age) values(12,"hello2",102);
update stu set age=1101 where id =10;
delete from stu where id=12;
commit;

-- 2
begin;
insert into stu(id,name,age) values(20,"hello1",101);
insert into stu(id,name,age) values(22,"hello2",102);
update stu set age=1101 where id =20;
delete from stu where id=22;
commit;

-- 3
begin;
insert into stu(id,name,age) values(30,"hello1",101);
insert into stu(id,name,age) values(32,"hello2",102);
update stu set age=1101 where id =30;
delete from stu where id=32;
commit;

--4
begin;
insert into stu(id,name,age) values(40,"hello1",101);
insert into stu(id,name,age) values(42,"hello2",102);
update stu set age=1101 where id =40;
delete from stu where id=42;
commit;

--5
begin;
insert into stu(id,name,age) values(50,"hello1",101);
insert into stu(id,name,age) values(52,"hello2",102);
update stu set age=1101 where id =50;
delete from stu where id=52;
commit;

--6
begin;
insert into stu(id,name,age) values(60,"hello1",101);
insert into stu(id,name,age) values(62,"hello2",102);
update stu set age=1101 where id =60;
delete from stu where id=62;
commit;

--7
begin;
insert into stu(id,name,age) values(70,"hello1",101);
insert into stu(id,name,age) values(72,"hello2",102);
update stu set age=1101 where id =70;
delete from stu where id=72;
commit;

--8
begin;
insert into stu(id,name,age) values(80,"hello1",101);
insert into stu(id,name,age) values(82,"hello2",102);
update stu set age=1101 where id =80;
delete from stu where id=82;
commit;

--9
begin;
insert into stu(id,name,age) values(90,"hello1",101);
insert into stu(id,name,age) values(92,"hello2",102);
update stu set age=1101 where id =90;
delete from stu where id=92;
commit;

--10
begin;
insert into stu(id,name,age) values(100,"hello1",101);
insert into stu(id,name,age) values(102,"hello2",102);
update stu set age=1101 where id =100;
delete from stu where id=102;
commit;

flush logs;

-- 11
begin;
insert into stu(id,name,age) values(110,"hello1",101);
insert into stu(id,name,age) values(112,"hello2",102);
update stu set age=1101 where id =110;
delete from stu where id=112;
commit;

-- 12
begin;
insert into stu(id,name,age) values(120,"hello1",101);
insert into stu(id,name,age) values(122,"hello2",102);
update stu set age=1101 where id =120;
delete from stu where id=122;
commit;

-- 13
begin;
insert into stu(id,name,age) values(130,"hello1",101);
insert into stu(id,name,age) values(132,"hello2",102);
update stu set age=1101 where id =130;
delete from stu where id=132;
commit;

--14
begin;
insert into stu(id,name,age) values(140,"hello1",101);
insert into stu(id,name,age) values(142,"hello2",102);
update stu set age=1101 where id =140;
delete from stu where id=142;
commit;

--15
begin;
insert into stu(id,name,age) values(150,"hello1",101);
insert into stu(id,name,age) values(152,"hello2",102);
update stu set age=1101 where id =150;
delete from stu where id=152;
commit;

--16
begin;
insert into stu(id,name,age) values(160,"hello1",101);
insert into stu(id,name,age) values(162,"hello2",102);
update stu set age=1101 where id =160;
delete from stu where id=162;
commit;

--17
begin;
insert into stu(id,name,age) values(170,"hello1",101);
insert into stu(id,name,age) values(172,"hello2",102);
update stu set age=1101 where id =170;
delete from stu where id=172;
commit;

--18
begin;
insert into stu(id,name,age) values(180,"hello1",101);
insert into stu(id,name,age) values(182,"hello2",102);
update stu set age=1101 where id =180;
delete from stu where id=182;
commit;

--19
begin;
insert into stu(id,name,age) values(190,"hello1",101);
insert into stu(id,name,age) values(192,"hello2",102);
update stu set age=1101 where id =190;
delete from stu where id=192;
commit;

--20
begin;
insert into stu(id,name,age) values(200,"hello1",101);
insert into stu(id,name,age) values(202,"hello2",102);
update stu set age=1101 where id =200;
delete from stu where id=202;
commit;

flush logs;

-- 21
begin;
insert into stu(id,name,age) values(210,"hello1",101);
insert into stu(id,name,age) values(212,"hello2",102);
update stu set age=1101 where id =210;
delete from stu where id=212;
commit;

-- 22
begin;
insert into stu(id,name,age) values(220,"hello1",101);
insert into stu(id,name,age) values(222,"hello2",102);
update stu set age=1101 where id =220;
delete from stu where id=222;
commit;

-- 23
begin;
insert into stu(id,name,age) values(230,"hello1",101);
insert into stu(id,name,age) values(232,"hello2",102);
update stu set age=1101 where id =230;
delete from stu where id=232;
commit;

--24
begin;
insert into stu(id,name,age) values(240,"hello1",101);
insert into stu(id,name,age) values(242,"hello2",102);
update stu set age=1101 where id =240;
delete from stu where id=242;
commit;

--25
begin;
insert into stu(id,name,age) values(250,"hello1",101);
insert into stu(id,name,age) values(252,"hello2",102);
update stu set age=1101 where id =250;
delete from stu where id=252;
commit;

--26
begin;
insert into stu(id,name,age) values(260,"hello1",101);
insert into stu(id,name,age) values(262,"hello2",102);
update stu set age=1101 where id =260;
delete from stu where id=262;
commit;

--27
begin;
insert into stu(id,name,age) values(270,"hello1",101);
insert into stu(id,name,age) values(272,"hello2",102);
update stu set age=1101 where id =270;
delete from stu where id=272;
commit;

--28
begin;
insert into stu(id,name,age) values(280,"hello1",101);
insert into stu(id,name,age) values(282,"hello2",102);
update stu set age=1101 where id =280;
delete from stu where id=282;
commit;

--29
begin;
insert into stu(id,name,age) values(290,"hello1",101);
insert into stu(id,name,age) values(292,"hello2",102);
update stu set age=1101 where id =290;
delete from stu where id=292;
commit;

--30
begin;
insert into stu(id,name,age) values(300,"hello1",101);
insert into stu(id,name,age) values(302,"hello2",102);
update stu set age=1101 where id =300;
delete from stu where id=302;
commit;
