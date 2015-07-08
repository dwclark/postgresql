begin transaction;

drop table if exists items;
drop table if exists all_types;

create table items (
       id int,
       description varchar(200)
);

create table all_types (
       id serial,
       my_boolean boolean,
       my_smallint smallint,
       my_int int,
       my_long bigint,
       my_decimal decimal(19,4),
       my_numeric numeric(10,2),
       my_real real,
       my_double double precision,
       my_money money,
       my_varchar varchar(250),
       my_text text,
       my_bytes bytea,
       my_time time without time zone,
       my_time_tz time with time zone,
       my_date date,
       my_timestamp timestamp without time zone,
       my_timestamp_tz timestamp with time zone
);

insert into items values (1, 'one');
insert into items values (2, 'two');

insert into all_types (my_boolean, my_smallint, my_int, my_long, my_decimal,
       my_numeric, my_real, my_double, my_money, my_varchar, my_text,
       my_bytes, my_time, my_time_tz, my_date, my_timestamp, my_timestamp_tz)
values (true, 100, 1000, 10000, 175.1234, 1234567.89, 700.5, 700000.1, 250.67, 'some varchar', 'some text',
       E'\\xDEADBEEF', '22:16:52.048607', '22:16:52.048607-05', '2010-12-25',
       '2015-07-07 22:17:38.475474', '2015-07-07 22:17:38.475474-05');

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO noauth;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO clearauth;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO md5auth;

commit;
