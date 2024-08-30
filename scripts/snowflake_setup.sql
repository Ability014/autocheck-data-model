--------- Setting up Airbyte Account on snowflake ------------------
-- set variables (these need to be uppercase)
set my_role = 'INSERT ROLE NAME';
set my_username = 'INSERT USERNAME';
set my_warehouse = 'INSERT WAREHOUSE';
set my_database = 'INSERT DATABASE';
--  For raw data sources
set schema1 = 'RAW';
-- For dbt models
set schema2 = 'MART';
set schema3 = 'INTERMEDIATE';
set schema4 = 'STAGING';

-- For web crawler data
set schema5 = 'CAR_LISTING';

-- set user password
set airbyte_password = 'insert pass';

begin;


-- create role
use role securityadmin;
create role if not exists identifier($my_role);
grant role identifier($my_role) to role SYSADMIN;

-- create user
create user if not exists identifier($my_username)
password = $my_password
default_role = $my_role
default_warehouse = $my_warehouse;

grant role identifier($my_role) to user identifier($my_username);


-- change role to sysadmin for warehouse / database steps
use role sysadmin;

-- create warehouse
create warehouse if not exists identifier($my_warehouse)
warehouse_size = xsmall
warehouse_type = standard
auto_suspend = 60
auto_resume = true
initially_suspended = true;


-- create database
create database if not exists identifier($my_database);

-- grant warehouse access
grant USAGE
on warehouse identifier($my_warehouse)
to role identifier($my_role);

-- grant database access
grant OWNERSHIP
on database identifier($my_database)
to role identifier($my_role);

commit;

begin;

USE DATABASE identifier($my_database);

-- create schema for Autocheck data
CREATE SCHEMA IF NOT EXISTS identifier($schema1);
CREATE SCHEMA IF NOT EXISTS identifier($schema2);
CREATE SCHEMA IF NOT EXISTS identifier($schema3);
CREATE SCHEMA IF NOT EXISTS identifier($schema4);
CREATE SCHEMA IF NOT EXISTS identifier($schema5);

commit;

begin;

USE DATABASE identifier($my_database);
-- grant schema access
grant OWNERSHIP
on schema identifier($schema1)
to role identifier($my_role);

grant OWNERSHIP
on schema identifier($schema2)
to role identifier($my_role);

grant OWNERSHIP
on schema identifier($schema3)
to role identifier($my_role);

grant OWNERSHIP
on schema identifier($schema4)
to role identifier($my_role);

grant OWNERSHIP
on schema identifier($schema5)
to role identifier($my_role);

commit;
