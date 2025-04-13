/*
==========================================================================================================
Create Role: making role every layer for different user (data engineers, data analysis,business analysis)
==========================================================================================================
Script Purpose:
    This Create Role performs the permissions access anything analysis process to 
    the 'bronze' schema tables , 'silver' schema tables ,and 'gold' schema tables. 
    and restrinting access from public
	Actions Performed:
		- CREATE ROLE
		- GRANT CONNECT ON DATABASE
		- GRANT USAGE ON SCHEMA
		- GRANT SQL COMMANDS ON SCHEMA		
		- ALTER  DEFAULT PRIVILEGES IN SCHEMA
		- REVOKE ALL ON SCHEMA				
Parameters:
    None. 
	  This create role does not accept any parameters or return any values.

Note : this query only work for postgreSQL engine
===========================================================================================================
*/

/*===================================CREATE ROLE============================================================*/

CREATE ROLE user_engineers WITH LOGIN PASSWORD 'password_engineers';
CREATE ROLE user_analysis WITH LOGIN PASSWORD 'password_analysis';
CREATE ROLE user_business WITH LOGIN PASSWORD 'password_business';

/*===================================CONNECT TO DATABASE============================================================*/
GRANT CONNECT ON DATABASE datawarehouse TO user_engineers,user_analysis,user_business;

/*===================================GIVING PERMISSION ON BRONZE LAYER============================================================*/
GRANT USAGE ON SCHEMA bronze TO user_engineers;
GRANT SELECT ,INSERT ,UPDATE, DELETE ON ALL TABLES IN SCHEMA bronze TO user_engineers;
ALTER  DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ,INSERT , UPDATE , DELETE ON TABLES TO user_engineers;

/*===================================GIVING PERMISSION ON SILVER LAYER============================================================*/
GRANT USAGE ON SCHEMA silver TO user_engineers,user_analysis ;
GRANT SELECT ,INSERT ,UPDATE, DELETE ON ALL TABLES IN SCHEMA silver TO user_engineers,user_analysis;
GRANT CREATE ON SCHEMA silver TO user_engineers, user_analysis;
ALTER  DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ,INSERT , UPDATE , DELETE ON TABLES TO user_engineers,user_analysis;

/*===================================GIVING PERMISSION ON GOLD LAYER============================================================*/
GRANT USAGE ON SCHEMA gold TO user_analysis,user_business ;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO user_analysis,user_business;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO user_analysis,user_business;

/*===============RESTRICT ACCESS==================================*/
REVOKE ALL ON SCHEMA bronze FROM public;
REVOKE ALL ON SCHEMA silver FROM public;
REVOKE ALL ON SCHEMA gold FROM public;