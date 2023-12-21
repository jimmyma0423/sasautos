**********************************************************************************************;
* program name	: autoexec_macros_link_SF;
* purpose		: to save, preview or drop table from Snowflake environment. Users need to change proj and CWID.
* 				  Please read "Example how to use macros".
* author		: Jihaeng Heo
* creation date	: May 30 2023
* parameters	:
*    tb   = name of dataset to save in project folder or Sandbox
**********************************************************************************************;

/*Autoexec macros - change only proj and CWID*/

OPTIONS MPRINT MINOPERATOR MLOGIC SYMBOLGEN;
%global proj CWID db DSN yr projDB adhoc;
%let proj= 2023-XXX-001; /*to change*/
%let CWID= XXXXX; /*to change*/
%let db= PROJECTS_DGOS;
%let DSN= snowflake;
%let yr= %substr(&proj.,1,4);
%let projDB= %sysfunc(compress(%substr(&proj.,1,4)_%substr(&proj.,6,3)_%substr(&proj.,10,3))); /*for Observational or Internal study*/
%let adhoc= %sysfunc(compress(A%substr(&proj.,1,4)%substr(&proj.,6,3)%substr(&proj.,10,3))); /*for Ad hoc study*/

** Libname to SAS server;
libname derive "/byr/warehouse/&yr./&proj.";  

** Libname to database in RWE Store in Snowflake;
libname mkscan snow dsn="&DSN." db="MKTSCAN_CLAIMS" schema="DATA" role="RWDSTORE_MKTSCAN_CLAIMS_R" conopts="token=%readToken()";
libname optum snow dsn="&DSN." db="OPTUM_CLAIMS" schema="DATA" role="RWDSTORE_OPTUM_CLAIMS_R" conopts="token=%readToken()";
%let sf = "DSN=&DSN.;DATABASE=MKTSCAN_CLAIMS;schema=DATA;role=RWDSTORE_PROJECTS_DGOS_RW;token=%readToken()";
%let mssf = "DSN=&DSN.;DATABASE=MKTSCAN_CLAIMS;schema=DATA;role=RWDSTORE_MKTSCAN_CLAIMS_R;token=%readToken()";
%let optumsf = "DSN=&DSN.;DATABASE=OPTUM_CLAIMS;schema=DATA;role=RWDSTORE_OPTUM_CLAIMS_R;token=%readToken()";

** Libname to the project folder in Snowflake, for only Observational study;
libname projDB snow dsn="&DSN." db="&db." schema="&projDB." role="RWDSTORE_PROJECTS_DGOS_RW" conopts="token=%readToken()";
%let psf = "DSN=&DSN.;DATABASE=&db.;schema=&projDB.;role=RWDSTORE_PROJECTS_DGOS_RW;token=%readToken()";

** Code to access personal sandbox;
libname sandbox snow dsn="&DSN." db="SANDBOX" schema="&CWID." role="&CWID." conopts="token=&access_token";
%let sb = "DSN=&DSN.;DATABASE=SANDBOX;schema=&CWID.;role=&CWID.;token=&access_token";


OPTIONS MPRINT MINOPERATOR MLOGIC SYMBOLGEN;
%global resets;
%let resets=0;

* NOTE: Use XXXX_sf macros for observation study, Use XXXX_sb macros for Ad hoc study;
** Preview macros **;
%macro prev_sf;
	%if &resets. eq 0 %then %do;
		%let resets=1;
		proc sql;
			connect to snow (conopts=&sf.);
			select * from connection to snow (
		%end;
	%else %do;
			%let resets=0;
			);
			disconnect from snow;
		quit;
	%end;
%mend prev_sf;

%macro prev_sb;
	%if &resets. eq 0 %then %do;
		%let resets=1;
		proc sql;
			connect to snow (conopts=&sb.);
			select * from connection to snow (
		%end;
	%else %do;
			%let resets=0;
			);
			disconnect from snow;
		quit;
	%end;
%mend prev_sb;


** Save macros **;
%macro save_sf(tb);
	%if &resets. eq 0 %then %do;
		%let resets=1;
		proc sql;
			connect to snow (conopts=&sf.);
			execute (use secondary roles all) by snow;
			execute (create or replace table "&db."."&projDB."."&tb." as
	%end;
	%else %do;
		%let resets=0;
		) by snow;
		disconnect from snow;
		quit;
	%end;
%mend save_sf;

%macro save_sb(tb);
	%if &resets. eq 0 %then %do;
		%let resets=1;
		proc sql;
			connect to snow (conopts=&sb.);
			execute (use secondary roles all) by snow;
			execute (create or replace table "SANDBOX"."&CWID."."&adhoc.&tb." as
	%end;
	%else %do;
		%let resets=0;
		) by snow;
		disconnect from snow;
		quit;
	%end;
%mend save_sb;


** Drop macros **;
%macro drop_sf(tb);
	proc sql;
		connect to snow (conopts=&sf.);
		execute (use secondary roles all) by snow;
		
		execute(DROP TABLE IF EXISTS "&db."."&projDB."."&tb."
			) by snow;
		disconnect from snow;
	quit;
%mend drop_sf;

%macro drop_sb(tb);
	proc sql;
		connect to snow (conopts=&sb.);
		execute (use secondary roles all) by snow;
		
		execute(DROP TABLE IF EXISTS "SANDBOX"."&CWID."."&adhoc.&tb."
			) by snow;
		disconnect from snow;
	quit;
%mend drop_sb;


** Save to sas macros**;
%macro save_sas(tb);
	%if &resets. eq 0 %then %do;
		%let resets=1;
		proc sql;
			connect to snow (conopts=&sf.);
			execute (use secondary roles all) by snow;
			create table &tb. as select * from connection to snow (
	%end;
	%else %do;
		%let resets=0;
		);
		disconnect from snow;
		quit;
	%end;
%mend save_sas;


** Preview using original database macros**;
%macro prev_db;
	%if &resets. eq 0 %then %do;
		%let resets=1;
		proc sql;
			connect to snow (conopts=&sb.);
			execute (use secondary roles all) by snow;
			execute (create or replace table "SANDBOX"."&CWID."."tst" as
	%end;
	%else %do;
		%let resets=0;
		) by snow;
		disconnect from snow;
		quit;

	%prev_sb
		select * from "SANDBOX"."&CWID."."tst"
	%prev_sb;
	
	%drop_sb(tst);
	%end;
%mend prev_db;


/*****Example how to use macros*****/
/* %prev_sf */
/* 	select count(patid) as obs, count(distinct patid) as pts */
/* 	from "&db."."&projDB."."_01_riociguat"  */
/* %prev_sf; */

/* %prev_sb */
/* 	select count(patid) as obs, count(distinct patid) as pts */
/* 	from "SANDBOX"."&CWID."."&adhoc._999_enrol" */
/* %prev_sb; */

/* %save_sf(_01_index) */
/* 	select patid, min(DOS) as index_dt */
/* 	from "&db."."&projDB."."_01_riociguat"  */
/* 	where '01-01-2017' <= DOS and DOS <= '12-31-2021' */
/* 	group by patid */
/* %save_sf; */

/* %save_sb(_01_hemo_tot) */
/* 	select PATID, DOS, hemo, code */
/* 	from "SANDBOX"."&CWID."."&adhoc._01_hemo_proc" */
/* %save_sb;	 */

/* %save_sas(_01_hemo_tot) */
/* 	select PATID, DOS, hemo, code */
/* 	from "SANDBOX"."&CWID."."&adhoc._01_hemo_proc" */
/* %save_sas; */

/* %drop_sb(tst); */

/* %drop_sf(tst); */

/* %prev_db */
/* 	select * */
/* 	from OPTUM_CLAIMS.DATA.LU_NDC */
/* 	where lower(prodnme) like '%jivi%' or lower(gennme) like '%jivi%' or lower(prodnme) like '%jivi%' */
/* %prev_db; */

