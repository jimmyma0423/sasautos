**********************************************************************************************;
* program name	: continuously_enrolled_snowflake;
* purpose		: To find out the starting date/end date of continuesly enroll time period 
*                 which contains the index date in Snowflake using Optum claims or Markscan database. 
* author		: Jihaeng Heo
* creation date	: August 27 2023
* parameters	:
* 	 sf_sb      = input 'sf' for Project folder or 'sb' for SANDBOX
*    db		    = input '0' for Optum claims or '1' for MarketScan
*    input_tb   = name of input dataset that contains the distinct patid and index date
*    output_tb  = name of output dataset which contains the patid, indexdate, enroll_start and enroll_end variable. 
*                 enroll_start/end variables give the start/end date of the continuously enrolled period that contains the index date
*    index_date = name of index date variable
*    gap        = number of days allowed as gap in the determination of continuesly enrollment. If continous enrollment without any gap is needed, input 0.
*    rx         = For only MarketScan, choose Cohort Drug Indicator. If want to select patients with both medical and drug benefit, input 1, otherwise, leave it.

* ouput variables:
*    enroll_start     = Start date of continuous enrollment period
*    enroll_end       = End date of continuous enrollment period
*    enroll_start_sub = Start date of part of continuous enrollment period by Commercial or Medicare insurance
*    enroll_end_sub   = End date of part of continuous enrollment period by Commercial or Medicare insurance
* 	 insurance_typ    = flag of Commercial (COM) or Medicare (MCR) within enroll_start_sub and enroll_end_sub
**********************************************************************************************;

%macro cont_enrol (sf_sb, db, input_tb, output_tb, index_date, gap, rx);
	proc sql;
    connect to snow (conopts=&&&sf_sb.);
      execute (use secondary roles all) by snow;
      execute (create or replace table &output_tb. as
		%if &db.=0 %then %do;
			%let database= %str(OPTUM_CLAIMS.DATA_202212.DOD_MBR_ENROLL_R);
			%let enroll_start= eligeff;
			%let enroll_end= eligend;
			%let ins_typ= BUS;
		%end;
		%else %if &db.=1 %then %do;
			%let database= %str(MKTSCAN_CLAIMS.DATA_202212.DETAIL_ENROLLMENT);
			%let enroll_start= dtstart;
			%let enroll_end= dtend;		
			%let ins_typ= insurance_type;
		%end;
		
		with _setup as (select distinct patid, &enroll_start., &enroll_end.
						, lag(&enroll_end.,1) over (partition by patid order by &enroll_start., &enroll_end.) as lag_&enroll_end.
						, lead(&enroll_start.,1) over (partition by patid order by &enroll_start., &enroll_end.) as lead_&enroll_start.
						, row_number () over (partition by patid order by &enroll_start., &enroll_end.) as n_row
					from &database.
					where patid in (select patid from &input_tb.) %if &db.=1 and &rx.=1 %then %do; and RX='1' %end;),
		
		_setup_typ as (select distinct *
					, case when n_row=1 or datediff(day, lag_&enroll_end., &enroll_start.) > (1+&gap.) then 'fst_dt_cont' end as start_typ
					, case when datediff(day, &enroll_end., lead_&enroll_start.) > (1+&gap.) or lead_&enroll_start. is null then 'lst_dt_cont' end as end_typ
					from _setup),
		
		_setup_fn as (select distinct a.patid, c.&index_date., a.&enroll_start. as enroll_start, b.&enroll_end. as enroll_end
					from (select distinct patid, &enroll_start., row_number () over (partition by patid order by &enroll_start.) as enrol_seq from _setup_typ where start_typ='fst_dt_cont') as a 
					inner join (select distinct patid, &enroll_end., row_number () over (partition by patid order by &enroll_end.) as enrol_seq from _setup_typ where end_typ='lst_dt_cont') as b
						on a.patid=b.patid and a.enrol_seq=b.enrol_seq	
					inner join &input_tb. as c 
						on a.patid=c.patid and c.&index_date. between a.&enroll_start. and b.&enroll_end.),

		_sub_period as (select distinct a.*
						%if &db.=0 %then %do; , b.&ins_typ. as insurance_typ %end;
						%if &db.=1 %then %do; , case when b.&ins_typ.=0 then 'COM' when b.&ins_typ.=1 then 'MCR' end as insurance_typ %end;
						, b.&enroll_start. as enroll_start_sub, b.&enroll_end. as enroll_end_sub
					from _setup_fn as a left join &database. as b
					on a.patid=b.patid and (b.&enroll_start. between a.enroll_start and a.enroll_end and b.&enroll_end. between a.enroll_start and a.enroll_end))
		
		select patid, &index_date., enroll_start, enroll_end, insurance_typ
			, enroll_start_sub
			, case when dateadd(day, -1, lead(enroll_start_sub,1) over (partition by patid order by enroll_start_sub)) is not null then 
				dateadd(day, -1, lead(enroll_start_sub,1) over (partition by patid order by enroll_start_sub))
				else enroll_end 			
				end as enroll_end_sub
		from (select *, lag(insurance_typ,1) over (partition by patid order by enroll_start_sub) as lead_insurance_typ
			from _sub_period)
		where lead_insurance_typ <> insurance_typ or lead_insurance_typ is null	
		) by snow;
		disconnect from snow;
	quit;
%mend cont_enrol;
/* %cont_enrol (sf_sb=  /*'sf' for Project folder or 'sb' for SANDBOX */
/* 			, db=    /*'0' for Optum claims or '1' for MarketScan */
/* 			, input_tb=  */
/* 			, output_tb=  */
/* 			, index_date=  */
/* 			, gap=  */
/*			, rx= );  */

/* * Example Optum*;  */
/* %cont_enrol (sf_sb= sb */
/* 			, db= 0 */
/* 			, input_tb= "SANDBOX"."GOOTN"."example_tb"  */
/* 			, output_tb= "SANDBOX"."GOOTN"."_999_enrol_optum"  */
/* 			, index_date= index_dt */
/* 			, gap= 0 */
/* 			, rx= ); */

/* * Example MS*; */
/* %cont_enrol (sf_sb= sb */
/* 			, db= 1 */
/* 			, input_tb= "SANDBOX"."GOOTN"."example_tb"  */
/* 			, output_tb= "SANDBOX"."GOOTN"."_999_enrol"  */
/* 			, index_date= index_dt */
/* 			, gap= 0 */
/* 			, rx= 1); */
