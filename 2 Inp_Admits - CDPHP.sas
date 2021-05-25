options compress=yes obs=max;
libname out "\\vm_fs01\Department\Finance\HCE Projects\241_Readmission Modeling\CDPHP";
libname input1 "D:\Project_Files\3_Financial_Value_Rpt\Data\201901" access=readonly;
libname input2 "D:\Project_Files\3_Financial_Value_Rpt\Data\201901\cdphp_staging" access=readonly;
/*libname input6 "D:\Project_Files\3_Financial_Value_Rpt\Data\201901\anthem_staging" access=readonly;*/
/*libname input7 "D:\Project_Files\3_Financial_Value_Rpt\Data\201901\hpsm_staging" access=readonly;*/
/*libname input8 "D:\Project_Files\3_Financial_Value_Rpt\Data\201901\bcbsma_staging" access=readonly;*/

/**********CDPHP *****************/
proc sql;
	create table HP2_inp_admits as
        select 2 as Healthplan_Id_Ref, inp.mbr_uid, mbr.Patientid as PatientID_ref,
			inp.Frst_Paid_Date, inp.Provider_Name,
			inp.Conf_Start_Date, inp.Conf_End_Date, PUT(inp.Frst_Paid_Date, yymmn6.) as Paid_Month,
			DRG_code, DRG_code as Pay_DRG, 
			case when DRG_Code between '001' and '999' then DRG_Code
				else 'Ukn' end as DRG,
			PUT(inp.Conf_End_Date, yymmn6.) as Discharged_Month,
			inp.ip_Readmit_30, 
			case when mbr.engaged_status is not null then mbr.engaged_status else 'O' end as hcta_engaged_status,
			case when mbr.predicted_intensity is not null then 4-input(left(mbr.predicted_intensity,1),1.) else 0 end as hcta_intensity,
			case when mbr.risk_adj_factor_partA is not null then mbr.risk_adj_factor_partA else 0 end as mmr_risk,
			case when mbr.CMS_ESRD= 'Y' then 1 else 0 end as ESRD,
			case when mbr.CMS_HOSPICE= 'Y' then 1 else 0 end as HOSPICE,
			PUT(inp.Conf_Start_Date, yymmn6.) as Incurred_Month,
			case when mbr1.engaged_status is not null then mbr1.engaged_status else 'O' end as start_engaged_status

		from input2.inp_admits as inp
		left join input1.consolidate_member_detail as mbr
			on inp.mbr_uid = mbr.mbr_uid
			and PUT(inp.Conf_End_Date, yymmn6.) = mbr.Incurred_month	
			and mbr.default_view = 'Y'	and mbr.Healthplan_id_Ref = 2
		left join input1.consolidate_member_detail as mbr1			
	 		on inp.mbr_uid = mbr1.mbr_uid		
			and PUT(inp.Conf_Start_Date, yymmn6.) = mbr1.Incurred_month	
			and mbr1.default_view = 'Y'	and mbr1.Healthplan_id_Ref = 2
		 where /* CDPHP*/
			year(inp.Conf_Start_Date) >= 2016
			and inp.mbr_uid is not null
			and inp.Admit_Count =1
			and inp.Service_Category NOT IN ('Invalid IP Confinement', 'Sub-Acute IP')
		order by mbr_uid, Conf_Start_Date desc, Conf_End_Date desc;	
quit;
/*******Reconcile with HCTA ****************************************/
proc sql;	
	create table x as
	select Discharged_Month, Incurred_Month, hcta_engaged_status as end_engaged_status, start_engaged_status, count(*) as AdmitCnt
	from HP2_inp_admits
	group by 1,2,3,4
;quit;	

/***** Get CCS_DX_Category ******/
proc sql;
create table hce_inp_admits as 
	select a.*, b.CCS_DX_Category_Rollup as ccs
	from HP2_inp_admits as a
	left join (select * from input1.fvr_claims_detail 
			where service_type = 'Inpatient Admit' and Admit_Count = 1) as b
	ON A.Healthplan_Id_Ref=B.Healthplan_Id_Ref 
	AND A.PatientId_ref=B.PatientId 
	and a.Conf_Start_Date = b.DateOfServiceFrom
	and	a.Conf_End_Date = b.DateOfServiceTo
	order by a.Healthplan_Id_Ref, a.mbr_uid, a.Conf_Start_Date desc, a.Conf_End_Date desc;	
;quit;
/************************************/
data hce_inp_admits(drop=Incurred_Month start_engaged_status);
set hce_inp_admits;
run;
/*****************************/
proc summary data=hce_inp_admits nway;
class  hcta_engaged_status;
var ip_Readmit_30;
output out=temp_sum1 (drop=_type_ /*_freq_*/)
Sum = ;
run;

/* Create Readmit_30 and Readmit_90 flags; new logic: mark the admission prior to the readmission */

data inp_30day_flag;		
	     SET hce_inp_admits;		
	     BY Healthplan_Id_Ref mbr_uid descending Conf_Start_Date descending Conf_End_Date;	
		format start_date end_date next_start_date mmddyy10.;

 start_date = Conf_Start_Date;
 end_date = Conf_End_Date;		
			
	     retain Next_Start_Date .;		
			
	     /* Excluding last admit for member -- calc days from next admit start to current admit discharge (end) */		
	     if first.mbr_uid = 0 		
	     THEN DO;		
			
	           Readmit_Days = intck('day', Conf_End_Date, Next_Start_Date);		
	     		
	           IF 0 < Readmit_Days <= 30 		
	                THEN Readmit_30 = 1;		
	           ELSE Readmit_30 = 0;		
			
	           IF 0 < Readmit_Days <= 90 		
	                THEN Readmit_90 = 1;		
	           ELSE Readmit_90 = 0;		
			
	     END;		
			
	    /* Reset next start date for next record*/		
	     Next_Start_Date = Conf_Start_Date;		
	run;

proc sort data=inp_30day_flag out=inp_30day_flag (drop=Next_Start_Date);
  BY mbr_uid start_date end_date;
run;
/* Create Readmit_30 and Readmit_90 flags; old logic: mark the readmission */
data inp_30day_flag;		
	     SET inp_30day_flag;		
	     BY Healthplan_Id_Ref mbr_uid  Start_Date End_Date;	
		format Prior_End_Date mmddyy10.;
		
	     retain Prior_End_Date .;		
			
	     /* Exclude the first admit -- calc days from last admit end to current admit start */		
	     if first.mbr_uid = 0 		
	     THEN DO;		
			
	           Old_Readmit_Days = intck('day', Prior_End_Date, Start_Date);		
	     		
	           IF 0 < Old_Readmit_Days <= 30 		
	                THEN Old_Readmit_30 = 1;		
	           ELSE Old_Readmit_30 = 0;		
		
	     END;		
			
	    /* Reset for next record*/		
	     Prior_End_Date = End_Date;		
	run;

proc contents data=inp_30day_flag; run;
/**** Re-sorting by PatientID and start date**/
proc sort data=inp_30day_flag out=inp_30day_flag (drop=Prior_End_Date Old_Readmit_Days);
  BY mbr_uid start_date end_date;
run;

data inp_30day_flag;
    set inp_30day_flag;
    by mbr_uid;
    group_id + first.mbr_uid;
    counter_in_group  + 1 - first.mbr_uid * counter_in_group;
run;
/****************************************/

proc summary data=inp_30day_flag(where=(Healthplan_Id_Ref^=6)) nway;
class  hcta_engaged_status;
var ip_Readmit_30 Old_Readmit_30 Readmit_30;
output out=temp_sum1 (drop=_type_ /*_freq_*/)
Sum = ;
run;

/*********************************************************/
/* Get visit details data */
proc sql;
connect to odbc as edw (datasrc="LMEDW");
 
	create table visits_details 
	AS SELECT *
	FROM 
	(select * from connection to EDW
	          (select *
				from [HCEReporting].[dbo].[anle_provider_visits_cdphp] where encounter_type = 'Visit'));

disconnect from edw;
quit;


data visits_details;
	set visits_details;
	format visitdate mmddyy10.;
	visitdate=mdy(substr(event_date,6,2),substr(event_date,9,2),substr(event_date,1,4));
run;

/**Get the latest admit end date for each visit***/
proc sql;
	create table visit_wMaxEndDate as
	select 	c.end_date as MaxEndDate,
		b.PatientID, b.visitdate, b.Urgent_Visit, b.Post_Dschg_Visit, b.Staff_Name, b.Staff_Speclty,
		b.next_acuitylevel as acuity, b.next_dda as dda
	from  visits_details as b
		join inp_30day_flag as c
		on b.patientid = c.PatientID_ref
		and b.visitdate >= c.end_date
		group by b.PatientID, b.visitdate, b.Urgent_Visit,b.Post_Dschg_Visit, b.Staff_Name, b.Staff_Speclty,
		b.next_acuitylevel, b.next_dda
		having end_date = max(end_date)
		order by b.PatientID, b.visitdate;
run;
/*********************************************************/
/* Combine post discharge visits with admits*/
proc sql;
	create table post_discharge as
	select a.*, 
		case when VisitDate is not null and INTCK('day', MaxEndDate, VisitDate) <=7 then 1 else 0 end as LMV07D,
		case when VisitDate is not null and INTCK('day', MaxEndDate, VisitDate) <=30 then 1 else 0 end as LMV30D,
		b.*
	from inp_30day_flag as a
	left join visit_wMaxEndDate as b
		on a.patientid_ref = b.patientid
		and a.end_date = b.MaxEndDate
	order by a.Healthplan_Id_Ref, a.patientid_ref, a.start_date, b.VisitDate ;
run;

/** Export results **/
/*
data out.post_discharge;
	set post_discharge;
run;
*/
/* Analyze results */

proc sort data=out.post_discharge(where=(start_date between '01Jan2016'd and '30Jun2018'd)) out=sum1 nodupkey;
	BY Healthplan_Id_Ref patientid_ref start_date end_date;	
run;

proc summary data=sum1 nway;
class /*HP_ID year*/ hcta_engaged_status;
var Ip_Readmit_30 Old_Readmit_30 Readmit_30 LMV07D LMV30D hcta_intensity mmr_risk ESRD HOSPICE;
output out=temp_sum (drop=_type_ /*_freq_*/)
Sum = ;
run;

/***************************************/
proc sql;
	create table sum2 as
	select Healthplan_Id_Ref as HP_ID, DRG,
		case when Readmit_30 = 1 then 'Y' else 'N' end as Readmit30_IND,
		hcta_engaged_status as engaged, 
		count(*) as Admits,
		sum(case when Readmit_30 = 1 then 1 else 0 end) as Readmit_30,
		case when LMV07D = 1 then 'Y' else 'N' end as LMV07D,
		case when LMV30D = 1 then 'Y' else 'N' end as LMV30D,
		ccs
	from sum1
	group by 1,2,3,4,7,8,9;
run;
/*******************************************/

/***** LMV 07D by provider******************/
proc sql;
create table provider7 as
	select Staff_Name, Staff_Speclty, 
		count(*) as Admits,
		sum(case when Readmit_30=. then 0 else Readmit_30 end) as Readmit_30,
		sum(hcta_intensity) as intensity,
		sum(mmr_risk) as mmr_risk,
		sum(ESRD) as ESRD
	from sum1
	where LMV07D = 1 and hcta_engaged_status = 'Y'
	group by 1,2
	
	order by Admits desc
;quit;

/***** LMV 30D by provider******************/
proc sql;
create table provider30 as
	select Staff_Name, Staff_Speclty, 
		count(*) as Admits,
		sum(case when Readmit_30=. then 0 else Readmit_30 end) as Readmit_30,
		sum(case when acuity = 'A' then 4 
				when acuity = 'B' then 3
				when acuity = 'C' then 1
				when acuity = 'D' then 1
				else 0 end) as total_acuity,
		sum(case when acuity in ('A','B','C','D') then 1 else 0 end) as valid_acuity_cnt,
		sum(ESRD) as ESRD
	from sum1
	where LMV30D = 1 and hcta_engaged_status = 'Y'
	group by 1,2
	
	order by Admits desc
;quit;

/* All details */

data temp;
	set sum1(where=(LMV30D = 1 and hcta_engaged_status = 'Y'));
	array vars {*} _numeric_;
	do i = 1 to dim(vars);
		if vars{i} = . then vars{i} = 0;
	end;
	drop i;
run;
/* Duncan's sas code for confidence interval */
data provider; 
set provider7;
Readmit_rate = readmit_30/admits;
;run;

proc means data=provider(where=(admits>9)) fw=8 maxdec=3 alpha=0.05 clm mean std;
/*class staff_name;*/
var Readmit_rate;
title 'Confidence Limits for Readmit_rate';
;run;

/**********************************************/
proc summary data=sum1(where=(staff_name in ('Alice Lewis') and LMV30D = 1)) nway;
class /*HP_ID year*/ patientid;
var Readmit_30 LMV30D;
output out=temp_sum1 (drop=_type_ /*_freq_*/)
Sum = ;
run;

proc summary data=sum1(where=(staff_name in ('Alice Lewis') and LMV30D = 1 and hcta_engaged_status = 'Y')) nway;
class /*HP_ID year*/ CCS;
var Readmit_30 LMV30D;
output out=temp_sum2 (drop=_type_ /*_freq_*/)
Sum = ;
run;
