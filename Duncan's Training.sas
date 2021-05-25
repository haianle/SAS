proc contents data=hce.hcta_member_months; run;
proc contents data=out.hnor_2015_cmplt_mmr; run;



data mm_15 (rename=(medical_member_months=mm default_view=final_landmark_cohort healthplan_memberid=member_id Risk_adj_factor_PartA=rafs
                    Risk_adj_PartA_mm=rafs_mm));

    set hce.hcta_member_months (keep=healthplan_id_ref healthplan_memberid patientid Risk_adj_factor_PartA Risk_adj_PartA_mm 
                                 incurred_month default_view engaged_status medical_member_months);

     where healthplan_id_ref=3 and substr(incurred_month,1,4)='2015' and default_view='Y';

     format incurred_month2 $10.;
     incurred_month2=substr(strip(incurred_month),1,6);

run;


proc freq data=mm_15;
tables incurred_month*rafs_mm / missing;
run;


/* Sort on Keys Prior to Data Step Merge */
/*

proc sort data=mm_15 out=mm_sorted;
by member_id incurred_month2;
run;

data mm_sorted;   set mm_sorted (drop=incurred_month);  rename incurred_month2=incurred_month; run;



proc sort data=out.hnor_2015_cmplt_mmr out=sorted;
by member_id incurred_month;
run;

/* In variable controls join results - also control variables on right most table as these overwrite variables with the same names */
/*
data mm_15 (rename=(risk_adjuster_factor_a=rafs));
   merge mm_sorted (in=a) sorted (keep=member_id incurred_month risk_adjuster_factor_a);
   by member_id incurred_month;
   if a=1;

   if risk_adjuster_factor_a>0 and mm=1 then rafs_mm=1;
      else rafs_mm=0;
run;

*/

data mm_16 ;
    set out.hnor_2016_mm_final (keep=member_id incurred_month mm rafs engaged_status rafs_mm partc_revenue final_landmark_cohort);
     where final_landmark_cohort='Y';

     healthplan_id_ref=3;

run;



data mm_17;
    set out.hnor_2017_mm_final (keep=member_id incurred_month mm rafs partc_revenue engaged_status rafs_mm final_landmark_cohort);
     where final_landmark_cohort='Y';

     healthplan_id_ref=3;

run;


/* First dataset controls variable lengths and formats.  To force put a format statement before the set statement */

data all_mm;

    format member_id $20. incurred_month $12.;

    set mm_15 mm_16 mm_17;

run;

proc freq data=all_mm;
tables incurred_month*rafs_mm / missing;
run;



proc summary data=all_mm nway missing;
class incurred_month engaged_status;
var mm rafs_mm rafs;
output out=rafs_sum (drop=_TYPE_ _FREQ_) sum=;
run;


proc sql;

   create table min_engaged
      as select member_id, min(substr(incurred_month,1,4)) as engagement_vintage from all_mm
          where engaged_status='Y'
      group by member_id
      order by member_id;

quit;

proc sort data=all_mm;
by member_id;

data all_mm;
   merge all_mm (in=a) min_engaged;
   by member_id;
   if a=1;

   year=substr(incurred_month,1,4);
run;


proc summary data=all_mm nway missing;
where engaged_status='Y';
class incurred_month engaged_status engagement_vintage;
id year;
var mm rafs_mm rafs;
output out=engaged_rafs_vintage (drop=_TYPE_ _FREQ_) sum=;
run;
