DATA work.test;
SET RTRAdata.LFS202125 (keep = ID PROV AGE LFSSTAT NOC_5);

/* Province B.C.*/
if PROV = 59;

/* Age 15 or over */
if AGE >= 15;

/*Labour Force Status*/

length lf_stat $20;

if LFSSTAT IN (1,2) then lf_stat = 'Employed';
else if LFSSTAT in (3,4,5) then lf_stat = 'Unemployed';
else lf_stat = 'not in LF';

/*First half of the NOCS plus missing*/

if NOC_5 > 49999;
if missing(NOC_5) then NOC_5="missing";
run;

%RTRAFreq(
     InputDataset=work.test,
     OutputName=eunocp1,
     ClassVarList= NOC_5 lf_stat,
     UserWeight=FINALWT);
run;
