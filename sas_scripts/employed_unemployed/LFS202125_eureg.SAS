DATA work.test;
SET RTRAdata.LFS202125 (keep = ID PROV ERTAB AGE LFSSTAT);

/* Province B.C.*/
if PROV = 59;

/* Age 15 or over */
if AGE >= 15;

/*Economic Region - British Columbia*/
length BC_Region $ 30;
IF   ERTAB IN ("5910") THEN BC_Region="Vancouver Island and Coast";
ELSE IF ERTAB IN ("5920") THEN BC_Region="Lower Mainland-Southwest";
ELSE IF ERTAB IN ("5930") THEN BC_Region="Thompson-Okanagan";
ELSE IF ERTAB IN ("5940") THEN BC_Region="Kootenay";
ELSE IF ERTAB IN ("5950") THEN BC_Region="Cariboo";
ELSE IF ERTAB IN ("5960", "5970") THEN BC_Region="North Coast & Nechako";
ELSE IF ERTAB IN ("5980") THEN BC_Region="Northeast";
ELSE BC_Region="Other";

/*Labour Force Status*/

length lf_stat $20;

if LFSSTAT IN (1,2) then lf_stat = 'Employed';
else if LFSSTAT in (3,4,5) then lf_stat = 'Unemployed';
else lf_stat = 'not in LF';

run;

%RTRAFreq(
     InputDataset=work.test,
     OutputName=eureg,
     ClassVarList= BC_Region lf_stat,
     UserWeight=FINALWT);
run;
