DATA work.test;
SET RTRAdata.LFS200005 (keep = ID PROV ERTAB AGE LFSSTAT NOC_5 NAICS_5 SYEAR);

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

/*Labour Force Status Employed*/

if LFSSTAT IN (1,2);

/*First half of the NOCS plus missing*/

if NOC_5 > 49999;
if missing(NOC_5) then NOC_5="missing";
if missing(NAICS_5) then NAICS_5="missing";
run;

%RTRAFreq(
     InputDataset=work.test,
     OutputName=stat0005p2,
     ClassVarList= SYEAR BC_Region NAICS_5 NOC_5,
     UserWeight=FINALWT);
run;
