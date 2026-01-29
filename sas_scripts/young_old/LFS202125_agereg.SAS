DATA work.test;
SET RTRAdata.LFS202125 (keep = ID PROV ERTAB AGE LFSSTAT);

/* Province B.C.*/
if PROV = 59;

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

/*age groups*/
length age_group $ 10;
IF AGE IN ("015", "016", "017", "018", "019", "020", "021", "022", "023", "024", "025", "026", "027", "028", "029", "030", "031", "032", "033", "034", "035", "036", "037", "038", "039", "040", "041", "042", "043", "044", "045", "046", "047", "048", "049", "050") THEN age_group="young";
ELSE age_group="old";

/*Labour Force Status Employed*/

if LFSSTAT IN (1,2);

run;

%RTRAFreq(
     InputDataset=work.test,
     OutputName=agereg202125,
     ClassVarList= BC_Region age_group,
     UserWeight=FINALWT);
run;
