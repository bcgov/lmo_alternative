DATA work.test;
SET RTRAdata.LFS202125 (keep = ID PROV AGE LFSSTAT NOC_5);

/* Province B.C.*/
if PROV = 59;

/*age groups*/
length age_group $ 10;
IF AGE IN ("015", "016", "017", "018", "019", "020", "021", "022", "023", "024", "025", "026", "027", "028", "029", "030", "031", "032", "033", "034", "035", "036", "037", "038", "039", "040", "041", "042", "043", "044", "045", "046", "047", "048", "049", "050") THEN age_group="young";
ELSE age_group="old";

/*Labour Force Status Employed*/

if LFSSTAT IN (1,2);

/*First half of the NOCS plus missing*/

if NOC_5 > 49999;
if missing(NOC_5) then NOC_5="missing";
run;

%RTRAFreq(
     InputDataset=work.test,
     OutputName=agenoc2,
     ClassVarList= age_group NOC_5,
     UserWeight=FINALWT);
run;
