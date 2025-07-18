# Databricks notebook source
from itertools import chain
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql import SparkSession, functions as F, Row
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import pandas as pd
import datetime as dt

# COMMAND ----------

# MAGIC %md
# MAGIC ##SDOH investigation

# COMMAND ----------

# MAGIC %md
# MAGIC ConceptID's and ConceptNames under class SDOH Attribute

# COMMAND ----------

# All Truveta Concepts with SDOH Attribute Classification
csdoh=spark.sql("""select conceptid, conceptname from cdh_truveta.concept @v13 where conceptclass = "SDOHAttribute"
                """).createOrReplaceTempView("sdoh_concepts")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sdoh_concepts

# COMMAND ----------

# MAGIC %md
# MAGIC Count how many times each (34) SDOH Attribute Concept ID appear in the Social Determinants of Health table.

# COMMAND ----------

attcounts=spark.sql("""
select 
  agg.AttributeConceptId,
  agg.count,
  c.*
from 
  (select 
    AttributeConceptId,
    count(*) as count
  from 
    cdh_truveta.socialdeterminantsofhealth @v13
  group by
    AttributeConceptId) agg
  LEFT join
    sdoh_concepts c on agg.AttributeConceptId = c.conceptid
    ORDER BY agg.count desc""").createOrReplaceTempView("attrcounts")

# COMMAND ----------

ccon=spark.sql("""select conceptid, conceptname from cdh_truveta.concept @v13""").createOrReplaceTempView("c_concepts")

# COMMAND ----------

# MAGIC %md
# MAGIC Normalized Value Concept IDs represent values for the attributes in SDOH table. Count Normalized Value Concept IDs values

# COMMAND ----------

normvcounts=spark.sql("""
select 
  agg.NormalizedValueConceptId,
  agg.count,
  c.*
from 
  (select 
    NormalizedValueConceptId,
    count(*) as count
  from cdh_truveta.socialdeterminantsofhealth
  group by
    NormalizedValueConceptId) agg
  LEFT join
    c_concepts c
    on agg.NormalizedValueConceptId = c.conceptid
    ORDER BY agg.count desc""").createOrReplaceTempView("normvcounts")

# COMMAND ----------

# MAGIC %md
# MAGIC Select from SDOH table, join with concept table

# COMMAND ----------

sdoh2 = spark.sql( """
SELECT 
    PersonId, 
    EffectiveStartDateTime, 
    AttributeConceptId, 
    NormalizedValueNumeric, 
    NormalizedValueConceptId
FROM 
    cdh_truveta.SocialDeterminantsOfHealth;
""").createOrReplaceTempView("tbl_sdoh2")

sdohcon3 = spark.sql("""
SELECT
    tbl_sdoh2.PersonId,
    tbl_sdoh2.EffectiveStartDateTime,
    tbl_sdoh2.NormalizedValueNumeric,
    tbl_sdoh2.NormalizedValueConceptId,
    c_concepts.ConceptId,
    c_concepts.ConceptName AS Attribute
FROM
    tbl_sdoh2
LEFT JOIN c_concepts ON
    tbl_sdoh2.AttributeConceptId = c_concepts.ConceptId;
""").createOrReplaceTempView("tbl_sdoh3")


# COMMAND ----------

# MAGIC %md
# MAGIC Select from Normalized Concept ID's join with concept table

# COMMAND ----------

sdohcon4 = spark.sql("""
SELECT
    s.PersonId,
    s.EffectiveStartDateTime,
    s.Attribute,
    c.ConceptName AS Attribute_Value_Cat,
    s.NormalizedValueNumeric AS Attribute_Value_Numeric
FROM
    tbl_sdoh3 s
LEFT JOIN c_concepts c ON
    s.NormalizedValueConceptId = c.ConceptId
WHERE
    c.ConceptName IS NOT NULL
    AND s.NormalizedValueNumeric IS NOT NULL;
""").createOrReplaceTempView("tbl_sdoh4")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl_sdoh4

# COMMAND ----------

# MAGIC %md
# MAGIC Join SDOH table with COVID target population

# COMMAND ----------

covidtpop_sdoh = spark.sql(""" SELECT tp.PersonId, 
                           s.EffectiveStartDateTime,
                           s.Attribute,
                           s.Attribute_Value_Cat,
                           s.Attribute_Value_Numeric 
                           FROM cdh_truveta_exploratory.ak21_truveta_MMWR_rx_tpop tp
                           LEFT JOIN (SELECT * FROM tbl_sdoh4 s) s ON tp.PersonId = s.PersonId
                           """).createOrReplaceTempView("covidtpop_sdoh")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from covidtpop_sdoh

# COMMAND ----------

# MAGIC %md
# MAGIC Deduplicate, find most recent SDOH attribute for each person id
# MAGIC
# MAGIC (modify this query to include SDOH attributes at indexdate for each person id in Cov pop, not just the most recent one)

# COMMAND ----------

dedup_covsdoh = spark.sql("""
WITH SortedData AS (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY PersonId, 
           Attribute ORDER BY EffectiveStartDateTime DESC, Attribute) --update based on indexdate from  covid gp
             AS rn
    FROM covidtpop_sdoh
)
SELECT *
FROM SortedData
WHERE rn = 1
ORDER BY PersonId, EffectiveStartDateTime, Attribute
""").createOrReplaceTempView("dedup_covsdoh_t")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Attribute, COUNT(DISTINCT PersonId) AS Count
# MAGIC FROM dedup_covsdoh_t
# MAGIC GROUP BY Attribute
# MAGIC
# MAGIC --HouseholdAnnualIncomeRange is available for 160517 out our covid population of 296800

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Attribute_Value_Cat AS HouseholdAnnualIncomeRange, COUNT(DISTINCT PersonId) AS Count
# MAGIC FROM dedup_covsdoh_t
# MAGIC WHERE Attribute = "HouseholdAnnualIncomeRange"
# MAGIC GROUP BY Attribute_Value_Cat
# MAGIC
# MAGIC --categories are not mutually exclusive and counts for overlapping categories do not addup.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Attribute_Value_Cat AS ProspectEstimatedIncomeRange, COUNT(DISTINCT PersonId) AS Count
# MAGIC FROM dedup_covsdoh_t
# MAGIC WHERE Attribute = "ProspectEstimatedIncomeRange"
# MAGIC GROUP BY Attribute_Value_Cat
# MAGIC
# MAGIC --same as household income range categories, categories are not mutually exclusive and counts for overlapping categories do not addup.

# COMMAND ----------

#sdoh pivot table
sdoh_piv = dedup_covsdoh.groupBy(['PersonId', 'Attribute']).agg({
   'EffectiveStartDateTime': 'first',
   'Attribute_Value_Cat': 'first', 
   'Attribute_Value_Numeric': 'first'}).toPandas()



# COMMAND ----------

sdoh_piv.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Urban/Rural investigation

# COMMAND ----------

covidtpop_loc = spark.sql(""" SELECT 
                           l.stateorprovinceconceptid,
                           l.postalorzipcode,
                           LEFT(l.postalorzipcode,3) as zip3dig,
                           tp.PersonId, 
                           tp.startdatetime,
                           case 
                           when zip3dig is null then 'No Information'
                           when zip3dig in ("005","006","007","008","009","010","011","014","015","016","017","018","019","020","021","022","023","024","025","026","027","028","029","030","031","038","039","040","041","044","054","055","060","061","062","063","064","065","066","068","069","070","071","072","073","074","075","076","077","078","079","080","081","082","083","084","085","086","087","088","089","090","091","092","093","094","095","096","097","098","099","100","101","102","103","104","105","106","107","108","109","110","111","112","113","114","115","116","117","118","119","121","122","123","125","126","128","130","132","135","137","139","140","141","142","143","145","146","149","150","151","152","153","154","156","159","164","165","168","170","171","173","174","175","176","180","181","182","185","186","187","189","190","191","192","193","194","195","196","197","198","200","201","202","203","204","205","206","207","208","209","210","211","212","214","215","217","218","219","220","221","222","223","224","228","229","230","231","232","233","234","235","236","237","238","240","250","251","253","254","255","257","258","260","271","272","274","275","276","277","280","281","282","284","286","288","290","292","293","294","295","296","297","298","300","301","302","303","306","307","308","309","311","312","313","314","316","319","321","322","323","325","326","327","328","329","330","331","332","333","334","335","336","337","338","339","340","341","342","344","346","347","349","352","354","356","357","358","361","362","365","366","368","370","371","372","374","375","376","379","381","392","395","399","401","402","405","410","430","432","436","439","440","441","442","443","444","445","447","449","450","452","453","454","455","459","460","461","462","463","464","465","466","468","471","477","478","480","481","482","483","484","485","489","491","494","495","500","503","507","509","511","522","524","528","530","531","532","534","535","537","543","549","551","553","554","555","558","569","571","581","582","585","591","600","601","602","603","604","605","606","607","608","609","611","612","615","616","617","618","620","622","625","627","630","631","633","640","641","645","649","651","658","660","661","662","666","672","681","685","700","701","703","704","705","706","707","708","711","720","722","727","729","730","731","733","741","750","751","752","753","755","760","761","762","763","765","767","769","770","772","773","774","775","776","777","778","780","781","782","784","785","786","787","791","794","796","797","798","799","800","801","802","803","805","806","809","810","815","820","826","836","837","840","841","842","843","844","846","850","851","852","853","857","863","870","871","876","880","885","888","889","890","891","895","897","900","901","902","903","904","905","906","907","908","910","911","912","913","914","915","916","917","918","919","920","921","922","923","924","925","926","927","928","930","931","932","933","934","935","937","938","939","940","941","942","943","944","945","946","947","948","949","950","951","952","953","956","957","958","962","963","964","965","966","968","969","970","972","973","975","980","981","982","983","984","986","990","992","993","994","995","997","00*","02*","07*","08*","09*","10*","11*","20*","33*","34*","60*","70*","90*","91*","92*","94*") then 'Urban'
                           when zip3dig in ("012","013","032","033","034","035","036","037","042","043","045","046","047","048","049","050","051","052","053","056","057","058","059","067","120","124","127","129","131","133","134","136","138","144","147","148","155","157","158","160","161","162","163","166","167","169","172","177","178","179","183","184","188","199","216","225","226","227","239","241","242","243","244","245","246","247","248","249","252","256","259","261","262","263","264","265","266","267","268","270","273","278","279","283","285","287","289","291","299","304","305","310","315","317","318","320","324","350","351","355","359","360","363","364","367","369","373","377","378","380","382","383","384","385","386","387","388","389","390","391","393","394","396","397","398","400","403","404","406","407","408","409","411","412","413","414","415","416","417","418","420","421","422","423","424","425","426","427","431","433","434","435","437","438","446","448","451","456","457","458","467","469","470","472","473","474","475","476","479","486","487","488","490","492","493","496","497","498","499","501","502","504","505","506","508","510","512","513","514","515","516","520","521","523","525","526","527","538","539","540","541","542","544","545","546","547","548","550","556","557","559","560","561","562","563","564","565","566","567","570","572","573","574","575","576","577","580","583","584","586","587","588","590","592","593","594","595","596","597","598","599","610","613","614","619","623","624","626","628","629","634","635","636","637","638","639","644","646","647","648","650","652","653","654","655","656","657","664","665","667","668","669","670","671","673","674","675","676","677","678","679","680","683","684","686","687","688","689","690","691","692","693","710","712","713","714","716","717","718","719","721","723","724","725","726","728","734","735","736","737","738","739","740","743","744","745","746","747","748","749","754","756","757","758","759","764","766","768","779","783","788","789","790","792","793","795","804","807","808","811","812","813","814","816","821","822","823","824","825","827","828","829","830","831","832","833","834","835","838","845","847","855","856","859","860","864","865","873","874","875","877","878","879","881","882","883","884","893","894","898","936","954","955","959","960","961","967","971","974","976","977","978","979","985","988","989","991","996","998","999","42*","69*") then 'Rural'
                           else 'Unknown'
                           end as rurality_status
                           FROM cdh_truveta_exploratory.ak21_truveta_MMWR_rx_tpop tp
                           LEFT JOIN cdh_truveta.personlocation @v13 s ON tp.PersonId = s.PersonId
                           LEFT JOIN cdh_truveta.location @v13 l ON l.id = s.locationid
                           """).createOrReplaceTempView("covidtpop_loc_t")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from covidtpop_loc_t

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT rurality_status, COUNT(DISTINCT PersonId) AS Count
# MAGIC FROM covidtpop_loc_t
# MAGIC GROUP BY rurality_status