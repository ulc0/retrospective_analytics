# Databricks notebook source
# MAGIC %md #Pregnancy identification algorithm

# COMMAND ----------

# MAGIC %md This code aims to identify a pregnancy and the outcome of a pregnancy using different endpoints and timming between events

# COMMAND ----------

# MAGIC %pip install black==22.3.0 tokenize-rt==4.2.1

# COMMAND ----------

# Loading fuctions and cluster optimization settings

#spark.sql("SHOW DATABASES").show(truncate=False)
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, size
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
spark.conf.set('spark.sql.shuffle.partitions',7200*4)
#spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', True)


# COMMAND ----------

spark.conf.set('spark.sql.shuffle.partitions', 6400)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', False)
spark.conf.set('spark.databricks.queryWatchdog.enabled', False)

# COMMAND ----------

/Workspace/CDH/Analytics/Knowledgebase/Health_Verity/hv_code_snippets/includes/0000-utils


# COMMAND ----------


../includes/0000-hv-filters

# COMMAND ----------

# MAGIC %md ## Episode identification oriented codes

# COMMAND ----------

# MAGIC %md ##Delivery codes

# COMMAND ----------

# MAGIC %md Creating delivery code dictionary: This dictionary contains a set of procedures and diagnosis codes that classify the outcome of the pregnancy as
# MAGIC
# MAGIC 1. Live birth A-LB
# MAGIC 2. Still birth B-SB
# MAGIC 3. Ectopic pregnancy C-ECT
# MAGIC 4. Abortion D-AB, D-SAB
# MAGIC 5. Unknown

# COMMAND ----------

outcomeCodes = [


                ["DX","Z332","D-AB","Encounter for elective termination of pregnancy"],
                ["DX","O04","D-AB","Complications following (induced) termination of pregnancy"],
                ["PCS","10A00ZZ","D-AB","Abortion of products of conception, open approach"],
                ["PCS","10A03ZZ","D-AB","Abortion of products of conception, percutaneous approach"],
                ["PCS","10A04ZZ","D-AB","Abortion of products of conception, percutaneous endoscopic approach"],
                ["PCS","10A07Z6","D-AB","Abortion of products of conception, vacuum, via natural or artificial opening"],
                ["PCS","10A07ZW","D-AB","Abortion of products of conception, laminaria, via natural or artificial opening"],
                ["PCS","10A07ZX","D-AB","Abortion of products of conception, abortifacient, via natural or artificial opening"],
                ["PCS","10A07ZZ","D-AB","Abortion of products of conception, via natural or artificial opening"],
                ["PCS","10A08ZZ","D-AB","Abortion of products of conception, via natural or artificial opening, endoscopic"],
                ["HCPCS","1966","D-AB","Anesthesia for induced abortion"],
                ["HCPCS","1964","D-AB","Anesthesia for abortion procedures"],
                ["HCPCS","59840","D-AB","Induced abortion by dilation and curettage"],
                ["HCPCS","59841","D-AB","Induced abortion by dilation and evacuation"],
                ["HCPCS","59850","D-AB","Induced abortion, by 1 or more intra-amniotic injections (amniocentesis injections), including hospital admission and visits, delivery of fetus and secundines;"],
                ["HCPCS","59851","D-AB","Induced abortion, by 1 or more intra-amniotic injections (amniocentesis injections), including hospital admission and visits, delivery of fetus and secundines; with dilation and curettage and/or evacuation"],
                ["HCPCS","59852","D-AB","Induced abortion, by 1 or more intra-amniotic injections (amniocentesis injections), including hospital admission and visits, delivery of fetus and secundines; with hysterotomy (failed intra-amniotic injection)"],
                ["HCPCS","59855","D-AB","Induced abortion, by 1 or more vaginal suppositories (e.g., prostaglandin) with or without cervical dilation (e.g., laminaria), including hospital admission and visits, delivery of fetus and secundines"],
                ["HCPCS","59856","D-AB","Induced abortion, by 1 or more vaginal suppositories (e.g., prostaglandin) with or without cervical dilation (e.g., laminaria), including hospital admission and visits, delivery of fetus and secundines; with dilation and curettage and/or evacuation"],
                ["HCPCS","59857","D-AB","Induced abortion, by 1 or more vaginal suppositories (e.g., prostaglandin) with or without cervical dilation (e.g., laminaria), including hospital admission and visits, delivery of fetus and secundines; with hysterotomy (failed medical evacuation)"],
                ["HCPCS","S0199","D-AB","Medically induced abortion by oral ingestion of medication including all associated services and supplies (e.g., patient counseling, office visits, confirmation of pregnancy by HCG, ultrasound to confirm duration of pregnancy)"],
                ["HCPCS","S2260","D-AB","Induced abortion, 17 to 24 weeks"],
                ["HCPCS","S2265","D-AB","Induced abortion, 25 to 28 weeks"],
                ["HCPCS","S2262","D-AB","Abortion for maternal indication, 25 weeks or greater"],
                ["HCPCS","S2266","D-AB","Induced abortion, 29 to 31 weeks"],
                ["HCPCS","S2267","D-AB","Induced abortion, 32 weeks or greater"],
                ["DRG","770","D-AB","Abortion with dilation and curettage, aspiration curettage or hysterotomy"],
                ["DRG","779","D-AB","Abortion without dilation and curettage"],
                ["DX","O00","C-ECT","Ectopic pregnancy"],
                ["DX","O08B","C-ECT","Complications following ectopic and molar pregnancy"],
                ["PCS","10T20ZZ","C-ECT","Resection of products of conception, ectopic, open approach"],
                ["PCS","10T23ZZ","C-ECT","Resection of products of conception, ectopic, percutaneous approach"],
                ["PCS","10T24ZZ","C-ECT","Resection of products of conception, ectopic, percutaneous endoscopic approach"],
                ["PCS","10T27ZZ","C-ECT","Resection of products of conception, ectopic, via natural or artificial opening"],
                ["PCS","10T28ZZ","C-ECT","Resection of products of conception, ectopic, via natural or artificial opening endoscopic"],
                ["PCS","10D27ZZ","C-ECT","Extraction of products of conception, ectopic, via natural or artificial opening"],
                ["PCS","10D28ZZ","C-ECT","Extraction of products of conception, ectopic, via natural or artificial opening endoscopic"],
                ["HCPCS","59100","C-ECT","Surgical treatment of ectopic pregnancy: tubal or ovarian, requiring salpingectomy and/or oophorectomy, abdominal or vaginal approach"],
                ["HCPCS","59120","C-ECT","Surgical treatment of ectopic pregnancy; tubal or ovarian, requiring salpingectomy and/or oophorectomy, abdominal or vaginal approach"],
                ["HCPCS","59121","C-ECT","Surgical treatment of ectopic pregnancy; tubal or ovarian, without salpingectomy and/or oophorectomy"],
                ["HCPCS","59130","C-ECT","Surgical treatment of ectopic pregnancy; abdominal pregnancy"],
                ["HCPCS","59135","C-ECT","Surgical treatment of ectopic pregnancy; interstitial, uterine pregnancy requiring total hysterectomy"],
                ["HCPCS","59136","C-ECT","Surgical treatment of ectopic pregnancy; interstitial, uterine pregnancy with partial resection of uterus"],
                ["HCPCS","59140","C-ECT","Surgical treatment of ectopic pregnancy; cervical, with evacuation"],
                ["HCPCS","59150","C-ECT","Laparoscopic treatment of ectopic pregnancy; without salpingectomy and/or oophorectomy"],
                ["HCPCS","59151","C-ECT","Laparoscopic treatment of ectopic pregnancy; with salpingectomy and/oroophorectomy"],
                ["DRG","777","C-ECT","Ectopic pregnancy"],
                ["DX","O01","C-ECT","Hydatidiform mole"],
                ["DX","O020","C-ECT","Blighted ovum and nonhydatidiform mole"],
                ["DX","O0289","C-ECT","Other abnormal products of conception"],
                ["DX","O029","C-ECT","Abnormal product of conception, unspecified"],
                ["HCPCS","59870d","C-ECT","Evacuation and curettage of uterus for hydatidiform mole"],
                ["DX","Z370","A-LB","Single live birth"],
                ["DX","Z372","A-LB","Twins, both liveborn"],
                ["DX","Z3750","A-LB","Multiple births, unspecified, all liveborn"],
                ["DX","Z3751","A-LB","Triplets, all liveborn"],
                ["DX","Z3752","A-LB","Quadruplets, all liveborn"],
                ["DX","Z3753","A-LB","Quintuplets, all liveborn"],
                ["DX","Z3754","A-LB","Sextuplets, all liveborn"],
                ["DX","Z3759","A-LB","Other multiple births, all liveborn"],
                ["DX","O80","A-LB","Encounter for full-term uncomplicated delivery"],
                ["DX","Z373","A-LB","Twins, one liveborn and one stillborn"],
                ["DX","Z3760","A-LB","Multiple births, unspecified, some liveborn"],
                ["DX","Z3761","A-LB","Triplets, some liveborn"],
                ["DX","Z3762","A-LB","Quadruplets, some liveborn"],
                ["DX","Z3763","A-LB","Quintuplets, some liveborn"],
                ["DX","Z3764","A-LB","Sextuplets, some liveborn"],
                ["DX","Z3769","A-LB","Other multiple births, some liveborn"],
                ["DX","O03","D-SAB","Spontaneous abortion"],
                ["DX","O021","D-SAB","Missed abortion"],
                ["HCPCS","1965","D-SAB","Anesthesia for missed abortion"],
                ["HCPCS","59812","D-SAB","Treatment of incomplete abortion, any trimester, completed surgically"],
                ["HCPCS","59820","D-SAB","Treatment of missed abortion, completed surgically; first trimester"],
                ["HCPCS","59821","D-SAB","Treatment of missed abortion, completed surgically; second trimester"],
                ["HCPCS","59830","D-SAB","Treatment of septic abortion, completed surgically"],
                ["DX","Z371","B-SB","Single stillbirth"],
                ["DX","Z374","B-SB","Twins, both stillborn"],
                ["DX","Z377","B-SB","Other multiple births, all stillborn"],
                ["DX","O364XX0","B-SB","Maternal care for intrauterine death, not applicable or unspecified"],
                ["DX","O364XX1","B-SB","Maternal care for intrauterine death, fetus 1"],
                ["DX","O364XX2","B-SB","Maternal care for intrauterine death, fetus 2"],
                ["DX","O364XX3","B-SB","Maternal care for intrauterine death, fetus 3"],
                ["DX","O364XX4","B-SB","Maternal care for intrauterine death, fetus 4"],
                ["DX","O364XX5","B-SB","Maternal care for intrauterine death, fetus 5"],
                ["DX","O364XX9","B-SB","Maternal care for intrauterine death, other fetus"],
                ["DX","O1002","Unknown","Pre-existing essential hypertension complicating childbirth"],
                ["DX","O1012","Unknown","Pre-existing hypertensive heart disease complicating childbirth"],
                ["DX","O1022","Unknown","Pre-existing hypertensive chronic kidney disease complicating childbirth"],
                ["DX","O1032","Unknown","Pre-existing hypertensive heart and chronic kidney disease complicating childbirth"],
                ["DX","O1042","Unknown","Pre-existing secondary hypertension complicating childbirth"],
                ["DX","O1092","Unknown","Unspecified pre-existing hypertension complicating childbirth"],
                ["DX","O114","Unknown","Pre-existing hypertension with pre-eclampsia, complicating childbirth"],
                ["DX","O1204","Unknown","Gestational edema, complicating childbirth"],
                ["DX","O1214","Unknown","Gestational proteinuria, complicating childbirth"],
                ["DX","O1224","Unknown","Gestational edema with proteinuria, complicating childbirth"],
                ["DX","O134","Unknown","Gestational [pregnancy-induced] hypertension without significant proteinuria, complicating childbirth"],
                ["DX","O1404","Unknown","Mild to moderate pre-eclampsia, complicating childbirth"],
                ["DX","O1414","Unknown","Severe pre-eclampsia complicating childbirth"],
                ["DX","O1424","Unknown","HELLP syndrome, complicating childbirth"],
                ["DX","O1494","Unknown","Unspecified pre-eclampsia, complicating childbirth"],
                ["DX","O151","Unknown","Eclampsia complicating childbirth"],
                ["DX","O164","Unknown","Unspecified maternal hypertension, complicating childbirth"],
                ["DX","O2402","Unknown","Pre-existing type 1 diabetes mellitus, in childbirth"],
                ["DX","O2412","Unknown","Pre-existing type 2 diabetes mellitus, in childbirth"],
                ["DX","O2432","Unknown","Unspecified pre-existing diabetes mellitus in childbirth"],
                ["DX","O24420","Unknown","Gestational diabetes mellitus in childbirth, diet controlled"],
                ["DX","O24424","Unknown","Gestational diabetes mellitus in childbirth, insulin controlled"],
                ["DX","O24425","Unknown","Gestational diabetes mellitus in childbirth, controlled by oral hypoglycemic drugs"],
                ["DX","O24429","Unknown","Gestational diabetes mellitus in childbirth, unspecified control"],
                ["DX","O2482","Unknown","Other pre-existing diabetes mellitus in childbirth"],
                ["DX","O2492","Unknown","Unspecified diabetes mellitus in childbirth"],
                ["DX","O252","Unknown","Malnutrition in childbirth"],
                ["DX","O2662","Unknown","Liver and biliary tract disorders in childbirth"],
                ["DX","O2672","Unknown","Subluxation of symphysis (pubis) in childbirth"],
                ["DX","O6010X0","Unknown","Preterm labor with preterm delivery, unspecified trimester, not applicable or unspecified"],
                ["DX","O6010X1","Unknown","Preterm labor with preterm delivery, unspecified trimester, fetus 1"],
                ["DX","O6010X2","Unknown","Preterm labor with preterm delivery, unspecified trimester, fetus 2"],
                ["DX","O6010X3","Unknown","Preterm labor with preterm delivery, unspecified trimester, fetus 3"],
                ["DX","O6010X4","Unknown","Preterm labor with preterm delivery, unspecified trimester, fetus 4"],
                ["DX","O6010X5","Unknown","Preterm labor with preterm delivery, unspecified trimester, fetus 5"],
                ["DX","O6010X9","Unknown","Preterm labor with preterm delivery, unspecified trimester, other fetus"],
                ["DX","O6012X0","Unknown","Preterm labor second trimester with preterm delivery second trimester, not applicable or unspecified"],
                ["DX","O6012X1","Unknown","Preterm labor second trimester with preterm delivery second trimester, fetus 1"],
                ["DX","O6012X2","Unknown","Preterm labor second trimester with preterm delivery second trimester, fetus 2"],
                ["DX","O6012X3","Unknown","Preterm labor second trimester with preterm delivery second trimester, fetus 3"],
                ["DX","O6012X4","Unknown","Preterm labor second trimester with preterm delivery second trimester, fetus 4"],
                ["DX","O6012X5","Unknown","Preterm labor second trimester with preterm delivery second trimester, fetus 5"],
                ["DX","O6012X9","Unknown","Preterm labor second trimester with preterm delivery second trimester, other fetus"],
                ["DX","O6013X0","Unknown","Preterm labor second trimester with preterm delivery third trimester, not applicable or unspecified"],
                ["DX","O6013X1","Unknown","Preterm labor second trimester with preterm delivery third trimester, fetus 1"],
                ["DX","O6013X2","Unknown","Preterm labor second trimester with preterm delivery third trimester, fetus 2"],
                ["DX","O6013X3","Unknown","Preterm labor second trimester with preterm delivery third trimester, fetus 3"],
                ["DX","O6013X4","Unknown","Preterm labor second trimester with preterm delivery third trimester, fetus 4"],
                ["DX","O6013X5","Unknown","Preterm labor second trimester with preterm delivery third trimester, fetus 5"],
                ["DX","O6013X9","Unknown","Preterm labor second trimester with preterm delivery third trimester, other fetus"],
                ["DX","O6014X0","Unknown","Preterm labor third trimester with preterm delivery third trimester, not applicable or unspecified"],
                ["DX","O6014X1","Unknown","Preterm labor third trimester with preterm delivery third trimester, fetus 1"],
                ["DX","O6014X2","Unknown","Preterm labor third trimester with preterm delivery third trimester, fetus 2"],
                ["DX","O6014X3","Unknown","Preterm labor third trimester with preterm delivery third trimester, fetus 3"],
                ["DX","O6014X4","Unknown","Preterm labor third trimester with preterm delivery third trimester, fetus 4"],
                ["DX","O6014X5","Unknown","Preterm labor third trimester with preterm delivery third trimester, fetus 5"],
                ["DX","O6014X9","Unknown","Preterm labor third trimester with preterm delivery third trimester, other fetus"],
                ["DX","O6020X0","Unknown","Term delivery with preterm labor, unspecified trimester, not applicable or unspecified"],
                ["DX","O6020X1","Unknown","Term delivery with preterm labor, unspecified trimester, fetus 1"],
                ["DX","O6020X2","Unknown","Term delivery with preterm labor, unspecified trimester, fetus 2"],
                ["DX","O6020X3","Unknown","Term delivery with preterm labor, unspecified trimester, fetus 3"],
                ["DX","O6020X4","Unknown","Term delivery with preterm labor, unspecified trimester, fetus 4"],
                ["DX","O6020X5","Unknown","Term delivery with preterm labor, unspecified trimester, fetus 5"],
                ["DX","O6020X9","Unknown","Term delivery with preterm labor, unspecified trimester, other fetus"],
                ["DX","O6022X0","Unknown","Term delivery with preterm labor, second trimester, not applicable or unspecified"],
                ["DX","O6022X1","Unknown","Term delivery with preterm labor, second trimester, fetus 1"],
                ["DX","O6022X2","Unknown","Term delivery with preterm labor, second trimester, fetus 2"],
                ["DX","O6022X3","Unknown","Term delivery with preterm labor, second trimester, fetus 3"],
                ["DX","O6022X4","Unknown","Term delivery with preterm labor, second trimester, fetus 4"],
                ["DX","O6022X5","Unknown","Term delivery with preterm labor, second trimester, fetus 5"],
                ["DX","O6022X9","Unknown","Term delivery with preterm labor, second trimester, other fetus"],
                ["DX","O6023X0","Unknown","Term delivery with preterm labor, third trimester, not applicable or unspecified"],
                ["DX","O6023X1","Unknown","Term delivery with preterm labor, third trimester, fetus 1"],
                ["DX","O6023X2","Unknown","Term delivery with preterm labor, third trimester, fetus 2"],
                ["DX","O6023X3","Unknown","Term delivery with preterm labor, third trimester, fetus 3"],
                ["DX","O6023X4","Unknown","Term delivery with preterm labor, third trimester, fetus 4"],
                ["DX","O6023X5","Unknown","Term delivery with preterm labor, third trimester, fetus 5"],
                ["DX","O6023X9","Unknown","Term delivery with preterm labor, third trimester, other fetus"],
                ["DX","O82","Unknown","Encounter for cesarean delivery without indication"],
                ["DX","O4200","Unknown","Premature rupture of membranes, onset of labor within 24 hours of rupture, unspecified weeks of gestation"],
                ["DX","O42011","Unknown","Preterm premature rupture of membranes, onset of labor within 24 hours of rupture, first trimester"],
                ["DX","O42012","Unknown","Preterm premature rupture of membranes, onset of labor within 24 hours of rupture, second trimester"],
                ["DX","O42013","Unknown","Preterm premature rupture of membranes, onset of labor within 24 hours of rupture, third trimester"],
                ["DX","O42019","Unknown","Preterm premature rupture of membranes, onset of labor within 24 hours of rupture, unspecified trimester"],
                ["DX","O4202","Unknown","Full-term premature rupture of membranes, onset of labor within 24 hours of rupture"],
                ["DX","O630","Unknown","Prolonged first stage (of labor)"],
                ["DX","O631","Unknown","Prolonged second stage (of labor)"],
                ["DX","O632","Unknown","Delayed delivery of second twin, triplet, etc."],
                ["DX","O639","Unknown","Long labor, unspecified"],
                ["DX","O640XX0","Unknown","Obstructed labor due to incomplete rotation of fetal head, not applicable or unspecified"],
                ["DX","O640XX1","Unknown","Obstructed labor due to incomplete rotation of fetal head, fetus 1"],
                ["DX","O640XX2","Unknown","Obstructed labor due to incomplete rotation of fetal head, fetus 2"],
                ["DX","O640XX3","Unknown","Obstructed labor due to incomplete rotation of fetal head, fetus 3"],
                ["DX","O640XX4","Unknown","Obstructed labor due to incomplete rotation of fetal head, fetus 4"],
                ["DX","O640XX5","Unknown","Obstructed labor due to incomplete rotation of fetal head, fetus 5"],
                ["DX","O640XX9","Unknown","Obstructed labor due to incomplete rotation of fetal head, other fetus"],
                ["DX","O641XX0","Unknown","Obstructed labor due to breech presentation, not applicable or unspecified"],
                ["DX","O641XX1","Unknown","Obstructed labor due to breech presentation, fetus 1"],
                ["DX","O641XX2","Unknown","Obstructed labor due to breech presentation, fetus 2"],
                ["DX","O641XX3","Unknown","Obstructed labor due to breech presentation, fetus 3"],
                ["DX","O641XX4","Unknown","Obstructed labor due to breech presentation, fetus 4"],
                ["DX","O641XX5","Unknown","Obstructed labor due to breech presentation, fetus 5"],
                ["DX","O641XX9","Unknown","Obstructed labor due to breech presentation, other fetus"],
                ["DX","O642XX0","Unknown","Obstructed labor due to face presentation, not applicable or unspecified"],
                ["DX","O642XX1","Unknown","Obstructed labor due to face presentation, fetus 1"],
                ["DX","O642XX2","Unknown","Obstructed labor due to face presentation, fetus 2"],
                ["DX","O642XX3","Unknown","Obstructed labor due to face presentation, fetus 3"],
                ["DX","O642XX4","Unknown","Obstructed labor due to face presentation, fetus 4"],
                ["DX","O642XX5","Unknown","Obstructed labor due to face presentation, fetus 5"],
                ["DX","O642XX9","Unknown","Obstructed labor due to face presentation, other fetus"],
                ["DX","O643XX0","Unknown","Obstructed labor due to brow presentation, not applicable or unspecified"],
                ["DX","O643XX1","Unknown","Obstructed labor due to brow presentation, fetus 1"],
                ["DX","O643XX2","Unknown","Obstructed labor due to brow presentation, fetus 2"],
                ["DX","O643XX3","Unknown","Obstructed labor due to brow presentation, fetus 3"],
                ["DX","O643XX4","Unknown","Obstructed labor due to brow presentation, fetus 4"],
                ["DX","O643XX5","Unknown","Obstructed labor due to brow presentation, fetus 5"],
                ["DX","O643XX9","Unknown","Obstructed labor due to brow presentation, other fetus"],
                ["DX","O644XX0","Unknown","Obstructed labor due to shoulder presentation, not applicable or unspecified"],
                ["DX","O644XX1","Unknown","Obstructed labor due to shoulder presentation, fetus 1"],
                ["DX","O644XX2","Unknown","Obstructed labor due to shoulder presentation, fetus 2"],
                ["DX","O644XX3","Unknown","Obstructed labor due to shoulder presentation, fetus 3"],
                ["DX","O644XX4","Unknown","Obstructed labor due to shoulder presentation, fetus 4"],
                ["DX","O644XX5","Unknown","Obstructed labor due to shoulder presentation, fetus 5"],
                ["DX","O644XX9","Unknown","Obstructed labor due to shoulder presentation, other fetus"],
                ["DX","O645XX0","Unknown","Obstructed labor due to compound presentation, not applicable or unspecified"],
                ["DX","O645XX1","Unknown","Obstructed labor due to compound presentation, fetus 1"],
                ["DX","O645XX2","Unknown","Obstructed labor due to compound presentation, fetus 2"],
                ["DX","O645XX3","Unknown","Obstructed labor due to compound presentation, fetus 3"],
                ["DX","O645XX4","Unknown","Obstructed labor due to compound presentation, fetus 4"],
                ["DX","O645XX5","Unknown","Obstructed labor due to compound presentation, fetus 5"],
                ["DX","O645XX9","Unknown","Obstructed labor due to compound presentation, other fetus"],
                ["DX","O648XX0","Unknown","Obstructed labor due to other malposition and malpresentation, not applicable or unspecified"],
                ["DX","O648XX1","Unknown","Obstructed labor due to other malposition and malpresentation, fetus 1"],
                ["DX","O648XX2","Unknown","Obstructed labor due to other malposition and malpresentation, fetus 2"],
                ["DX","O648XX3","Unknown","Obstructed labor due to other malposition and malpresentation, fetus 3"],
                ["DX","O648XX4","Unknown","Obstructed labor due to other malposition and malpresentation, fetus 4"],
                ["DX","O648XX5","Unknown","Obstructed labor due to other malposition and malpresentation, fetus 5"],
                ["DX","O648XX9","Unknown","Obstructed labor due to other malposition and malpresentation, other fetus"],
                ["DX","O649XX0","Unknown","Obstructed labor due to malposition and malpresentation, unspecified, not applicable or unspecified"],
                ["DX","O649XX1","Unknown","Obstructed labor due to malposition and malpresentation, unspecified, fetus 1"],
                ["DX","O649XX2","Unknown","Obstructed labor due to malposition and malpresentation, unspecified, fetus 2"],
                ["DX","O649XX3","Unknown","Obstructed labor due to malposition and malpresentation, unspecified, fetus 3"],
                ["DX","O649XX4","Unknown","Obstructed labor due to malposition and malpresentation, unspecified, fetus 4"],
                ["DX","O649XX5","Unknown","Obstructed labor due to malposition and malpresentation, unspecified, fetus 5"],
                ["DX","O649XX9","Unknown","Obstructed labor due to malposition and malpresentation, unspecified, other fetus"],
                ["DX","O650","Unknown","Obstructed labor due to deformed pelvis"],
                ["DX","O651","Unknown","Obstructed labor due to generally contracted pelvis"],
                ["DX","O652","Unknown","Obstructed labor due to pelvic inlet contraction"],
                ["DX","O653","Unknown","Obstructed labor due to pelvic outlet and mid-cavity contraction"],
                ["DX","O654","Unknown","Obstructed labor due to fetopelvic disproportion, unspecified"],
                ["DX","O655","Unknown","Obstructed labor due to abnormality of maternal pelvic organs"],
                ["DX","O658","Unknown","Obstructed labor due to other maternal pelvic abnormalities"],
                ["DX","O659","Unknown","Obstructed labor due to maternal pelvic abnormality, unspecified"],
                ["DX","O660","Unknown","Obstructed labor due to shoulder dystocia"],
                ["DX","O661","Unknown","Obstructed labor due to locked twins"],
                ["DX","O662","Unknown","Obstructed labor due to unusually large fetus"],
                ["DX","O663","Unknown","Obstructed labor due to other abnormalities of fetus"],
                ["DX","O665","Unknown","Attempted application of vacuum extractor and forceps"],
                ["DX","O666","Unknown","Obstructed labor due to other multiple fetuses"],
                ["DX","O668","Unknown","Other specified obstructed labor"],
                ["DX","O669","Unknown","Obstructed labor, unspecified"],
                ["DX","O670","Unknown","Intrapartum hemorrhage with coagulation defect"],
                ["DX","O678","Unknown","Other intrapartum hemorrhage"],
                ["DX","O679","Unknown","Intrapartum hemorrhage, unspecified"],
                ["DX","O68","Unknown","Labor and delivery complicated by abnormality of fetal acid-base balance"],
                ["DX","O690XX0","Unknown","Labor and delivery complicated by prolapse of cord, not applicable or unspecified"],
                ["DX","O690XX1","Unknown","Labor and delivery complicated by prolapse of cord, fetus 1"],
                ["DX","O690XX2","Unknown","Labor and delivery complicated by prolapse of cord, fetus 2"],
                ["DX","O690XX3","Unknown","Labor and delivery complicated by prolapse of cord, fetus 3"],
                ["DX","O690XX4","Unknown","Labor and delivery complicated by prolapse of cord, fetus 4"],
                ["DX","O690XX5","Unknown","Labor and delivery complicated by prolapse of cord, fetus 5"],
                ["DX","O690XX9","Unknown","Labor and delivery complicated by prolapse of cord, other fetus"],
                ["DX","O691XX0","Unknown","Labor and delivery complicated by cord around neck, with compression, not applicable or unspecified"],
                ["DX","O691XX1","Unknown","Labor and delivery complicated by cord around neck, with compression, fetus 1"],
                ["DX","O691XX2","Unknown","Labor and delivery complicated by cord around neck, with compression, fetus 2"],
                ["DX","O691XX3","Unknown","Labor and delivery complicated by cord around neck, with compression, fetus 3"],
                ["DX","O691XX4","Unknown","Labor and delivery complicated by cord around neck, with compression, fetus 4"],
                ["DX","O691XX5","Unknown","Labor and delivery complicated by cord around neck, with compression, fetus 5"],
                ["DX","O691XX9","Unknown","Labor and delivery complicated by cord around neck, with compression, other fetus"],
                ["DX","O692XX0","Unknown","Labor and delivery complicated by other cord entanglement, with compression, not applicable or unspecified"],
                ["DX","O692XX1","Unknown","Labor and delivery complicated by other cord entanglement, with compression, fetus 1"],
                ["DX","O692XX2","Unknown","Labor and delivery complicated by other cord entanglement, with compression, fetus 2"],
                ["DX","O692XX3","Unknown","Labor and delivery complicated by other cord entanglement, with compression, fetus 3"],
                ["DX","O692XX4","Unknown","Labor and delivery complicated by other cord entanglement, with compression, fetus 4"],
                ["DX","O692XX5","Unknown","Labor and delivery complicated by other cord entanglement, with compression, fetus 5"],
                ["DX","O692XX9","Unknown","Labor and delivery complicated by other cord entanglement, with compression, other fetus"],
                ["DX","O693XX0","Unknown","Labor and delivery complicated by short cord, not applicable or unspecified"],
                ["DX","O693XX1","Unknown","Labor and delivery complicated by short cord, fetus 1"],
                ["DX","O693XX2","Unknown","Labor and delivery complicated by short cord, fetus 2"],
                ["DX","O693XX3","Unknown","Labor and delivery complicated by short cord, fetus 3"],
                ["DX","O693XX4","Unknown","Labor and delivery complicated by short cord, fetus 4"],
                ["DX","O693XX5","Unknown","Labor and delivery complicated by short cord, fetus 5"],
                ["DX","O693XX9","Unknown","Labor and delivery complicated by short cord, other fetus"],
                ["DX","O694XX0","Unknown","Labor and delivery complicated by vasa previa, not applicable or unspecified"],
                ["DX","O694XX1","Unknown","Labor and delivery complicated by vasa previa, fetus 1"],
                ["DX","O694XX2","Unknown","Labor and delivery complicated by vasa previa, fetus 2"],
                ["DX","O694XX3","Unknown","Labor and delivery complicated by vasa previa, fetus 3"],
                ["DX","O694XX4","Unknown","Labor and delivery complicated by vasa previa, fetus 4"],
                ["DX","O694XX5","Unknown","Labor and delivery complicated by vasa previa, fetus 5"],
                ["DX","O694XX9","Unknown","Labor and delivery complicated by vasa previa, other fetus"],
                ["DX","O695XX0","Unknown","Labor and delivery complicated by vascular lesion of cord, not applicable or unspecified"],
                ["DX","O695XX1","Unknown","Labor and delivery complicated by vascular lesion of cord, fetus 1"],
                ["DX","O695XX2","Unknown","Labor and delivery complicated by vascular lesion of cord, fetus 2"],
                ["DX","O695XX3","Unknown","Labor and delivery complicated by vascular lesion of cord, fetus 3"],
                ["DX","O695XX4","Unknown","Labor and delivery complicated by vascular lesion of cord, fetus 4"],
                ["DX","O695XX5","Unknown","Labor and delivery complicated by vascular lesion of cord, fetus 5"],
                ["DX","O695XX9","Unknown","Labor and delivery complicated by vascular lesion of cord, other fetus"],
                ["DX","O6981X0","Unknown","Labor and delivery complicated by cord around neck, without compression, not applicable or unspecified"],
                ["DX","O6981X1","Unknown","Labor and delivery complicated by cord around neck, without compression, fetus 1"],
                ["DX","O6981X2","Unknown","Labor and delivery complicated by cord around neck, without compression, fetus 2"],
                ["DX","O6981X3","Unknown","Labor and delivery complicated by cord around neck, without compression, fetus 3"],
                ["DX","O6981X4","Unknown","Labor and delivery complicated by cord around neck, without compression, fetus 4"],
                ["DX","O6981X5","Unknown","Labor and delivery complicated by cord around neck, without compression, fetus 5"],
                ["DX","O6981X9","Unknown","Labor and delivery complicated by cord around neck, without compression, other fetus"],
                ["DX","O6982X0","Unknown","Labor and delivery complicated by other cord entanglement, without compression, not applicable or unspecified"],
                ["DX","O6982X1","Unknown","Labor and delivery complicated by other cord entanglement, without compression, fetus 1"],
                ["DX","O6982X2","Unknown","Labor and delivery complicated by other cord entanglement, without compression, fetus 2"],
                ["DX","O6982X3","Unknown","Labor and delivery complicated by other cord entanglement, without compression, fetus 3"],
                ["DX","O6982X4","Unknown","Labor and delivery complicated by other cord entanglement, without compression, fetus 4"],
                ["DX","O6982X5","Unknown","Labor and delivery complicated by other cord entanglement, without compression, fetus 5"],
                ["DX","O6982X9","Unknown","Labor and delivery complicated by other cord entanglement, without compression, other fetus"],
                ["DX","O6989X0","Unknown","Labor and delivery complicated by other cord complications, not applicable or unspecified"],
                ["DX","O6989X1","Unknown","Labor and delivery complicated by other cord complications, fetus 1"],
                ["DX","O6989X2","Unknown","Labor and delivery complicated by other cord complications, fetus 2"],
                ["DX","O6989X3","Unknown","Labor and delivery complicated by other cord complications, fetus 3"],
                ["DX","O6989X4","Unknown","Labor and delivery complicated by other cord complications, fetus 4"],
                ["DX","O6989X5","Unknown","Labor and delivery complicated by other cord complications, fetus 5"],
                ["DX","O6989X9","Unknown","Labor and delivery complicated by other cord complications, other fetus"],
                ["DX","O699XX0","Unknown","Labor and delivery complicated by cord complication, unspecified, not applicable or unspecified"],
                ["DX","O699XX1","Unknown","Labor and delivery complicated by cord complication, unspecified, fetus 1"],
                ["DX","O699XX2","Unknown","Labor and delivery complicated by cord complication, unspecified, fetus 2"],
                ["DX","O699XX3","Unknown","Labor and delivery complicated by cord complication, unspecified, fetus 3"],
                ["DX","O699XX4","Unknown","Labor and delivery complicated by cord complication, unspecified, fetus 4"],
                ["DX","O699XX5","Unknown","Labor and delivery complicated by cord complication, unspecified, fetus 5"],
                ["DX","O699XX9","Unknown","Labor and delivery complicated by cord complication, unspecified, other fetus"],
                ["DX","O700","Unknown","First degree perineal laceration during delivery"],
                ["DX","O701","Unknown","Second degree perineal laceration during delivery"],
                ["DX","O702","Unknown","Third degree perineal laceration during delivery"],
                ["DX","O7020","Unknown","Third degree perineal laceration during delivery, unspecified"],
                ["DX","O7021","Unknown","Third degree perineal laceration during delivery, IIIa"],
                ["DX","O7022","Unknown","Third degree perineal laceration during delivery, IIIb"],
                ["DX","O7023","Unknown","Third degree perineal laceration during delivery, IIIc"],
                ["DX","O703","Unknown","Fourth degree perineal laceration during delivery"],
                ["DX","O704","Unknown","Anal sphincter tear complicating delivery, not associated with third degree laceration"],
                ["DX","O709","Unknown","Perineal laceration during delivery, unspecified"],
                ["DX","O711","Unknown","Rupture of uterus during labor"],
                ["DX","O721","Unknown","Other immediate postpartum hemorrhage"],
                ["DX","O740","Unknown","Aspiration pneumonitis due to anesthesia during labor and delivery"],
                ["DX","O741","Unknown","Other pulmonary complications of anesthesia during labor and delivery"],
                ["DX","O742","Unknown","Cardiac complications of anesthesia during labor and delivery"],
                ["DX","O743","Unknown","Central nervous system complications of anesthesia during labor and delivery"],
                ["DX","O744","Unknown","Toxic reaction to local anesthesia during labor and delivery"],
                ["DX","O745","Unknown","Spinal and epidural anesthesia-induced headache during labor and delivery"],
                ["DX","O746","Unknown","Other complications of spinal and epidural anesthesia during labor and delivery"],
                ["DX","O747","Unknown","Failed or difficult intubation for anesthesia during labor and delivery"],
                ["DX","O748","Unknown","Other complications of anesthesia during labor and delivery"],
                ["DX","O749","Unknown","Complication of anesthesia during labor and delivery, unspecified"],
                ["DX","O750","Unknown","Maternal distress during labor and delivery"],
                ["DX","O752","Unknown","Pyrexia during labor, not elsewhere classified"],
                ["DX","O753","Unknown","Other infection during labor"],
                ["DX","O755","Unknown","Delayed delivery after artificial rupture of membranes"],
                ["DX","O7581","Unknown","Maternal exhaustion complicating labor and delivery"],
                ["DX","O7582","Unknown","Onset (spontaneous) of labor after 37 completed weeks of gestation but before 39 completed weeks gestation, with delivery by (planned) cesarean section"],
                ["DX","O7589","Unknown","Other specified complications of labor and delivery"],
                ["DX","O759","Unknown","Complication of labor and delivery, unspecified"],
                ["DX","O76","Unknown","Abnormality in fetal heart rate and rhythm complicating labor and delivery"],
                ["DX","O770","Unknown","Labor and delivery complicated by meconium in amniotic fluid"],
                ["DX","O771","Unknown","Fetal stress in labor or delivery due to drug administration"],
                ["DX","O778","Unknown","Labor and delivery complicated by other evidence of fetal stress"],
                ["DX","O779","Unknown","Labor and delivery complicated by fetal stress, unspecified"],
                ["DX","O8802","Unknown","Air embolism in childbirth"],
                ["DX","O8812","Unknown","Amniotic fluid embolism in childbirth"],
                ["DX","O8822","Unknown","Thromboembolism in childbirth"],
                ["DX","O8832","Unknown","Pyemic and septic embolism in childbirth"],
                ["DX","O8882","Unknown","Other embolism in childbirth"],
                ["DX","O9802","Unknown","Tuberculosis complicating childbirth"],
                ["DX","O9812","Unknown","Syphilis complicating childbirth"],
                ["DX","O9822","Unknown","Gonorrhea complicating childbirth"],
                ["DX","O9832","Unknown","Other infections with a predominantly sexual mode of transmission complicating childbirth"],
                ["DX","O9842","Unknown","Viral hepatitis complicating childbirth"],
                ["DX","O9852","Unknown","Other viral diseases complicating childbirth"],
                ["DX","O9862","Unknown","Protozoal diseases complicating childbirth"],
                ["DX","O9872","Unknown","Human immunodeficiency virus [HIV] disease complicating childbirth"],
                ["DX","O9882","Unknown","Other maternal infectious and parasitic diseases complicating childbirth"],
                ["DX","O9892","Unknown","Unspecified maternal infectious and parasitic disease complicating childbirth"],
                ["DX","O9902","Unknown","Anemia complicating childbirth"],
                ["DX","O9912","Unknown","Other diseases of the blood and blood-forming organs and certain disorders involving the immune mechanism complicating childbirth"],
                ["DX","O99214","Unknown","Obesity complicating childbirth"],
                ["DX","O99284","Unknown","Endocrine, nutritional and metabolic diseases complicating childbirth"],
                ["DX","O99314","Unknown","Alcohol use complicating childbirth"],
                ["DX","O99324","Unknown","Drug use complicating childbirth"],
                ["DX","O99334","Unknown","Smoking (tobacco) complicating childbirth"],
                ["DX","O99344","Unknown","Other mental disorders complicating childbirth"],
                ["DX","O99354","Unknown","Diseases of the nervous system complicating childbirth"],
                ["DX","O9942","Unknown","Diseases of the circulatory system complicating childbirth"],
                ["DX","O9952","Unknown","Diseases of the respiratory system complicating childbirth"],
                ["DX","O9962","Unknown","Diseases of the digestive system complicating childbirth"],
                ["DX","O9972","Unknown","Diseases of the skin and subcutaneous tissue complicating childbirth"],
                ["DX","O99814","Unknown","Abnormal glucose complicating childbirth"],
                ["DX","O99824","Unknown","Streptococcus B carrier state complicating childbirth"],
                ["DX","O99834","Unknown","Other infection carrier state complicating childbirth"],
                ["DX","O99844","Unknown","Bariatric surgery status complicating childbirth"],
                ["DX","O9A12","Unknown","Malignant neoplasm complicating childbirth"],
                ["DX","O9A22","Unknown","Injury, poisoning and certain other consequences of external causes complicating childbirth"],
                ["DX","O9A32","Unknown","Physical abuse complicating childbirth"],
                ["DX","O9A42","Unknown","Sexual abuse complicating childbirth"],
                ["DX","O9A52","Unknown","Psychological abuse complicating childbirth"],
                ["DX","Z379","Unknown","Outcome of delivery, unspecified"],
                ["DX","Z390","Unknown","Encounter for care and examination of mother immediately after delivery"],
                ["PR","0W8NXZZ","Unknown","Division of Female Perineum, External Approach"],
                ["PR","10D00Z0","Unknown","Extraction of Products of Conception, High, Open Approach"],
                ["PR","10D00Z1","Unknown","Extraction of Products of Conception, Low, Open Approach"],
                ["PR","10D00Z2","Unknown","Extraction of POC, Extraperitoneal, Open Approach"],
                ["PR","10D07Z3","Unknown","Extraction of POC, Low Forceps, Via Opening"],
                ["PR","10D07Z4","Unknown","Extraction of POC, Mid Forceps, Via Opening"],
                ["PR","10D07Z5","Unknown","Extraction of POC, High Forceps, Via Opening"],
                ["PR","10D07Z6","Unknown","Extraction of Products of Conception, Vacuum, Via Opening"],
                ["PR","10D07Z7","Unknown","Extraction of POC, Int Version, Via Opening"],
                ["PR","10D07Z8","Unknown","Extraction of Products of Conception, Other, Via Opening"],
                ["PR","10D17Z9","Unknown","Manual Extraction of Retained POC, Via Opening"],
                ["PR","10D17ZZ","Unknown","Extraction of Products of Conception, Retained, Via Opening"],
                ["PR","10D18Z9","Unknown","Manual Extraction of Products of Conception, Retained, Endo"],
                ["PR","10D18ZZ","Unknown","Extraction of Products of Conception, Retained, Endo"],
                ["PR","10E0XZZ","Unknown","Delivery of Products of Conception, External Approach"],
                ["CPT/HCPCS","1960","Unknown","Anesthesia for vaginal delivery only"],
                ["CPT/HCPCS","1961","Unknown","Anesthesia for cesarean delivery only"],
                ["CPT/HCPCS","1962","Unknown","Anesthesia for urgent hysterectomy following delivery"],
                ["CPT/HCPCS","1963","Unknown","Anesthesia for cesarean hysterectomy"],
                ["CPT/HCPCS","1967","Unknown","Neuraxial labor analgesia/anesthesia for planned vaginal delivery (this includes any repeat subarachnoid needle placement and drug injection and/or any necessary replacement of an epidural catheter during labor)"],
                ["CPT/HCPCS","1968","Unknown","Anesthesia for cesarean delivery following neuraxial labor analgesia/anesthesia (List separately in addition to code for primary procedure performed)"],
                ["CPT/HCPCS","1969","Unknown","Anesthesia for cesarean hysterectomy after neuraxial nerve anesthesia"],
                ["CPT/HCPCS","59050","Unknown","Fetal monitoring during labor by consulting physician (ie, non-attending physician) with written report; supervision and interpretation"],
                ["CPT/HCPCS","59051","Unknown","Fetal monitoring during labor by consulting physician (ie, non-attending physician) with written report; interpretation only"],
                ["CPT/HCPCS","59409","Unknown","Vaginal delivery only (with or without episiotomy and/or forceps);"],
                ["CPT/HCPCS","59410","Unknown","Vaginal delivery only (with or without episiotomy and/or forceps); including postpartum care"],
                ["CPT/HCPCS","59414","Unknown","Delivery of placenta (separate procedure)"],
                ["CPT/HCPCS","59514","Unknown","Cesarean delivery only;"],
                ["CPT/HCPCS","59515","Unknown","Cesarean delivery only; including postpartum care"],
                ["CPT/HCPCS","59525","Unknown","Subtotal or total hysterectomy after cesarean delivery (List separately in addition to code for primary procedure)"],
                ["CPT/HCPCS","59612","Unknown","Vaginal delivery only, after previous cesarean delivery (with or without episiotomy and/or forceps);"],
                ["CPT/HCPCS","59614","Unknown","Vaginal delivery only, after previous cesarean delivery (with or without episiotomy and/or forceps); including postpartum care"],
                ["CPT/HCPCS","59620","Unknown","Cesarean delivery only, following attempted vaginal delivery after previous cesarean delivery;"],
                ["CPT/HCPCS","59622","Unknown","Cesarean delivery only, following attempted vaginal delivery after previous cesarean delivery; including postpartum care"],
                ["CPT/HCPCS","99436","Unknown","Attendance at delivery (when requested by delivering physician) and initial stabilization of newborn"],
                ["CPT/HCPCS","99464","Unknown","Attendance at delivery (when requested by the delivering physician or other qualified health care professional) and initial stabilization of newborn"],
                ["CPT/HCPCS","99465","Unknown","Delivery/birthing room resuscitation, provision of positive pressure ventilation and/or chest compressions in the presence of acute inadequate ventilation and/or cardiac output"],
                ["CPT/HCPCS","G9356","Unknown","Elective delivery or early induction performed"],
                ["DRG","371/766","Unknown","Uncomplicated cesarean section"],
                ["DRG","373/775","Unknown","Uncomplicated vaginal delivery"],
                ["DRG","374/767","Unknown","Uncomplicated vaginal delivery with sterilization and/or dilatation & curettage"],
                ["DRG","370/765","Unknown","Complicated cesarean section"],
                ["DRG","372/774","Unknown","Complicated vaginal delivery"],
                ["DRG","375/768","Unknown","Vaginal delivery with operation room procedure except sterilization and/or dilatation & curettage"],
                ["DX","O0000","C-ECT","Abdominal pregnancy without intrauterine pregnancy"],
                ["DX","O0001","C-ECT","Abdominal pregnancy with intrauterine pregnancy"],
                ["DX","O0010","C-ECT","Tubal pregnancy without intrauterine pregnancy"],
                ["DX","O0011","C-ECT","Tubal pregnancy with intrauterine pregnancy"],
                ["DX","O0020","C-ECT","Ovarian pregnancy without intrauterine pregnancy"],
                ["DX","O0021","C-ECT","Ovarian pregnancy with intrauterine pregnancy"],
                ["DX","O0080","C-ECT","Other ectopic pregnancy without intrauterine pregnancy"],
                ["DX","O0081","C-ECT","Other ectopic pregnancy with intrauterine pregnancy"],
                ["DX","O0090","C-ECT","Unspecified ectopic pregnancy without intrauterine pregnancy"],
                ["DX","O0091","C-ECT","Unspecified ectopic pregnancy with intrauterine pregnancy"],
                ["DX","O00101","C-ECT","Right tubal pregnancy without intrauterine pregnancy"],
                ["DX","O00102","C-ECT","Left tubal pregnancy without intrauterine pregnancy"],
                ["DX","O00109","C-ECT","Unspecified tubal pregnancy without intrauterine pregnancy"],
                ["DX","O00111","C-ECT","Right tubal pregnancy with intrauterine pregnancy"],
                ["DX","O00112","C-ECT","Left tubal pregnancy with intrauterine pregnancy"],
                ["DX","O00119","C-ECT","Unspecified tubal pregnancy with intrauterine pregnancy"],
                ["DX","O002","C-ECT","Ovarian pregnancy"],
                ["DX","O00201","C-ECT","Right ovarian pregnancy without intrauterine pregnancy"],
                ["DX","O00202","C-ECT","Left ovarian pregnancy without intrauterine pregnancy"],
                ["DX","O00209","C-ECT","Unspecified ovarian pregnancy without intrauterine pregnancy"],
                ["DX","O00211","C-ECT","Right ovarian pregnancy with intrauterine pregnancy"],
                ["DX","O00212","C-ECT","Left ovarian pregnancy with intrauterine pregnancy"],
                ["DX","O00219","C-ECT","Unspecified ovarian pregnancy with intrauterine pregnancy"],
                ["DX","O008","C-ECT","Other ectopic pregnancy"],
                ["DX","O009","C-ECT","Ectopic pregnancy, unspecified"]


                       
            ]

columns = ["Type_of_Code","code","Pregnancy_Outcome","Description"]


# COMMAND ----------


#spark_outcome_codes = SparkSession.builder.appName('sparkdf').getOrCreate()
spark_outcome_codes_sdf = spark.createDataFrame(outcomeCodes, columns) 
display(spark_outcome_codes_sdf)

# COMMAND ----------

# MAGIC %md Saving a table for query that can be accessed by other analyst

# COMMAND ----------


outcome_dict_df=spark_outcome_codes_sdf.groupBy("Pregnancy_Outcome").agg( F.collect_list("code").alias("code") ).toPandas()


# COMMAND ----------

print(outcome_dict_df.to_dict('records'))

# COMMAND ----------

#spark.sql("""DROP TABLE IF EXISTS cdh_hv_mother_baby_covid_exploratory.deliveryCodes_run9""")
#spark.sql("""DROP TABLE IF EXISTS cdh_hv_mother_baby_covid_exploratory.outcomeCodes_run9""")
#spark_outcome_codes_sdf.write.mode("overwrite").saveAsTable("cdh_hv_mother_baby_covid_exploratory.outcomeCodes_run9")

# COMMAND ----------

# MAGIC %md # Endpoints codes

# COMMAND ----------

# MAGIC %md There is a set of codes used for delivery episodes, this is known as a pregnancy endpoint. This enpoints can have a LB,SB,ECT,AB and Unknown outcomes. Therefore, the first step is to identify the delivery and then look at the outcome codes around the vecity of the endpoint to capture the outcome of the delivery. These endpoint codes are as follows

# COMMAND ----------

# MAGIC %md 
# MAGIC 1. CD-10-CM: O75.82, O80, Z37.0, Z37.1, Z37.9
# MAGIC
# MAGIC 2. ICD-10-PCS: 10D00Z0, 10D00Z1, 10D00Z2, 10D07Z3, 10D07Z4, 10D07Z5, 10D07Z6, 10D07Z7, 10D07Z8, 10E0XZZ
# MAGIC
# MAGIC 3. DRG:  765, 766, 767,  768, 774, 775, 783 , 784, 785, 786, 787, 788, 796, 797, 798, 805, 806, 807

# COMMAND ----------

# MAGIC %md # Look up table - Estimating lookback period

# COMMAND ----------

# MAGIC %md CPT, ICD10 diag, ICD procedure (outcome oriented codes)

# COMMAND ----------

LookUpCodes = [
    
    
["DX","O368310","Maternal care for abnormalities of the fetal heart rate or rhythm, first trimester, not applicable or unspecified","42"],
["DX","O368311","Maternal care for abnormalities of the fetal heart rate or rhythm, first trimester, fetus 1","42"],
["DX","O368312","Maternal care for abnormalities of the fetal heart rate or rhythm, first trimester, fetus 2","42"],
["DX","O368313","Maternal care for abnormalities of the fetal heart rate or rhythm, first trimester, fetus 3","42"],
["DX","O368314","Maternal care for abnormalities of the fetal heart rate or rhythm, first trimester, fetus 4","42"],
["DX","O368315","Maternal care for abnormalities of the fetal heart rate or rhythm, first trimester, fetus 5","42"],
["DX","O42011","Preterm premature rupture of membranes, onset of labor within 24 hours of rupture, first trimester","42"],
["DX","O42111","Preterm premature rupture of membranes, onset of labor more than 24 hours following rupture, first trimester","42"],
["DX","O42911","Preterm premature rupture of membranes, unspecified as to length of time between rupture and onset of labor, first trimester","42"],
["PX","76801","Ultrasound, pregnant uterus, real time with image documentation, fetal and maternal evaluation, first trimester (< 14 weeks 0 days), transabdominal approach; single or first gestation","42"],
["PX","76802","Ultrasound, pregnant uterus, real time with image documentation, fetal and maternal evaluation, first trimester (< 14 weeks 0 days), transabdominal approach; each additional gestation (List separately in addition to code for primary procedure)","42"],
["DX","O0901","Supervision of pregnancy with history of infertility, first trimester","42"],
["DX","O0911","Supervision of pregnancy with history of ectopic pregnancy, first trimester","42"],
["DX","O09211","Supervision of pregnancy with history of pre-term labor, first trimester","42"],
["DX","O09291","Supervision of pregnancy with other poor reproductive or obstetric history, first trimester","42"],
["DX","O0931","Supervision of pregnancy with insufficient antenatal care, first trimester","42"],
["DX","O0941","Supervision of pregnancy with grand multiparity, first trimester","42"],
["DX","O09511","Supervision of elderly primigravida, first trimester","42"],
["DX","O09521","Supervision of elderly multigravida, first trimester","42"],
["DX","O09611","Supervision of young primigravida, first trimester","42"],
["DX","O09621","Supervision of young multigravida, first trimester","42"],
["DX","O0971","Supervision of high risk pregnancy due to social problems, first trimester","42"],
["DX","O09811","Supervision of pregnancy resulting from assisted reproductive technology, first trimester","42"],
["DX","O09821","Supervision of pregnancy with history of in utero procedure during previous pregnancy, first trimester","42"],
["DX","O09891","Supervision of other high risk pregnancies, first trimester","42"],
["DX","O0991","Supervision of high risk pregnancy, unspecified, first trimester","42"],
["DX","O09A1","Supervision of pregnancy with history of molar pregnancy, first trimester","42"],
["DX","O10011","Pre-existing essential hypertension complicating pregnancy, first trimester","42"],
["DX","O10111","Pre-existing hypertensive heart disease complicating pregnancy, first trimester","42"],
["DX","O10211","Pre-existing hypertensive chronic kidney disease complicating pregnancy, first trimester","42"],
["DX","O10311","Pre-existing hypertensive heart and chronic kidney disease complicating pregnancy, first trimester","42"],
["DX","O10411","Pre-existing secondary hypertension complicating pregnancy, first trimester","42"],
["DX","O10911","Unspecified pre-existing hypertension complicating pregnancy, first trimester","42"],
["DX","O111","Pre-existing hypertension with pre-eclampsia, first trimester","42"],
["DX","O1201","Gestational edema, first trimester","42"],
["DX","O1211","Gestational proteinuria, first trimester","42"],
["DX","O1221","Gestational edema with proteinuria, first trimester","42"],
["DX","O131","Gestational [pregnancy-induced] hypertension without significant proteinuria, first trimester","42"],
["DX","O161","Unspecified maternal hypertension, first trimester","42"],
["DX","O2201","Varicose veins of lower extremity in pregnancy, first trimester","42"],
["DX","O2211","Genital varices in pregnancy, first trimester","42"],
["DX","O2221","Superficial thrombophlebitis in pregnancy, first trimester","42"],
["DX","O2231","Deep phlebothrombosis in pregnancy, first trimester","42"],
["DX","O2241","Hemorrhoids in pregnancy, first trimester","42"],
["DX","O2251","Cerebral venous thrombosis in pregnancy, first trimester","42"],
["DX","O228X1","Other venous complications in pregnancy, first trimester","42"],
["DX","O2291","Venous complication in pregnancy, unspecified, first trimester","42"],
["DX","O2301","Infections of kidney in pregnancy, first trimester","42"],
["DX","O2311","Infections of bladder in pregnancy, first trimester","42"],
["DX","O2321","Infections of urethra in pregnancy, first trimester","42"],
["DX","O2331","Infections of other parts of urinary tract in pregnancy, first trimester","42"],
["DX","O2341","Unspecified infection of urinary tract in pregnancy, first trimester","42"],
["DX","O23511","Infections of cervix in pregnancy, first trimester","42"],
["DX","O23521","Salpingo-oophoritis in pregnancy, first trimester","42"],
["DX","O23591","Infection of other part of genital tract in pregnancy, first trimester","42"],
["DX","O2391","Unspecified genitourinary tract infection in pregnancy, first trimester","42"],
["DX","O24011","Pre-existing type 1 diabetes mellitus, in pregnancy, first trimester","42"],
["DX","O24111","Pre-existing type 2 diabetes mellitus, in pregnancy, first trimester","42"],
["DX","O24311","Unspecified pre-existing diabetes mellitus in pregnancy, first trimester","42"],
["DX","O24811","Other pre-existing diabetes mellitus in pregnancy, first trimester","42"],
["DX","O24911","Unspecified diabetes mellitus in pregnancy, first trimester","42"],
["DX","O2511","Malnutrition in pregnancy, first trimester","42"],
["DX","O2601","Excessive weight gain in pregnancy, first trimester","42"],
["DX","O2611","Low weight gain in pregnancy, first trimester","42"],
["DX","O2621","Pregnancy care for patient with recurrent pregnancy loss, first trimester","42"],
["DX","O2631","Retained intrauterine contraceptive device in pregnancy, first trimester","42"],
["DX","O2641","Herpes gestationis, first trimester","42"],
["DX","O2651","Maternal hypotension syndrome, first trimester","42"],
["DX","O26611","Liver and biliary tract disorders in pregnancy, first trimester","42"],
["DX","O26711","Subluxation of symphysis (pubis) in pregnancy, first trimester","42"],
["DX","O26811","Pregnancy related exhaustion and fatigue, first trimester","42"],
["DX","O26821","Pregnancy related peripheral neuritis, first trimester","42"],
["DX","O26831","Pregnancy related renal disease, first trimester","42"],
["DX","O26841","Uterine size-date discrepancy, first trimester","42"],
["DX","O26851","Spotting complicating pregnancy, first trimester","42"],
["DX","O26891","Other specified pregnancy related conditions, first trimester","42"],
["DX","O2691","Pregnancy related conditions, unspecified, first trimester","42"],
["DX","O29011","Aspiration pneumonitis due to anesthesia during pregnancy, first trimester","42"],
["DX","O29021","Pressure collapse of lung due to anesthesia during pregnancy, first trimester","42"],
["DX","O29091","Other pulmonary complications of anesthesia during pregnancy, first trimester","42"],
["DX","O29111","Cardiac arrest due to anesthesia during pregnancy, first trimester","42"],
["DX","O29121","Cardiac failure due to anesthesia during pregnancy, first trimester","42"],
["DX","O29191","Other cardiac complications of anesthesia during pregnancy, first trimester","42"],
["DX","O29211","Cerebral anoxia due to anesthesia during pregnancy, first trimester","42"],
["DX","O29291","Other central nervous system complications of anesthesia during pregnancy, first trimester","42"],
["DX","O293X1","Toxic reaction to local anesthesia during pregnancy, first trimester","42"],
["DX","O2941","Spinal and epidural anesthesia induced headache during pregnancy, first trimester","42"],
["DX","O295X1","Other complications of spinal and epidural anesthesia during pregnancy, first trimester","42"],
["DX","O2961","Failed or difficult intubation for anesthesia during pregnancy, first trimester","42"],
["DX","O298X1","Other complications of anesthesia during pregnancy, first trimester","42"],
["DX","O2991","Unspecified complication of anesthesia during pregnancy, first trimester","42"],
["DX","O30001","Twin pregnancy, unspecified number of placenta and unspecified number of amniotic sacs, first trimester","42"],
["DX","O30011","Twin pregnancy, monochorionic/monoamniotic, first trimester","42"],
["DX","O30021","Conjoined twin pregnancy, first trimester","42"],
["DX","O30031","Twin pregnancy, monochorionic/diamniotic, first trimester","42"],
["DX","O30041","Twin pregnancy, dichorionic/diamniotic, first trimester","42"],
["DX","O30091","Twin pregnancy, unable to determine number of placenta and number of amniotic sacs, first trimester","42"],
["DX","O30101","Triplet pregnancy, unspecified number of placenta and unspecified number of amniotic sacs, first trimester","42"],
["DX","O30111","Triplet pregnancy with two or more monochorionic fetuses, first trimester","42"],
["DX","O30121","Triplet pregnancy with two or more monoamniotic fetuses, first trimester","42"],
["DX","O30131","Triplet pregnancy, trichorionic/triamniotic, first trimester","42"],
["DX","O30191","Triplet pregnancy, unable to determine number of placenta and number of amniotic sacs, first trimester","42"],
["DX","O30201","Quadruplet pregnancy, unspecified number of placenta and unspecified number of amniotic sacs, first trimester","42"],
["DX","O30211","Quadruplet pregnancy with two or more monochorionic fetuses, first trimester","42"],
["DX","O30221","Quadruplet pregnancy with two or more monoamniotic fetuses, first trimester","42"],
["DX","O30231","Quadruplet pregnancy, quadrachorionic/quadra-amniotic, first trimester","42"],
["DX","O30291","Quadruplet pregnancy, unable to determine number of placenta and number of amniotic sacs, first trimester","42"],
["DX","O30801","Other specified multiple gestation, unspecified number of placenta and unspecified number of amniotic sacs, first trimester","42"],
["DX","O30811","Other specified multiple gestation with two or more monochorionic fetuses, first trimester","42"],
["DX","O30821","Other specified multiple gestation with two or more monoamniotic fetuses, first trimester","42"],
["DX","O30831","Other specified multiple gestation, number of chorions and amnions are both equal to the number of fetuses, first trimester","42"],
["DX","O30891","Other specified multiple gestation, unable to determine number of placenta and number of amniotic sacs, first trimester","42"],
["DX","O3091","Multiple gestation, unspecified, first trimester","42"],
["DX","O3101X0","Papyraceous fetus, first trimester, not applicable or unspecified","42"],
["DX","O3101X1","Papyraceous fetus, first trimester, fetus 1","42"],
["DX","O3101X2","Papyraceous fetus, first trimester, fetus 2","42"],
["DX","O3101X3","Papyraceous fetus, first trimester, fetus 3","42"],
["DX","O3101X4","Papyraceous fetus, first trimester, fetus 4","42"],
["DX","O3101X5","Papyraceous fetus, first trimester, fetus 5","42"],
["DX","O3101X9","Papyraceous fetus, first trimester, other fetus","42"],
["DX","O3111X0","Continuing pregnancy after spontaneous abortion of one fetus or more, first trimester, not applicable or unspecified","42"],
["DX","O3111X1","Continuing pregnancy after spontaneous abortion of one fetus or more, first trimester, fetus 1","42"],
["DX","O3111X2","Continuing pregnancy after spontaneous abortion of one fetus or more, first trimester, fetus 2","42"],
["DX","O3111X3","Continuing pregnancy after spontaneous abortion of one fetus or more, first trimester, fetus 3","42"],
["DX","O3111X4","Continuing pregnancy after spontaneous abortion of one fetus or more, first trimester, fetus 4","42"],
["DX","O3111X5","Continuing pregnancy after spontaneous abortion of one fetus or more, first trimester, fetus 5","42"],
["DX","O3111X9","Continuing pregnancy after spontaneous abortion of one fetus or more, first trimester, other fetus","42"],
["DX","O3121X0","Continuing pregnancy after intrauterine death of one fetus or more, first trimester, not applicable or unspecified","42"],
["DX","O3121X1","Continuing pregnancy after intrauterine death of one fetus or more, first trimester, fetus 1","42"],
["DX","O3121X2","Continuing pregnancy after intrauterine death of one fetus or more, first trimester, fetus 2","42"],
["DX","O3121X3","Continuing pregnancy after intrauterine death of one fetus or more, first trimester, fetus 3","42"],
["DX","O3121X4","Continuing pregnancy after intrauterine death of one fetus or more, first trimester, fetus 4","42"],
["DX","O3121X5","Continuing pregnancy after intrauterine death of one fetus or more, first trimester, fetus 5","42"],
["DX","O3121X9","Continuing pregnancy after intrauterine death of one fetus or more, first trimester, other fetus","42"],
["DX","O3131X0","Continuing pregnancy after elective fetal reduction of one fetus or more, first trimester, not applicable or unspecified","42"],
["DX","O3131X1","Continuing pregnancy after elective fetal reduction of one fetus or more, first trimester, fetus 1","42"],
["DX","O3131X2","Continuing pregnancy after elective fetal reduction of one fetus or more, first trimester, fetus 2","42"],
["DX","O3131X3","Continuing pregnancy after elective fetal reduction of one fetus or more, first trimester, fetus 3","42"],
["DX","O3131X4","Continuing pregnancy after elective fetal reduction of one fetus or more, first trimester, fetus 4","42"],
["DX","O3131X5","Continuing pregnancy after elective fetal reduction of one fetus or more, first trimester, fetus 5","42"],
["DX","O3131X9","Continuing pregnancy after elective fetal reduction of one fetus or more, first trimester, other fetus","42"],
["DX","O318X10","Other complications specific to multiple gestation, first trimester, not applicable or unspecified","42"],
["DX","O318X11","Other complications specific to multiple gestation, first trimester, fetus 1","42"],
["DX","O318X12","Other complications specific to multiple gestation, first trimester, fetus 2","42"],
["DX","O318X13","Other complications specific to multiple gestation, first trimester, fetus 3","42"],
["DX","O318X14","Other complications specific to multiple gestation, first trimester, fetus 4","42"],
["DX","O318X15","Other complications specific to multiple gestation, first trimester, fetus 5","42"],
["DX","O318X19","Other complications specific to multiple gestation, first trimester, other fetus","42"],
["DX","O3401","Maternal care for unspecified congenital malformation of uterus, first trimester","42"],
["DX","O3411","Maternal care for benign tumor of corpus uteri, first trimester","42"],
["DX","O3431","Maternal care for cervical incompetence, first trimester","42"],
["DX","O3441","Maternal care for other abnormalities of cervix, first trimester","42"],
["DX","O34511","Maternal care for incarceration of gravid uterus, first trimester","42"],
["DX","O34521","Maternal care for prolapse of gravid uterus, first trimester","42"],
["DX","O34531","Maternal care for retroversion of gravid uterus, first trimester","42"],
["DX","O34591","Maternal care for other abnormalities of gravid uterus, first trimester","42"],
["DX","O3461","Maternal care for abnormality of vagina, first trimester","42"],
["DX","O3471","Maternal care for abnormality of vulva and perineum, first trimester","42"],
["DX","O3481","Maternal care for other abnormalities of pelvic organs, first trimester","42"],
["DX","O3491","Maternal care for abnormality of pelvic organ, unspecified, first trimester","42"],
["DX","O360110","Maternal care for anti-D [Rh] antibodies, first trimester, not applicable or unspecified","42"],
["DX","O360111","Maternal care for anti-D [Rh] antibodies, first trimester, fetus 1","42"],
["DX","O360112","Maternal care for anti-D [Rh] antibodies, first trimester, fetus 2","42"],
["DX","O360113","Maternal care for anti-D [Rh] antibodies, first trimester, fetus 3","42"],
["DX","O360114","Maternal care for anti-D [Rh] antibodies, first trimester, fetus 4","42"],
["DX","O360115","Maternal care for anti-D [Rh] antibodies, first trimester, fetus 5","42"],
["DX","O360119","Maternal care for anti-D [Rh] antibodies, first trimester, other fetus","42"],
["DX","O360910","Maternal care for other rhesus isoimmunization, first trimester, not applicable or unspecified","42"],
["DX","O360911","Maternal care for other rhesus isoimmunization, first trimester, fetus 1","42"],
["DX","O360912","Maternal care for other rhesus isoimmunization, first trimester, fetus 2","42"],
["DX","O360913","Maternal care for other rhesus isoimmunization, first trimester, fetus 3","42"],
["DX","O360914","Maternal care for other rhesus isoimmunization, first trimester, fetus 4","42"],
["DX","O360915","Maternal care for other rhesus isoimmunization, first trimester, fetus 5","42"],
["DX","O360919","Maternal care for other rhesus isoimmunization, first trimester, other fetus","42"],
["DX","O361110","Maternal care for Anti-A sensitization, first trimester, not applicable or unspecified","42"],
["DX","O361111","Maternal care for Anti-A sensitization, first trimester, fetus 1","42"],
["DX","O361112","Maternal care for Anti-A sensitization, first trimester, fetus 2","42"],
["DX","O361113","Maternal care for Anti-A sensitization, first trimester, fetus 3","42"],
["DX","O361114","Maternal care for Anti-A sensitization, first trimester, fetus 4","42"],
["DX","O361115","Maternal care for Anti-A sensitization, first trimester, fetus 5","42"],
["DX","O361119","Maternal care for Anti-A sensitization, first trimester, other fetus","42"],
["DX","O361910","Maternal care for other isoimmunization, first trimester, not applicable or unspecified","42"],
["DX","O361911","Maternal care for other isoimmunization, first trimester, fetus 1","42"],
["DX","O361912","Maternal care for other isoimmunization, first trimester, fetus 2","42"],
["DX","O361913","Maternal care for other isoimmunization, first trimester, fetus 3","42"],
["DX","O361914","Maternal care for other isoimmunization, first trimester, fetus 4","42"],
["DX","O361915","Maternal care for other isoimmunization, first trimester, fetus 5","42"],
["DX","O361919","Maternal care for other isoimmunization, first trimester, other fetus","42"],
["DX","O3621X0","Maternal care for hydrops fetalis, first trimester, not applicable or unspecified","42"],
["DX","O3621X1","Maternal care for hydrops fetalis, first trimester, fetus 1","42"],
["DX","O3621X2","Maternal care for hydrops fetalis, first trimester, fetus 2","42"],
["DX","O3621X3","Maternal care for hydrops fetalis, first trimester, fetus 3","42"],
["DX","O3621X4","Maternal care for hydrops fetalis, first trimester, fetus 4","42"],
["DX","O3621X5","Maternal care for hydrops fetalis, first trimester, fetus 5","42"],
["DX","O3621X9","Maternal care for hydrops fetalis, first trimester, other fetus","42"],
["DX","O365110","Maternal care for known or suspected placental insufficiency, first trimester, not applicable or unspecified","42"],
["DX","O365111","Maternal care for known or suspected placental insufficiency, first trimester, fetus 1","42"],
["DX","O365112","Maternal care for known or suspected placental insufficiency, first trimester, fetus 2","42"],
["DX","O365113","Maternal care for known or suspected placental insufficiency, first trimester, fetus 3","42"],
["DX","O365114","Maternal care for known or suspected placental insufficiency, first trimester, fetus 4","42"],
["DX","O365115","Maternal care for known or suspected placental insufficiency, first trimester, fetus 5","42"],
["DX","O365119","Maternal care for known or suspected placental insufficiency, first trimester, other fetus","42"],
["DX","O365910","Maternal care for other known or suspected poor fetal growth, first trimester, not applicable or unspecified","42"],
["DX","O365911","Maternal care for other known or suspected poor fetal growth, first trimester, fetus 1","42"],
["DX","O365912","Maternal care for other known or suspected poor fetal growth, first trimester, fetus 2","42"],
["DX","O365913","Maternal care for other known or suspected poor fetal growth, first trimester, fetus 3","42"],
["DX","O365914","Maternal care for other known or suspected poor fetal growth, first trimester, fetus 4","42"],
["DX","O365915","Maternal care for other known or suspected poor fetal growth, first trimester, fetus 5","42"],
["DX","O365919","Maternal care for other known or suspected poor fetal growth, first trimester, other fetus","42"],
["DX","O3661X0","Maternal care for excessive fetal growth, first trimester, not applicable or unspecified","42"],
["DX","O3661X1","Maternal care for excessive fetal growth, first trimester, fetus 1","42"],
["DX","O3661X2","Maternal care for excessive fetal growth, first trimester, fetus 2","42"],
["DX","O3661X3","Maternal care for excessive fetal growth, first trimester, fetus 3","42"],
["DX","O3661X4","Maternal care for excessive fetal growth, first trimester, fetus 4","42"],
["DX","O3661X5","Maternal care for excessive fetal growth, first trimester, fetus 5","42"],
["DX","O3661X9","Maternal care for excessive fetal growth, first trimester, other fetus","42"],
["DX","O3671X0","Maternal care for viable fetus in abdominal pregnancy, first trimester, not applicable or unspecified","42"],
["DX","O3671X1","Maternal care for viable fetus in abdominal pregnancy, first trimester, fetus 1","42"],
["DX","O3671X2","Maternal care for viable fetus in abdominal pregnancy, first trimester, fetus 2","42"],
["DX","O3671X3","Maternal care for viable fetus in abdominal pregnancy, first trimester, fetus 3","42"],
["DX","O3671X4","Maternal care for viable fetus in abdominal pregnancy, first trimester, fetus 4","42"],
["DX","O3671X5","Maternal care for viable fetus in abdominal pregnancy, first trimester, fetus 5","42"],
["DX","O3671X9","Maternal care for viable fetus in abdominal pregnancy, first trimester, other fetus","42"],
["DX","O368210","Fetal anemia and thrombocytopenia, first trimester, not applicable or unspecified","42"],
["DX","O368211","Fetal anemia and thrombocytopenia, first trimester, fetus 1","42"],
["DX","O368212","Fetal anemia and thrombocytopenia, first trimester, fetus 2","42"],
["DX","O368213","Fetal anemia and thrombocytopenia, first trimester, fetus 3","42"],
["DX","O368214","Fetal anemia and thrombocytopenia, first trimester, fetus 4","42"],
["DX","O368215","Fetal anemia and thrombocytopenia, first trimester, fetus 5","42"],
["DX","O368219","Fetal anemia and thrombocytopenia, first trimester, other fetus","42"],
["DX","O368910","Maternal care for other specified fetal problems, first trimester, not applicable or unspecified","42"],
["DX","O368911","Maternal care for other specified fetal problems, first trimester, fetus 1","42"],
["DX","O368912","Maternal care for other specified fetal problems, first trimester, fetus 2","42"],
["DX","O368913","Maternal care for other specified fetal problems, first trimester, fetus 3","42"],
["DX","O368914","Maternal care for other specified fetal problems, first trimester, fetus 4","42"],
["DX","O368915","Maternal care for other specified fetal problems, first trimester, fetus 5","42"],
["DX","O368919","Maternal care for other specified fetal problems, first trimester, other fetus","42"],
["DX","O3691X0","Maternal care for fetal problem, unspecified, first trimester, not applicable or unspecified","42"],
["DX","O3691X1","Maternal care for fetal problem, unspecified, first trimester, fetus 1","42"],
["DX","O3691X2","Maternal care for fetal problem, unspecified, first trimester, fetus 2","42"],
["DX","O3691X3","Maternal care for fetal problem, unspecified, first trimester, fetus 3","42"],
["DX","O3691X4","Maternal care for fetal problem, unspecified, first trimester, fetus 4","42"],
["DX","O3691X5","Maternal care for fetal problem, unspecified, first trimester, fetus 5","42"],
["DX","O3691X9","Maternal care for fetal problem, unspecified, first trimester, other fetus","42"],
["DX","O401XX0","Polyhydramnios, first trimester, not applicable or unspecified","42"],
["DX","O401XX1","Polyhydramnios, first trimester, fetus 1","42"],
["DX","O401XX2","Polyhydramnios, first trimester, fetus 2","42"],
["DX","O401XX3","Polyhydramnios, first trimester, fetus 3","42"],
["DX","O401XX4","Polyhydramnios, first trimester, fetus 4","42"],
["DX","O401XX5","Polyhydramnios, first trimester, fetus 5","42"],
["DX","O401XX9","Polyhydramnios, first trimester, other fetus","42"],
["DX","O4101X0","Oligohydramnios, first trimester, not applicable or unspecified","42"],
["DX","O4101X1","Oligohydramnios, first trimester, fetus 1","42"],
["DX","O4101X2","Oligohydramnios, first trimester, fetus 2","42"],
["DX","O4101X3","Oligohydramnios, first trimester, fetus 3","42"],
["DX","O4101X4","Oligohydramnios, first trimester, fetus 4","42"],
["DX","O4101X5","Oligohydramnios, first trimester, fetus 5","42"],
["DX","O4101X9","Oligohydramnios, first trimester, other fetus","42"],
["DX","O411010","Infection of amniotic sac and membranes, unspecified, first trimester, not applicable or unspecified","42"],
["DX","O411011","Infection of amniotic sac and membranes, unspecified, first trimester, fetus 1","42"],
["DX","O411012","Infection of amniotic sac and membranes, unspecified, first trimester, fetus 2","42"],
["DX","O411013","Infection of amniotic sac and membranes, unspecified, first trimester, fetus 3","42"],
["DX","O411014","Infection of amniotic sac and membranes, unspecified, first trimester, fetus 4","42"],
["DX","O411015","Infection of amniotic sac and membranes, unspecified, first trimester, fetus 5","42"],
["DX","O411019","Infection of amniotic sac and membranes, unspecified, first trimester, other fetus","42"],
["DX","O411210","Chorioamnionitis, first trimester, not applicable or unspecified","42"],
["DX","O411211","Chorioamnionitis, first trimester, fetus 1","42"],
["DX","O411212","Chorioamnionitis, first trimester, fetus 2","42"],
["DX","O411213","Chorioamnionitis, first trimester, fetus 3","42"],
["DX","O411214","Chorioamnionitis, first trimester, fetus 4","42"],
["DX","O411215","Chorioamnionitis, first trimester, fetus 5","42"],
["DX","O411219","Chorioamnionitis, first trimester, other fetus","42"],
["DX","O411410","Placentitis, first trimester, not applicable or unspecified","42"],
["DX","O411411","Placentitis, first trimester, fetus 1","42"],
["DX","O411412","Placentitis, first trimester, fetus 2","42"],
["DX","O411413","Placentitis, first trimester, fetus 3","42"],
["DX","O411414","Placentitis, first trimester, fetus 4","42"],
["DX","O411415","Placentitis, first trimester, fetus 5","42"],
["DX","O411419","Placentitis, first trimester, other fetus","42"],
["DX","O418X10","Other specified disorders of amniotic fluid and membranes, first trimester, not applicable or unspecified","42"],
["DX","O418X11","Other specified disorders of amniotic fluid and membranes, first trimester, fetus 1","42"],
["DX","O418X12","Other specified disorders of amniotic fluid and membranes, first trimester, fetus 2","42"],
["DX","O418X13","Other specified disorders of amniotic fluid and membranes, first trimester, fetus 3","42"],
["DX","O418X14","Other specified disorders of amniotic fluid and membranes, first trimester, fetus 4","42"],
["DX","O418X15","Other specified disorders of amniotic fluid and membranes, first trimester, fetus 5","42"],
["DX","O418X19","Other specified disorders of amniotic fluid and membranes, first trimester, other fetus","42"],
["DX","O4191X0","Disorder of amniotic fluid and membranes, unspecified, first trimester, not applicable or unspecified","42"],
["DX","O4191X1","Disorder of amniotic fluid and membranes, unspecified, first trimester, fetus 1","42"],
["DX","O4191X2","Disorder of amniotic fluid and membranes, unspecified, first trimester, fetus 2","42"],
["DX","O4191X3","Disorder of amniotic fluid and membranes, unspecified, first trimester, fetus 3","42"],
["DX","O4191X4","Disorder of amniotic fluid and membranes, unspecified, first trimester, fetus 4","42"],
["DX","O4191X5","Disorder of amniotic fluid and membranes, unspecified, first trimester, fetus 5","42"],
["DX","O4191X9","Disorder of amniotic fluid and membranes, unspecified, first trimester, other fetus","42"],
["DX","O43011","Fetomaternal placental transfusion syndrome, first trimester","42"],
["DX","O43021","Fetus-to-fetus placental transfusion syndrome, first trimester","42"],
["DX","O43101","Malformation of placenta, unspecified, first trimester","42"],
["DX","O43111","Circumvallate placenta, first trimester","42"],
["DX","O43121","Velamentous insertion of umbilical cord, first trimester","42"],
["DX","O43191","Other malformation of placenta, first trimester","42"],
["DX","O43211","Placenta accreta, first trimester","42"],
["DX","O43221","Placenta increta, first trimester","42"],
["DX","O43231","Placenta percreta, first trimester","42"],
["DX","O43811","Placental infarction, first trimester","42"],
["DX","O43891","Other placental disorders, first trimester","42"],
["DX","O4391","Unspecified placental disorder, first trimester","42"],
["DX","O4401","Complete placenta previa NOS or without hemorrhage, first trimester","42"],
["DX","O4411","Complete placenta previa with hemorrhage, first trimester","42"],
["DX","O4421","Partial placenta previa NOS or without hemorrhage, first trimester","42"],
["DX","O4431","Partial placenta previa with hemorrhage, first trimester","42"],
["DX","O4441","Low lying placenta NOS or without hemorrhage, first trimester","42"],
["DX","O4451","Low lying placenta with hemorrhage, first trimester","42"],
["DX","O45001","Premature separation of placenta with coagulation defect, unspecified, first trimester","42"],
["DX","O45011","Premature separation of placenta with afibrinogenemia, first trimester","42"],
["DX","O45021","Premature separation of placenta with disseminated intravascular coagulation, first trimester","42"],
["DX","O45091","Premature separation of placenta with other coagulation defect, first trimester","42"],
["DX","O458X1","Other premature separation of placenta, first trimester","42"],
["DX","O4591","Premature separation of placenta, unspecified, first trimester","42"],
["DX","O46001","Antepartum hemorrhage with coagulation defect, unspecified, first trimester","42"],
["DX","O46011","Antepartum hemorrhage with afibrinogenemia, first trimester","42"],
["DX","O46021","Antepartum hemorrhage with disseminated intravascular coagulation, first trimester","42"],
["DX","O46091","Antepartum hemorrhage with other coagulation defect, first trimester","42"],
["DX","O468X1","Other antepartum hemorrhage, first trimester","42"],
["DX","O4691","Antepartum hemorrhage, unspecified, first trimester","42"],
["DX","O88011","Air embolism in pregnancy, first trimester","42"],
["DX","O88111","Amniotic fluid embolism in pregnancy, first trimester","42"],
["DX","O88211","Thromboembolism in pregnancy, first trimester","42"],
["DX","O88311","Pyemic and septic embolism in pregnancy, first trimester","42"],
["DX","O88811","Other embolism in pregnancy, first trimester","42"],
["DX","O91011","Infection of nipple associated with pregnancy, first trimester","42"],
["DX","O91111","Abscess of breast associated with pregnancy, first trimester","42"],
["DX","O91211","Nonpurulent mastitis associated with pregnancy, first trimester","42"],
["DX","O92011","Retracted nipple associated with pregnancy, first trimester","42"],
["DX","O92111","Cracked nipple associated with pregnancy, first trimester","42"],
["DX","O98011","Tuberculosis complicating pregnancy, first trimester","42"],
["DX","O98111","Syphilis complicating pregnancy, first trimester","42"],
["DX","O98211","Gonorrhea complicating pregnancy, first trimester","42"],
["DX","O98311","Other infections with a predominantly sexual mode of transmission complicating pregnancy, first trimester","42"],
["DX","O98411","Viral hepatitis complicating pregnancy, first trimester","42"],
["DX","O98511","Other viral diseases complicating pregnancy, first trimester","42"],
["DX","O98611","Protozoal diseases complicating pregnancy, first trimester","42"],
["DX","O98711","Human immunodeficiency virus [HIV] disease complicating pregnancy, first trimester","42"],
["DX","O98811","Other maternal infectious and parasitic diseases complicating pregnancy, first trimester","42"],
["DX","O98911","Unspecified maternal infectious and parasitic disease complicating pregnancy, first trimester","42"],
["DX","O99011","Anemia complicating pregnancy, first trimester","42"],
["DX","O99111","Other diseases of the blood and blood-forming organs and certain disorders involving the immune mechanism complicating pregnancy, first trimester","42"],
["DX","O99211","Obesity complicating pregnancy, first trimester","42"],
["DX","O99281","Endocrine, nutritional and metabolic diseases complicating pregnancy, first trimester","42"],
["DX","O99311","Alcohol use complicating pregnancy, first trimester","42"],
["DX","O99321","Drug use complicating pregnancy, first trimester","42"],
["DX","O99331","Smoking (tobacco) complicating pregnancy, first trimester","42"],
["DX","O99341","Other mental disorders complicating pregnancy, first trimester","42"],
["DX","O99351","Diseases of the nervous system complicating pregnancy, first trimester","42"],
["DX","O99411","Diseases of the circulatory system complicating pregnancy, first trimester","42"],
["DX","O99511","Diseases of the respiratory system complicating pregnancy, first trimester","42"],
["DX","O99611","Diseases of the digestive system complicating pregnancy, first trimester","42"],
["DX","O99711","Diseases of the skin and subcutaneous tissue complicating pregnancy, first trimester","42"],
["DX","O99841","Bariatric surgery status complicating pregnancy, first trimester","42"],
["DX","O9A111","Malignant neoplasm complicating pregnancy, first trimester","42"],
["DX","O9A211","Injury, poisoning and certain other consequences of external causes complicating pregnancy, first trimester","42"],
["DX","O9A311","Physical abuse complicating pregnancy, first trimester","42"],
["DX","O9A411","Sexual abuse complicating pregnancy, first trimester","42"],
["DX","O9A511","Psychological abuse complicating pregnancy, first trimester","42"],
["DX","Z3401","Encounter for supervision of normal first pregnancy, first trimester","42"],
["DX","Z3481","Encounter for supervision of other normal pregnancy, first trimester","42"],
["DX","Z3491","Encounter for supervision of normal pregnancy, unspecified, first trimester","42"],
["DX","O6012X0","Preterm labor second trimester with preterm delivery second trimester, not applicable or unspecified","133"],
["DX","O6012X1","Preterm labor second trimester with preterm delivery second trimester, fetus 1","133"],
["DX","O6012X2","Preterm labor second trimester with preterm delivery second trimester, fetus 2","133"],
["DX","O6012X3","Preterm labor second trimester with preterm delivery second trimester, fetus 3","133"],
["DX","O6012X4","Preterm labor second trimester with preterm delivery second trimester, fetus 4","133"],
["DX","O6012X5","Preterm labor second trimester with preterm delivery second trimester, fetus 5","133"],
["DX","O6012X9","Preterm labor second trimester with preterm delivery second trimester, other fetus","133"],
["DX","O42012","Preterm premature rupture of membranes, onset of labor within 24 hours of rupture, second trimester","133"],
["DX","O42112","Preterm premature rupture of membranes, onset of labor more than 24 hours following rupture, second trimester","133"],
["DX","O42912","Preterm premature rupture of membranes, unspecified as to length of time between rupture and onset of labor, second trimester","133"],
["DX","O0902","Supervision of pregnancy with history of infertility, second trimester","133"],
["DX","O0912","Supervision of pregnancy with history of ectopic pregnancy, second trimester","133"],
["DX","O09212","Supervision of pregnancy with history of pre-term labor, second trimester","133"],
["DX","O09292","Supervision of pregnancy with other poor reproductive or obstetric history, second trimester","133"],
["DX","O0932","Supervision of pregnancy with insufficient antenatal care, second trimester","133"],
["DX","O0942","Supervision of pregnancy with grand multiparity, second trimester","133"],
["DX","O09512","Supervision of elderly primigravida, second trimester","133"],
["DX","O09522","Supervision of elderly multigravida, second trimester","133"],
["DX","O09612","Supervision of young primigravida, second trimester","133"],
["DX","O09622","Supervision of young multigravida, second trimester","133"],
["DX","O0972","Supervision of high risk pregnancy due to social problems, second trimester","133"],
["DX","O09812","Supervision of pregnancy resulting from assisted reproductive technology, second trimester","133"],
["DX","O09822","Supervision of pregnancy with history of in utero procedure during previous pregnancy, second trimester","133"],
["DX","O09892","Supervision of other high risk pregnancies, second trimester","133"],
["DX","O0992","Supervision of high risk pregnancy, unspecified, second trimester","133"],
["DX","O09A2","Supervision of pregnancy with history of molar pregnancy, second trimester","133"],
["DX","O10012","Pre-existing essential hypertension complicating pregnancy, second trimester","133"],
["DX","O10112","Pre-existing hypertensive heart disease complicating pregnancy, second trimester","133"],
["DX","O10212","Pre-existing hypertensive chronic kidney disease complicating pregnancy, second trimester","133"],
["DX","O10312","Pre-existing hypertensive heart and chronic kidney disease complicating pregnancy, second trimester","133"],
["DX","O10412","Pre-existing secondary hypertension complicating pregnancy, second trimester","133"],
["DX","O10912","Unspecified pre-existing hypertension complicating pregnancy, second trimester","133"],
["DX","O112","Pre-existing hypertension with pre-eclampsia, second trimester","133"],
["DX","O1202","Gestational edema, second trimester","133"],
["DX","O1212","Gestational proteinuria, second trimester","133"],
["DX","O1222","Gestational edema with proteinuria, second trimester","133"],
["DX","O132","Gestational [pregnancy-induced] hypertension without significant proteinuria, second trimester","133"],
["DX","O1402","Mild to moderate pre-eclampsia, second trimester","133"],
["DX","O1412","Severe pre-eclampsia, second trimester","133"],
["DX","O1422","HELLP syndrome (HELLP), second trimester","133"],
["DX","O1492","Unspecified pre-eclampsia, second trimester","133"],
["DX","O1502","Eclampsia complicating pregnancy, second trimester","133"],
["DX","O162","Unspecified maternal hypertension, second trimester","133"],
["DX","O2202","Varicose veins of lower extremity in pregnancy, second trimester","133"],
["DX","O2212","Genital varices in pregnancy, second trimester","133"],
["DX","O2222","Superficial thrombophlebitis in pregnancy, second trimester","133"],
["DX","O2232","Deep phlebothrombosis in pregnancy, second trimester","133"],
["DX","O2242","Hemorrhoids in pregnancy, second trimester","133"],
["DX","O2252","Cerebral venous thrombosis in pregnancy, second trimester","133"],
["DX","O228X2","Other venous complications in pregnancy, second trimester","133"],
["DX","O2292","Venous complication in pregnancy, unspecified, second trimester","133"],
["DX","O2302","Infections of kidney in pregnancy, second trimester","133"],
["DX","O2312","Infections of bladder in pregnancy, second trimester","133"],
["DX","O2322","Infections of urethra in pregnancy, second trimester","133"],
["DX","O2332","Infections of other parts of urinary tract in pregnancy, second trimester","133"],
["DX","O2342","Unspecified infection of urinary tract in pregnancy, second trimester","133"],
["DX","O23512","Infections of cervix in pregnancy, second trimester","133"],
["DX","O23522","Salpingo-oophoritis in pregnancy, second trimester","133"],
["DX","O23592","Infection of other part of genital tract in pregnancy, second trimester","133"],
["DX","O2392","Unspecified genitourinary tract infection in pregnancy, second trimester","133"],
["DX","O24012","Pre-existing type 1 diabetes mellitus, in pregnancy, second trimester","133"],
["DX","O24112","Pre-existing type 2 diabetes mellitus, in pregnancy, second trimester","133"],
["DX","O24312","Unspecified pre-existing diabetes mellitus in pregnancy, second trimester","133"],
["DX","O24812","Other pre-existing diabetes mellitus in pregnancy, second trimester","133"],
["DX","O24912","Unspecified diabetes mellitus in pregnancy, second trimester","133"],
["DX","O2512","Malnutrition in pregnancy, second trimester","133"],
["DX","O2602","Excessive weight gain in pregnancy, second trimester","133"],
["DX","O2612","Low weight gain in pregnancy, second trimester","133"],
["DX","O2622","Pregnancy care for patient with recurrent pregnancy loss, second trimester","133"],
["DX","O2632","Retained intrauterine contraceptive device in pregnancy, second trimester","133"],
["DX","O2642","Herpes gestationis, second trimester","133"],
["DX","O2652","Maternal hypotension syndrome, second trimester","133"],
["DX","O26612","Liver and biliary tract disorders in pregnancy, second trimester","133"],
["DX","O26712","Subluxation of symphysis (pubis) in pregnancy, second trimester","133"],
["DX","O26812","Pregnancy related exhaustion and fatigue, second trimester","133"],
["DX","O26822","Pregnancy related peripheral neuritis, second trimester","133"],
["DX","O26832","Pregnancy related renal disease, second trimester","133"],
["DX","O26842","Uterine size-date discrepancy, second trimester","133"],
["DX","O26852","Spotting complicating pregnancy, second trimester","133"],
["DX","O26872","Cervical shortening, second trimester","133"],
["DX","O26892","Other specified pregnancy related conditions, second trimester","133"],
["DX","O2692","Pregnancy related conditions, unspecified, second trimester","133"],
["DX","O29012","Aspiration pneumonitis due to anesthesia during pregnancy, second trimester","133"],
["DX","O29022","Pressure collapse of lung due to anesthesia during pregnancy, second trimester","133"],
["DX","O29092","Other pulmonary complications of anesthesia during pregnancy, second trimester","133"],
["DX","O29112","Cardiac arrest due to anesthesia during pregnancy, second trimester","133"],
["DX","O29122","Cardiac failure due to anesthesia during pregnancy, second trimester","133"],
["DX","O29192","Other cardiac complications of anesthesia during pregnancy, second trimester","133"],
["DX","O29212","Cerebral anoxia due to anesthesia during pregnancy, second trimester","133"],
["DX","O29292","Other central nervous system complications of anesthesia during pregnancy, second trimester","133"],
["DX","O293X2","Toxic reaction to local anesthesia during pregnancy, second trimester","133"],
["DX","O2942","Spinal and epidural anesthesia induced headache during pregnancy, second trimester","133"],
["DX","O295X2","Other complications of spinal and epidural anesthesia during pregnancy, second trimester","133"],
["DX","O2962","Failed or difficult intubation for anesthesia during pregnancy, second trimester","133"],
["DX","O298X2","Other complications of anesthesia during pregnancy, second trimester","133"],
["DX","O2992","Unspecified complication of anesthesia during pregnancy, second trimester","133"],
["DX","O30002","Twin pregnancy, unspecified number of placenta and unspecified number of amniotic sacs, second trimester","133"],
["DX","O30012","Twin pregnancy, monochorionic/monoamniotic, second trimester","133"],
["DX","O30022","Conjoined twin pregnancy, second trimester","133"],
["DX","O30032","Twin pregnancy, monochorionic/diamniotic, second trimester","133"],
["DX","O30042","Twin pregnancy, dichorionic/diamniotic, second trimester","133"],
["DX","O30092","Twin pregnancy, unable to determine number of placenta and number of amniotic sacs, second trimester","133"],
["DX","O30102","Triplet pregnancy, unspecified number of placenta and unspecified number of amniotic sacs, second trimester","133"],
["DX","O30112","Triplet pregnancy with two or more monochorionic fetuses, second trimester","133"],
["DX","O30122","Triplet pregnancy with two or more monoamniotic fetuses, second trimester","133"],
["DX","O30132","Triplet pregnancy, trichorionic/triamniotic, second trimester","133"],
["DX","O30192","Triplet pregnancy, unable to determine number of placenta and number of amniotic sacs, second trimester","133"],
["DX","O30202","Quadruplet pregnancy, unspecified number of placenta and unspecified number of amniotic sacs, second trimester","133"],
["DX","O30212","Quadruplet pregnancy with two or more monochorionic fetuses, second trimester","133"],
["DX","O30222","Quadruplet pregnancy with two or more monoamniotic fetuses, second trimester","133"],
["DX","O30232","Quadruplet pregnancy, quadrachorionic/quadra-amniotic, second trimester","133"],
["DX","O30292","Quadruplet pregnancy, unable to determine number of placenta and number of amniotic sacs, second trimester","133"],
["DX","O30802","Other specified multiple gestation, unspecified number of placenta and unspecified number of amniotic sacs, second trimester","133"],
["DX","O30812","Other specified multiple gestation with two or more monochorionic fetuses, second trimester","133"],
["DX","O30822","Other specified multiple gestation with two or more monoamniotic fetuses, second trimester","133"],
["DX","O30832","Other specified multiple gestation, number of chorions and amnions are both equal to the number of fetuses, second trimester","133"],
["DX","O30892","Other specified multiple gestation, unable to determine number of placenta and number of amniotic sacs, second trimester","133"],
["DX","O3092","Multiple gestation, unspecified, second trimester","133"],
["DX","O3102X0","Papyraceous fetus, second trimester, not applicable or unspecified","133"],
["DX","O3102X1","Papyraceous fetus, second trimester, fetus 1","133"],
["DX","O3102X2","Papyraceous fetus, second trimester, fetus 2","133"],
["DX","O3102X3","Papyraceous fetus, second trimester, fetus 3","133"],
["DX","O3102X4","Papyraceous fetus, second trimester, fetus 4","133"],
["DX","O3102X5","Papyraceous fetus, second trimester, fetus 5","133"],
["DX","O3102X9","Papyraceous fetus, second trimester, other fetus","133"],
["DX","O3112X0","Continuing pregnancy after spontaneous abortion of one fetus or more, second trimester, not applicable or unspecified","133"],
["DX","O3112X1","Continuing pregnancy after spontaneous abortion of one fetus or more, second trimester, fetus 1","133"],
["DX","O3112X2","Continuing pregnancy after spontaneous abortion of one fetus or more, second trimester, fetus 2","133"],
["DX","O3112X3","Continuing pregnancy after spontaneous abortion of one fetus or more, second trimester, fetus 3","133"],
["DX","O3112X4","Continuing pregnancy after spontaneous abortion of one fetus or more, second trimester, fetus 4","133"],
["DX","O3112X5","Continuing pregnancy after spontaneous abortion of one fetus or more, second trimester, fetus 5","133"],
["DX","O3112X9","Continuing pregnancy after spontaneous abortion of one fetus or more, second trimester, other fetus","133"],
["DX","O3122X0","Continuing pregnancy after intrauterine death of one fetus or more, second trimester, not applicable or unspecified","133"],
["DX","O3122X1","Continuing pregnancy after intrauterine death of one fetus or more, second trimester, fetus 1","133"],
["DX","O3122X2","Continuing pregnancy after intrauterine death of one fetus or more, second trimester, fetus 2","133"],
["DX","O3122X3","Continuing pregnancy after intrauterine death of one fetus or more, second trimester, fetus 3","133"],
["DX","O3122X4","Continuing pregnancy after intrauterine death of one fetus or more, second trimester, fetus 4","133"],
["DX","O3122X5","Continuing pregnancy after intrauterine death of one fetus or more, second trimester, fetus 5","133"],
["DX","O3122X9","Continuing pregnancy after intrauterine death of one fetus or more, second trimester, other fetus","133"],
["DX","O3132X0","Continuing pregnancy after elective fetal reduction of one fetus or more, second trimester, not applicable or unspecified","133"],
["DX","O3132X1","Continuing pregnancy after elective fetal reduction of one fetus or more, second trimester, fetus 1","133"],
["DX","O3132X2","Continuing pregnancy after elective fetal reduction of one fetus or more, second trimester, fetus 2","133"],
["DX","O3132X3","Continuing pregnancy after elective fetal reduction of one fetus or more, second trimester, fetus 3","133"],
["DX","O3132X4","Continuing pregnancy after elective fetal reduction of one fetus or more, second trimester, fetus 4","133"],
["DX","O3132X5","Continuing pregnancy after elective fetal reduction of one fetus or more, second trimester, fetus 5","133"],
["DX","O3132X9","Continuing pregnancy after elective fetal reduction of one fetus or more, second trimester, other fetus","133"],
["DX","O318X20","Other complications specific to multiple gestation, second trimester, not applicable or unspecified","133"],
["DX","O318X21","Other complications specific to multiple gestation, second trimester, fetus 1","133"],
["DX","O318X22","Other complications specific to multiple gestation, second trimester, fetus 2","133"],
["DX","O318X23","Other complications specific to multiple gestation, second trimester, fetus 3","133"],
["DX","O318X24","Other complications specific to multiple gestation, second trimester, fetus 4","133"],
["DX","O318X25","Other complications specific to multiple gestation, second trimester, fetus 5","133"],
["DX","O318X29","Other complications specific to multiple gestation, second trimester, other fetus","133"],
["DX","O3402","Maternal care for unspecified congenital malformation of uterus, second trimester","133"],
["DX","O3412","Maternal care for benign tumor of corpus uteri, second trimester","133"],
["DX","O3432","Maternal care for cervical incompetence, second trimester","133"],
["DX","O3442","Maternal care for other abnormalities of cervix, second trimester","133"],
["DX","O34512","Maternal care for incarceration of gravid uterus, second trimester","133"],
["DX","O34522","Maternal care for prolapse of gravid uterus, second trimester","133"],
["DX","O34532","Maternal care for retroversion of gravid uterus, second trimester","133"],
["DX","O34592","Maternal care for other abnormalities of gravid uterus, second trimester","133"],
["DX","O3462","Maternal care for abnormality of vagina, second trimester","133"],
["DX","O3472","Maternal care for abnormality of vulva and perineum, second trimester","133"],
["DX","O3482","Maternal care for other abnormalities of pelvic organs, second trimester","133"],
["DX","O3492","Maternal care for abnormality of pelvic organ, unspecified, second trimester","133"],
["DX","O360120","Maternal care for anti-D [Rh] antibodies, second trimester, not applicable or unspecified","133"],
["DX","O360121","Maternal care for anti-D [Rh] antibodies, second trimester, fetus 1","133"],
["DX","O360122","Maternal care for anti-D [Rh] antibodies, second trimester, fetus 2","133"],
["DX","O360123","Maternal care for anti-D [Rh] antibodies, second trimester, fetus 3","133"],
["DX","O360124","Maternal care for anti-D [Rh] antibodies, second trimester, fetus 4","133"],
["DX","O360125","Maternal care for anti-D [Rh] antibodies, second trimester, fetus 5","133"],
["DX","O360129","Maternal care for anti-D [Rh] antibodies, second trimester, other fetus","133"],
["DX","O360920","Maternal care for other rhesus isoimmunization, second trimester, not applicable or unspecified","133"],
["DX","O360921","Maternal care for other rhesus isoimmunization, second trimester, fetus 1","133"],
["DX","O360922","Maternal care for other rhesus isoimmunization, second trimester, fetus 2","133"],
["DX","O360923","Maternal care for other rhesus isoimmunization, second trimester, fetus 3","133"],
["DX","O360924","Maternal care for other rhesus isoimmunization, second trimester, fetus 4","133"],
["DX","O360925","Maternal care for other rhesus isoimmunization, second trimester, fetus 5","133"],
["DX","O360929","Maternal care for other rhesus isoimmunization, second trimester, other fetus","133"],
["DX","O361120","Maternal care for Anti-A sensitization, second trimester, not applicable or unspecified","133"],
["DX","O361121","Maternal care for Anti-A sensitization, second trimester, fetus 1","133"],
["DX","O361122","Maternal care for Anti-A sensitization, second trimester, fetus 2","133"],
["DX","O361123","Maternal care for Anti-A sensitization, second trimester, fetus 3","133"],
["DX","O361124","Maternal care for Anti-A sensitization, second trimester, fetus 4","133"],
["DX","O361125","Maternal care for Anti-A sensitization, second trimester, fetus 5","133"],
["DX","O361129","Maternal care for Anti-A sensitization, second trimester, other fetus","133"],
["DX","O361920","Maternal care for other isoimmunization, second trimester, not applicable or unspecified","133"],
["DX","O361921","Maternal care for other isoimmunization, second trimester, fetus 1","133"],
["DX","O361922","Maternal care for other isoimmunization, second trimester, fetus 2","133"],
["DX","O361923","Maternal care for other isoimmunization, second trimester, fetus 3","133"],
["DX","O361924","Maternal care for other isoimmunization, second trimester, fetus 4","133"],
["DX","O361925","Maternal care for other isoimmunization, second trimester, fetus 5","133"],
["DX","O361929","Maternal care for other isoimmunization, second trimester, other fetus","133"],
["DX","O3622X0","Maternal care for hydrops fetalis, second trimester, not applicable or unspecified","133"],
["DX","O3622X1","Maternal care for hydrops fetalis, second trimester, fetus 1","133"],
["DX","O3622X2","Maternal care for hydrops fetalis, second trimester, fetus 2","133"],
["DX","O3622X3","Maternal care for hydrops fetalis, second trimester, fetus 3","133"],
["DX","O3622X4","Maternal care for hydrops fetalis, second trimester, fetus 4","133"],
["DX","O3622X5","Maternal care for hydrops fetalis, second trimester, fetus 5","133"],
["DX","O3622X9","Maternal care for hydrops fetalis, second trimester, other fetus","133"],
["DX","O365120","Maternal care for known or suspected placental insufficiency, second trimester, not applicable or unspecified","133"],
["DX","O365121","Maternal care for known or suspected placental insufficiency, second trimester, fetus 1","133"],
["DX","O365122","Maternal care for known or suspected placental insufficiency, second trimester, fetus 2","133"],
["DX","O365123","Maternal care for known or suspected placental insufficiency, second trimester, fetus 3","133"],
["DX","O365124","Maternal care for known or suspected placental insufficiency, second trimester, fetus 4","133"],
["DX","O365125","Maternal care for known or suspected placental insufficiency, second trimester, fetus 5","133"],
["DX","O365129","Maternal care for known or suspected placental insufficiency, second trimester, other fetus","133"],
["DX","O365920","Maternal care for other known or suspected poor fetal growth, second trimester, not applicable or unspecified","133"],
["DX","O365921","Maternal care for other known or suspected poor fetal growth, second trimester, fetus 1","133"],
["DX","O365922","Maternal care for other known or suspected poor fetal growth, second trimester, fetus 2","133"],
["DX","O365923","Maternal care for other known or suspected poor fetal growth, second trimester, fetus 3","133"],
["DX","O365924","Maternal care for other known or suspected poor fetal growth, second trimester, fetus 4","133"],
["DX","O365925","Maternal care for other known or suspected poor fetal growth, second trimester, fetus 5","133"],
["DX","O365929","Maternal care for other known or suspected poor fetal growth, second trimester, other fetus","133"],
["DX","O3662X0","Maternal care for excessive fetal growth, second trimester, not applicable or unspecified","133"],
["DX","O3662X1","Maternal care for excessive fetal growth, second trimester, fetus 1","133"],
["DX","O3662X2","Maternal care for excessive fetal growth, second trimester, fetus 2","133"],
["DX","O3662X3","Maternal care for excessive fetal growth, second trimester, fetus 3","133"],
["DX","O3662X4","Maternal care for excessive fetal growth, second trimester, fetus 4","133"],
["DX","O3662X5","Maternal care for excessive fetal growth, second trimester, fetus 5","133"],
["DX","O3662X9","Maternal care for excessive fetal growth, second trimester, other fetus","133"],
["DX","O3672X0","Maternal care for viable fetus in abdominal pregnancy, second trimester, not applicable or unspecified","133"],
["DX","O3672X1","Maternal care for viable fetus in abdominal pregnancy, second trimester, fetus 1","133"],
["DX","O3672X2","Maternal care for viable fetus in abdominal pregnancy, second trimester, fetus 2","133"],
["DX","O3672X3","Maternal care for viable fetus in abdominal pregnancy, second trimester, fetus 3","133"],
["DX","O3672X4","Maternal care for viable fetus in abdominal pregnancy, second trimester, fetus 4","133"],
["DX","O3672X5","Maternal care for viable fetus in abdominal pregnancy, second trimester, fetus 5","133"],
["DX","O3672X9","Maternal care for viable fetus in abdominal pregnancy, second trimester, other fetus","133"],
["DX","O368120","Decreased fetal movements, second trimester, not applicable or unspecified","133"],
["DX","O368121","Decreased fetal movements, second trimester, fetus 1","133"],
["DX","O368122","Decreased fetal movements, second trimester, fetus 2","133"],
["DX","O368123","Decreased fetal movements, second trimester, fetus 3","133"],
["DX","O368124","Decreased fetal movements, second trimester, fetus 4","133"],
["DX","O368125","Decreased fetal movements, second trimester, fetus 5","133"],
["DX","O368129","Decreased fetal movements, second trimester, other fetus","133"],
["DX","O368220","Fetal anemia and thrombocytopenia, second trimester, not applicable or unspecified","133"],
["DX","O368221","Fetal anemia and thrombocytopenia, second trimester, fetus 1","133"],
["DX","O368222","Fetal anemia and thrombocytopenia, second trimester, fetus 2","133"],
["DX","O368223","Fetal anemia and thrombocytopenia, second trimester, fetus 3","133"],
["DX","O368224","Fetal anemia and thrombocytopenia, second trimester, fetus 4","133"],
["DX","O368225","Fetal anemia and thrombocytopenia, second trimester, fetus 5","133"],
["DX","O368229","Fetal anemia and thrombocytopenia, second trimester, other fetus","133"],
["DX","O368320","Maternal care for abnormalities of the fetal heart rate or rhythm, second trimester, not applicable or unspecified","133"],
["DX","O368321","Maternal care for abnormalities of the fetal heart rate or rhythm, second trimester, fetus 1","133"],
["DX","O368322","Maternal care for abnormalities of the fetal heart rate or rhythm, second trimester, fetus 2","133"],
["DX","O368323","Maternal care for abnormalities of the fetal heart rate or rhythm, second trimester, fetus 3","133"],
["DX","O368324","Maternal care for abnormalities of the fetal heart rate or rhythm, second trimester, fetus 4","133"],
["DX","O368325","Maternal care for abnormalities of the fetal heart rate or rhythm, second trimester, fetus 5","133"],
["DX","O368329","Maternal care for abnormalities of the fetal heart rate or rhythm, second trimester, other fetus","133"],
["DX","O368920","Maternal care for other specified fetal problems, second trimester, not applicable or unspecified","133"],
["DX","O368921","Maternal care for other specified fetal problems, second trimester, fetus 1","133"],
["DX","O368922","Maternal care for other specified fetal problems, second trimester, fetus 2","133"],
["DX","O368923","Maternal care for other specified fetal problems, second trimester, fetus 3","133"],
["DX","O368924","Maternal care for other specified fetal problems, second trimester, fetus 4","133"],
["DX","O368925","Maternal care for other specified fetal problems, second trimester, fetus 5","133"],
["DX","O368929","Maternal care for other specified fetal problems, second trimester, other fetus","133"],
["DX","O3692X0","Maternal care for fetal problem, unspecified, second trimester, not applicable or unspecified","133"],
["DX","O3692X1","Maternal care for fetal problem, unspecified, second trimester, fetus 1","133"],
["DX","O3692X2","Maternal care for fetal problem, unspecified, second trimester, fetus 2","133"],
["DX","O3692X3","Maternal care for fetal problem, unspecified, second trimester, fetus 3","133"],
["DX","O3692X4","Maternal care for fetal problem, unspecified, second trimester, fetus 4","133"],
["DX","O3692X5","Maternal care for fetal problem, unspecified, second trimester, fetus 5","133"],
["DX","O3692X9","Maternal care for fetal problem, unspecified, second trimester, other fetus","133"],
["DX","O402XX0","Polyhydramnios, second trimester, not applicable or unspecified","133"],
["DX","O402XX1","Polyhydramnios, second trimester, fetus 1","133"],
["DX","O402XX2","Polyhydramnios, second trimester, fetus 2","133"],
["DX","O402XX3","Polyhydramnios, second trimester, fetus 3","133"],
["DX","O402XX4","Polyhydramnios, second trimester, fetus 4","133"],
["DX","O402XX5","Polyhydramnios, second trimester, fetus 5","133"],
["DX","O402XX9","Polyhydramnios, second trimester, other fetus","133"],
["DX","O4102X0","Oligohydramnios, second trimester, not applicable or unspecified","133"],
["DX","O4102X1","Oligohydramnios, second trimester, fetus 1","133"],
["DX","O4102X2","Oligohydramnios, second trimester, fetus 2","133"],
["DX","O4102X3","Oligohydramnios, second trimester, fetus 3","133"],
["DX","O4102X4","Oligohydramnios, second trimester, fetus 4","133"],
["DX","O4102X5","Oligohydramnios, second trimester, fetus 5","133"],
["DX","O4102X9","Oligohydramnios, second trimester, other fetus","133"],
["DX","O411020","Infection of amniotic sac and membranes, unspecified, second trimester, not applicable or unspecified","133"],
["DX","O411021","Infection of amniotic sac and membranes, unspecified, second trimester, fetus 1","133"],
["DX","O411022","Infection of amniotic sac and membranes, unspecified, second trimester, fetus 2","133"],
["DX","O411023","Infection of amniotic sac and membranes, unspecified, second trimester, fetus 3","133"],
["DX","O411024","Infection of amniotic sac and membranes, unspecified, second trimester, fetus 4","133"],
["DX","O411025","Infection of amniotic sac and membranes, unspecified, second trimester, fetus 5","133"],
["DX","O411029","Infection of amniotic sac and membranes, unspecified, second trimester, other fetus","133"],
["DX","O411220","Chorioamnionitis, second trimester, not applicable or unspecified","133"],
["DX","O411221","Chorioamnionitis, second trimester, fetus 1","133"],
["DX","O411222","Chorioamnionitis, second trimester, fetus 2","133"],
["DX","O411223","Chorioamnionitis, second trimester, fetus 3","133"],
["DX","O411224","Chorioamnionitis, second trimester, fetus 4","133"],
["DX","O411225","Chorioamnionitis, second trimester, fetus 5","133"],
["DX","O411229","Chorioamnionitis, second trimester, other fetus","133"],
["DX","O411420","Placentitis, second trimester, not applicable or unspecified","133"],
["DX","O411421","Placentitis, second trimester, fetus 1","133"],
["DX","O411422","Placentitis, second trimester, fetus 2","133"],
["DX","O411423","Placentitis, second trimester, fetus 3","133"],
["DX","O411424","Placentitis, second trimester, fetus 4","133"],
["DX","O411425","Placentitis, second trimester, fetus 5","133"],
["DX","O411429","Placentitis, second trimester, other fetus","133"],
["DX","O418X20","Other specified disorders of amniotic fluid and membranes, second trimester, not applicable or unspecified","133"],
["DX","O418X21","Other specified disorders of amniotic fluid and membranes, second trimester, fetus 1","133"],
["DX","O418X22","Other specified disorders of amniotic fluid and membranes, second trimester, fetus 2","133"],
["DX","O418X23","Other specified disorders of amniotic fluid and membranes, second trimester, fetus 3","133"],
["DX","O418X24","Other specified disorders of amniotic fluid and membranes, second trimester, fetus 4","133"],
["DX","O418X25","Other specified disorders of amniotic fluid and membranes, second trimester, fetus 5","133"],
["DX","O418X29","Other specified disorders of amniotic fluid and membranes, second trimester, other fetus","133"],
["DX","O4192X0","Disorder of amniotic fluid and membranes, unspecified, second trimester, not applicable or unspecified","133"],
["DX","O4192X1","Disorder of amniotic fluid and membranes, unspecified, second trimester, fetus 1","133"],
["DX","O4192X2","Disorder of amniotic fluid and membranes, unspecified, second trimester, fetus 2","133"],
["DX","O4192X3","Disorder of amniotic fluid and membranes, unspecified, second trimester, fetus 3","133"],
["DX","O4192X4","Disorder of amniotic fluid and membranes, unspecified, second trimester, fetus 4","133"],
["DX","O4192X5","Disorder of amniotic fluid and membranes, unspecified, second trimester, fetus 5","133"],
["DX","O4192X9","Disorder of amniotic fluid and membranes, unspecified, second trimester, other fetus","133"],
["DX","O43012","Fetomaternal placental transfusion syndrome, second trimester","133"],
["DX","O43022","Fetus-to-fetus placental transfusion syndrome, second trimester","133"],
["DX","O43102","Malformation of placenta, unspecified, second trimester","133"],
["DX","O43112","Circumvallate placenta, second trimester","133"],
["DX","O43122","Velamentous insertion of umbilical cord, second trimester","133"],
["DX","O43192","Other malformation of placenta, second trimester","133"],
["DX","O43212","Placenta accreta, second trimester","133"],
["DX","O43222","Placenta increta, second trimester","133"],
["DX","O43232","Placenta percreta, second trimester","133"],
["DX","O43812","Placental infarction, second trimester","133"],
["DX","O43892","Other placental disorders, second trimester","133"],
["DX","O4392","Unspecified placental disorder, second trimester","133"],
["DX","O4402","Complete placenta previa NOS or without hemorrhage, second trimester","133"],
["DX","O4412","Complete placenta previa with hemorrhage, second trimester","133"],
["DX","O4422","Partial placenta previa NOS or without hemorrhage, second trimester","133"],
["DX","O4432","Partial placenta previa with hemorrhage, second trimester","133"],
["DX","O4442","Low lying placenta NOS or without hemorrhage, second trimester","133"],
["DX","O4452","Low lying placenta with hemorrhage, second trimester","133"],
["DX","O45002","Premature separation of placenta with coagulation defect, unspecified, second trimester","133"],
["DX","O45012","Premature separation of placenta with afibrinogenemia, second trimester","133"],
["DX","O45022","Premature separation of placenta with disseminated intravascular coagulation, second trimester","133"],
["DX","O45092","Premature separation of placenta with other coagulation defect, second trimester","133"],
["DX","O458X2","Other premature separation of placenta, second trimester","133"],
["DX","O4592","Premature separation of placenta, unspecified, second trimester","133"],
["DX","O46002","Antepartum hemorrhage with coagulation defect, unspecified, second trimester","133"],
["DX","O46012","Antepartum hemorrhage with afibrinogenemia, second trimester","133"],
["DX","O46022","Antepartum hemorrhage with disseminated intravascular coagulation, second trimester","133"],
["DX","O46092","Antepartum hemorrhage with other coagulation defect, second trimester","133"],
["DX","O468X2","Other antepartum hemorrhage, second trimester","133"],
["DX","O4692","Antepartum hemorrhage, unspecified, second trimester","133"],
["DX","O4702","False labor before 37 completed weeks of gestation, second trimester","133"],
["DX","O6002","Preterm labor without delivery, second trimester","133"],
["DX","O7102","Rupture of uterus before onset of labor, second trimester","133"],
["DX","O88012","Air embolism in pregnancy, second trimester","133"],
["DX","O88112","Amniotic fluid embolism in pregnancy, second trimester","133"],
["DX","O88212","Thromboembolism in pregnancy, second trimester","133"],
["DX","O88312","Pyemic and septic embolism in pregnancy, second trimester","133"],
["DX","O88812","Other embolism in pregnancy, second trimester","133"],
["DX","O91012","Infection of nipple associated with pregnancy, second trimester","133"],
["DX","O91112","Abscess of breast associated with pregnancy, second trimester","133"],
["DX","O91212","Nonpurulent mastitis associated with pregnancy, second trimester","133"],
["DX","O92012","Retracted nipple associated with pregnancy, second trimester","133"],
["DX","O92112","Cracked nipple associated with pregnancy, second trimester","133"],
["DX","O98012","Tuberculosis complicating pregnancy, second trimester","133"],
["DX","O98112","Syphilis complicating pregnancy, second trimester","133"],
["DX","O98212","Gonorrhea complicating pregnancy, second trimester","133"],
["DX","O98312","Other infections with a predominantly sexual mode of transmission complicating pregnancy, second trimester","133"],
["DX","O98412","Viral hepatitis complicating pregnancy, second trimester","133"],
["DX","O98512","Other viral diseases complicating pregnancy, second trimester","133"],
["DX","O98612","Protozoal diseases complicating pregnancy, second trimester","133"],
["DX","O98712","Human immunodeficiency virus [HIV] disease complicating pregnancy, second trimester","133"],
["DX","O98812","Other maternal infectious and parasitic diseases complicating pregnancy, second trimester","133"],
["DX","O98912","Unspecified maternal infectious and parasitic disease complicating pregnancy, second trimester","133"],
["DX","O99012","Anemia complicating pregnancy, second trimester","133"],
["DX","O99112","Other diseases of the blood and blood-forming organs and certain disorders involving the immune mechanism complicating pregnancy, second trimester","133"],
["DX","O99212","Obesity complicating pregnancy, second trimester","133"],
["DX","O99282","Endocrine, nutritional and metabolic diseases complicating pregnancy, second trimester","133"],
["DX","O99312","Alcohol use complicating pregnancy, second trimester","133"],
["DX","O99322","Drug use complicating pregnancy, second trimester","133"],
["DX","O99332","Smoking (tobacco) complicating pregnancy, second trimester","133"],
["DX","O99342","Other mental disorders complicating pregnancy, second trimester","133"],
["DX","O99352","Diseases of the nervous system complicating pregnancy, second trimester","133"],
["DX","O99412","Diseases of the circulatory system complicating pregnancy, second trimester","133"],
["DX","O99512","Diseases of the respiratory system complicating pregnancy, second trimester","133"],
["DX","O99612","Diseases of the digestive system complicating pregnancy, second trimester","133"],
["DX","O99712","Diseases of the skin and subcutaneous tissue complicating pregnancy, second trimester","133"],
["DX","O99842","Bariatric surgery status complicating pregnancy, second trimester","133"],
["DX","O9A112","Malignant neoplasm complicating pregnancy, second trimester","133"],
["DX","O9A212","Injury, poisoning and certain other consequences of external causes complicating pregnancy, second trimester","133"],
["DX","O9A312","Physical abuse complicating pregnancy, second trimester","133"],
["DX","O9A412","Sexual abuse complicating pregnancy, second trimester","133"],
["DX","O9A512","Psychological abuse complicating pregnancy, second trimester","133"],
["DX","Z3402","Encounter for supervision of normal first pregnancy, second trimester","133"],
["DX","Z3482","Encounter for supervision of other normal pregnancy, second trimester","133"],
["DX","Z3492","Encounter for supervision of normal pregnancy, unspecified, second trimester","133"],
["DX","O6013X0","Preterm labor second trimester with preterm delivery third trimester, not applicable or unspecified","238"],
["DX","O6013X1","Preterm labor second trimester with preterm delivery third trimester, fetus 1","238"],
["DX","O6013X2","Preterm labor second trimester with preterm delivery third trimester, fetus 2","238"],
["DX","O6013X3","Preterm labor second trimester with preterm delivery third trimester, fetus 3","238"],
["DX","O6013X4","Preterm labor second trimester with preterm delivery third trimester, fetus 4","238"],
["DX","O6013X5","Preterm labor second trimester with preterm delivery third trimester, fetus 5","238"],
["DX","O6013X9","Preterm labor second trimester with preterm delivery third trimester, other fetus","238"],
["DX","O6014X0","Preterm labor third trimester with preterm delivery third trimester, not applicable or unspecified","238"],
["DX","O6014X1","Preterm labor third trimester with preterm delivery third trimester, fetus 1","238"],
["DX","O6014X2","Preterm labor third trimester with preterm delivery third trimester, fetus 2","238"],
["DX","O6014X3","Preterm labor third trimester with preterm delivery third trimester, fetus 3","238"],
["DX","O6014X4","Preterm labor third trimester with preterm delivery third trimester, fetus 4","238"],
["DX","O6014X5","Preterm labor third trimester with preterm delivery third trimester, fetus 5","238"],
["DX","O6014X9","Preterm labor third trimester with preterm delivery third trimester, other fetus","238"],
["DX","O42013","Preterm premature rupture of membranes, onset of labor within 24 hours of rupture, third trimester","238"],
["DX","O42113","Preterm premature rupture of membranes, onset of labor more than 24 hours following rupture, third trimester","238"],
["DX","O42913","Preterm premature rupture of membranes, unspecified as to length of time between rupture and onset of labor, third trimester","238"],
["DX","O0903","Supervision of pregnancy with history of infertility, third trimester","238"],
["DX","O0913","Supervision of pregnancy with history of ectopic pregnancy, third trimester","238"],
["DX","O09213","Supervision of pregnancy with history of pre-term labor, third trimester","238"],
["DX","O09293","Supervision of pregnancy with other poor reproductive or obstetric history, third trimester","238"],
["DX","O0933","Supervision of pregnancy with insufficient antenatal care, third trimester","238"],
["DX","O0943","Supervision of pregnancy with grand multiparity, third trimester","238"],
["DX","O09513","Supervision of elderly primigravida, third trimester","238"],
["DX","O09523","Supervision of elderly multigravida, third trimester","238"],
["DX","O09613","Supervision of young primigravida, third trimester","238"],
["DX","O09623","Supervision of young multigravida, third trimester","238"],
["DX","O0973","Supervision of high risk pregnancy due to social problems, third trimester","238"],
["DX","O09813","Supervision of pregnancy resulting from assisted reproductive technology, third trimester","238"],
["DX","O09823","Supervision of pregnancy with history of in utero procedure during previous pregnancy, third trimester","238"],
["DX","O09893","Supervision of other high risk pregnancies, third trimester","238"],
["DX","O0993","Supervision of high risk pregnancy, unspecified, third trimester","238"],
["DX","O09A3","Supervision of pregnancy with history of molar pregnancy, third trimester","238"],
["DX","O10013","Pre-existing essential hypertension complicating pregnancy, third trimester","238"],
["DX","O10113","Pre-existing hypertensive heart disease complicating pregnancy, third trimester","238"],
["DX","O10213","Pre-existing hypertensive chronic kidney disease complicating pregnancy, third trimester","238"],
["DX","O10313","Pre-existing hypertensive heart and chronic kidney disease complicating pregnancy, third trimester","238"],
["DX","O10413","Pre-existing secondary hypertension complicating pregnancy, third trimester","238"],
["DX","O10913","Unspecified pre-existing hypertension complicating pregnancy, third trimester","238"],
["DX","O113","Pre-existing hypertension with pre-eclampsia, third trimester","238"],
["DX","O1203","Gestational edema, third trimester","238"],
["DX","O1213","Gestational proteinuria, third trimester","238"],
["DX","O1223","Gestational edema with proteinuria, third trimester","238"],
["DX","O133","Gestational [pregnancy-induced] hypertension without significant proteinuria, third trimester","238"],
["DX","O1403","Mild to moderate pre-eclampsia, third trimester","238"],
["DX","O1413","Severe pre-eclampsia, third trimester","238"],
["DX","O1423","HELLP syndrome (HELLP), third trimester","238"],
["DX","O1493","Unspecified pre-eclampsia, third trimester","238"],
["DX","O1503","Eclampsia complicating pregnancy, third trimester","238"],
["DX","O163","Unspecified maternal hypertension, third trimester","238"],
["DX","O2203","Varicose veins of lower extremity in pregnancy, third trimester","238"],
["DX","O2213","Genital varices in pregnancy, third trimester","238"],
["DX","O2223","Superficial thrombophlebitis in pregnancy, third trimester","238"],
["DX","O2233","Deep phlebothrombosis in pregnancy, third trimester","238"],
["DX","O2243","Hemorrhoids in pregnancy, third trimester","238"],
["DX","O2253","Cerebral venous thrombosis in pregnancy, third trimester","238"],
["DX","O228X3","Other venous complications in pregnancy, third trimester","238"],
["DX","O2293","Venous complication in pregnancy, unspecified, third trimester","238"],
["DX","O2303","Infections of kidney in pregnancy, third trimester","238"],
["DX","O2313","Infections of bladder in pregnancy, third trimester","238"],
["DX","O2323","Infections of urethra in pregnancy, third trimester","238"],
["DX","O2333","Infections of other parts of urinary tract in pregnancy, third trimester","238"],
["DX","O2343","Unspecified infection of urinary tract in pregnancy, third trimester","238"],
["DX","O23513","Infections of cervix in pregnancy, third trimester","238"],
["DX","O23523","Salpingo-oophoritis in pregnancy, third trimester","238"],
["DX","O23593","Infection of other part of genital tract in pregnancy, third trimester","238"],
["DX","O2393","Unspecified genitourinary tract infection in pregnancy, third trimester","238"],
["DX","O24013","Pre-existing type 1 diabetes mellitus, in pregnancy, third trimester","238"],
["DX","O24113","Pre-existing type 2 diabetes mellitus, in pregnancy, third trimester","238"],
["DX","O24313","Unspecified pre-existing diabetes mellitus in pregnancy, third trimester","238"],
["DX","O24813","Other pre-existing diabetes mellitus in pregnancy, third trimester","238"],
["DX","O24913","Unspecified diabetes mellitus in pregnancy, third trimester","238"],
["DX","O2513","Malnutrition in pregnancy, third trimester","238"],
["DX","O2603","Excessive weight gain in pregnancy, third trimester","238"],
["DX","O2613","Low weight gain in pregnancy, third trimester","238"],
["DX","O2623","Pregnancy care for patient with recurrent pregnancy loss, third trimester","238"],
["DX","O2633","Retained intrauterine contraceptive device in pregnancy, third trimester","238"],
["DX","O2643","Herpes gestationis, third trimester","238"],
["DX","O2653","Maternal hypotension syndrome, third trimester","238"],
["DX","O26613","Liver and biliary tract disorders in pregnancy, third trimester","238"],
["DX","O26713","Subluxation of symphysis (pubis) in pregnancy, third trimester","238"],
["DX","O26813","Pregnancy related exhaustion and fatigue, third trimester","238"],
["DX","O26823","Pregnancy related peripheral neuritis, third trimester","238"],
["DX","O26833","Pregnancy related renal disease, third trimester","238"],
["DX","O26843","Uterine size-date discrepancy, third trimester","238"],
["DX","O26853","Spotting complicating pregnancy, third trimester","238"],
["DX","O26873","Cervical shortening, third trimester","238"],
["DX","O26893","Other specified pregnancy related conditions, third trimester","238"],
["DX","O2693","Pregnancy related conditions, unspecified, third trimester","238"],
["DX","O29013","Aspiration pneumonitis due to anesthesia during pregnancy, third trimester","238"],
["DX","O29023","Pressure collapse of lung due to anesthesia during pregnancy, third trimester","238"],
["DX","O29093","Other pulmonary complications of anesthesia during pregnancy, third trimester","238"],
["DX","O29113","Cardiac arrest due to anesthesia during pregnancy, third trimester","238"],
["DX","O29123","Cardiac failure due to anesthesia during pregnancy, third trimester","238"],
["DX","O29193","Other cardiac complications of anesthesia during pregnancy, third trimester","238"],
["DX","O29213","Cerebral anoxia due to anesthesia during pregnancy, third trimester","238"],
["DX","O29293","Other central nervous system complications of anesthesia during pregnancy, third trimester","238"],
["DX","O293X3","Toxic reaction to local anesthesia during pregnancy, third trimester","238"],
["DX","O2943","Spinal and epidural anesthesia induced headache during pregnancy, third trimester","238"],
["DX","O295X3","Other complications of spinal and epidural anesthesia during pregnancy, third trimester","238"],
["DX","O2963","Failed or difficult intubation for anesthesia during pregnancy, third trimester","238"],
["DX","O298X3","Other complications of anesthesia during pregnancy, third trimester","238"],
["DX","O2993","Unspecified complication of anesthesia during pregnancy, third trimester","238"],
["DX","O30003","Twin pregnancy, unspecified number of placenta and unspecified number of amniotic sacs, third trimester","238"],
["DX","O30013","Twin pregnancy, monochorionic/monoamniotic, third trimester","238"],
["DX","O30023","Conjoined twin pregnancy, third trimester","238"],
["DX","O30033","Twin pregnancy, monochorionic/diamniotic, third trimester","238"],
["DX","O30043","Twin pregnancy, dichorionic/diamniotic, third trimester","238"],
["DX","O30093","Twin pregnancy, unable to determine number of placenta and number of amniotic sacs, third trimester","238"],
["DX","O30103","Triplet pregnancy, unspecified number of placenta and unspecified number of amniotic sacs, third trimester","238"],
["DX","O30113","Triplet pregnancy with two or more monochorionic fetuses, third trimester","238"],
["DX","O30123","Triplet pregnancy with two or more monoamniotic fetuses, third trimester","238"],
["DX","O30133","Triplet pregnancy, trichorionic/triamniotic, third trimester","238"],
["DX","O30193","Triplet pregnancy, unable to determine number of placenta and number of amniotic sacs, third trimester","238"],
["DX","O30203","Quadruplet pregnancy, unspecified number of placenta and unspecified number of amniotic sacs, third trimester","238"],
["DX","O30213","Quadruplet pregnancy with two or more monochorionic fetuses, third trimester","238"],
["DX","O30223","Quadruplet pregnancy with two or more monoamniotic fetuses, third trimester","238"],
["DX","O30233","Quadruplet pregnancy, quadrachorionic/quadra-amniotic, third trimester","238"],
["DX","O30293","Quadruplet pregnancy, unable to determine number of placenta and number of amniotic sacs, third trimester","238"],
["DX","O30803","Other specified multiple gestation, unspecified number of placenta and unspecified number of amniotic sacs, third trimester","238"],
["DX","O30813","Other specified multiple gestation with two or more monochorionic fetuses, third trimester","238"],
["DX","O30823","Other specified multiple gestation with two or more monoamniotic fetuses, third trimester","238"],
["DX","O30833","Other specified multiple gestation, number of chorions and amnions are both equal to the number of fetuses, third trimester","238"],
["DX","O30893","Other specified multiple gestation, unable to determine number of placenta and number of amniotic sacs, third trimester","238"],
["DX","O3093","Multiple gestation, unspecified, third trimester","238"],
["DX","O3103X0","Papyraceous fetus, third trimester, not applicable or unspecified","238"],
["DX","O3103X1","Papyraceous fetus, third trimester, fetus 1","238"],
["DX","O3103X2","Papyraceous fetus, third trimester, fetus 2","238"],
["DX","O3103X3","Papyraceous fetus, third trimester, fetus 3","238"],
["DX","O3103X4","Papyraceous fetus, third trimester, fetus 4","238"],
["DX","O3103X5","Papyraceous fetus, third trimester, fetus 5","238"],
["DX","O3103X9","Papyraceous fetus, third trimester, other fetus","238"],
["DX","O3113X0","Continuing pregnancy after spontaneous abortion of one fetus or more, third trimester, not applicable or unspecified","238"],
["DX","O3113X1","Continuing pregnancy after spontaneous abortion of one fetus or more, third trimester, fetus 1","238"],
["DX","O3113X2","Continuing pregnancy after spontaneous abortion of one fetus or more, third trimester, fetus 2","238"],
["DX","O3113X3","Continuing pregnancy after spontaneous abortion of one fetus or more, third trimester, fetus 3","238"],
["DX","O3113X4","Continuing pregnancy after spontaneous abortion of one fetus or more, third trimester, fetus 4","238"],
["DX","O3113X5","Continuing pregnancy after spontaneous abortion of one fetus or more, third trimester, fetus 5","238"],
["DX","O3113X9","Continuing pregnancy after spontaneous abortion of one fetus or more, third trimester, other fetus","238"],
["DX","O3123X0","Continuing pregnancy after intrauterine death of one fetus or more, third trimester, not applicable or unspecified","238"],
["DX","O3123X1","Continuing pregnancy after intrauterine death of one fetus or more, third trimester, fetus 1","238"],
["DX","O3123X2","Continuing pregnancy after intrauterine death of one fetus or more, third trimester, fetus 2","238"],
["DX","O3123X3","Continuing pregnancy after intrauterine death of one fetus or more, third trimester, fetus 3","238"],
["DX","O3123X4","Continuing pregnancy after intrauterine death of one fetus or more, third trimester, fetus 4","238"],
["DX","O3123X5","Continuing pregnancy after intrauterine death of one fetus or more, third trimester, fetus 5","238"],
["DX","O3123X9","Continuing pregnancy after intrauterine death of one fetus or more, third trimester, other fetus","238"],
["DX","O3133X0","Continuing pregnancy after elective fetal reduction of one fetus or more, third trimester, not applicable or unspecified","238"],
["DX","O3133X1","Continuing pregnancy after elective fetal reduction of one fetus or more, third trimester, fetus 1","238"],
["DX","O3133X2","Continuing pregnancy after elective fetal reduction of one fetus or more, third trimester, fetus 2","238"],
["DX","O3133X3","Continuing pregnancy after elective fetal reduction of one fetus or more, third trimester, fetus 3","238"],
["DX","O3133X4","Continuing pregnancy after elective fetal reduction of one fetus or more, third trimester, fetus 4","238"],
["DX","O3133X5","Continuing pregnancy after elective fetal reduction of one fetus or more, third trimester, fetus 5","238"],
["DX","O3133X9","Continuing pregnancy after elective fetal reduction of one fetus or more, third trimester, other fetus","238"],
["DX","O318X30","Other complications specific to multiple gestation, third trimester, not applicable or unspecified","238"],
["DX","O318X31","Other complications specific to multiple gestation, third trimester, fetus 1","238"],
["DX","O318X32","Other complications specific to multiple gestation, third trimester, fetus 2","238"],
["DX","O318X33","Other complications specific to multiple gestation, third trimester, fetus 3","238"],
["DX","O318X34","Other complications specific to multiple gestation, third trimester, fetus 4","238"],
["DX","O318X35","Other complications specific to multiple gestation, third trimester, fetus 5","238"],
["DX","O318X39","Other complications specific to multiple gestation, third trimester, other fetus","238"],
["DX","O3403","Maternal care for unspecified congenital malformation of uterus, third trimester","238"],
["DX","O3413","Maternal care for benign tumor of corpus uteri, third trimester","238"],
["DX","O3433","Maternal care for cervical incompetence, third trimester","238"],
["DX","O3443","Maternal care for other abnormalities of cervix, third trimester","238"],
["DX","O34513","Maternal care for incarceration of gravid uterus, third trimester","238"],
["DX","O34523","Maternal care for prolapse of gravid uterus, third trimester","238"],
["DX","O34533","Maternal care for retroversion of gravid uterus, third trimester","238"],
["DX","O34593","Maternal care for other abnormalities of gravid uterus, third trimester","238"],
["DX","O3463","Maternal care for abnormality of vagina, third trimester","238"],
["DX","O3473","Maternal care for abnormality of vulva and perineum, third trimester","238"],
["DX","O3483","Maternal care for other abnormalities of pelvic organs, third trimester","238"],
["DX","O3493","Maternal care for abnormality of pelvic organ, unspecified, third trimester","238"],
["DX","O360130","Maternal care for anti-D [Rh] antibodies, third trimester, not applicable or unspecified","238"],
["DX","O360131","Maternal care for anti-D [Rh] antibodies, third trimester, fetus 1","238"],
["DX","O360132","Maternal care for anti-D [Rh] antibodies, third trimester, fetus 2","238"],
["DX","O360133","Maternal care for anti-D [Rh] antibodies, third trimester, fetus 3","238"],
["DX","O360134","Maternal care for anti-D [Rh] antibodies, third trimester, fetus 4","238"],
["DX","O360135","Maternal care for anti-D [Rh] antibodies, third trimester, fetus 5","238"],
["DX","O360139","Maternal care for anti-D [Rh] antibodies, third trimester, other fetus","238"],
["DX","O360930","Maternal care for other rhesus isoimmunization, third trimester, not applicable or unspecified","238"],
["DX","O360931","Maternal care for other rhesus isoimmunization, third trimester, fetus 1","238"],
["DX","O360932","Maternal care for other rhesus isoimmunization, third trimester, fetus 2","238"],
["DX","O360933","Maternal care for other rhesus isoimmunization, third trimester, fetus 3","238"],
["DX","O360934","Maternal care for other rhesus isoimmunization, third trimester, fetus 4","238"],
["DX","O360935","Maternal care for other rhesus isoimmunization, third trimester, fetus 5","238"],
["DX","O360939","Maternal care for other rhesus isoimmunization, third trimester, other fetus","238"],
["DX","O361130","Maternal care for Anti-A sensitization, third trimester, not applicable or unspecified","238"],
["DX","O361131","Maternal care for Anti-A sensitization, third trimester, fetus 1","238"],
["DX","O361132","Maternal care for Anti-A sensitization, third trimester, fetus 2","238"],
["DX","O361133","Maternal care for Anti-A sensitization, third trimester, fetus 3","238"],
["DX","O361134","Maternal care for Anti-A sensitization, third trimester, fetus 4","238"],
["DX","O361135","Maternal care for Anti-A sensitization, third trimester, fetus 5","238"],
["DX","O361139","Maternal care for Anti-A sensitization, third trimester, other fetus","238"],
["DX","O361930","Maternal care for other isoimmunization, third trimester, not applicable or unspecified","238"],
["DX","O361931","Maternal care for other isoimmunization, third trimester, fetus 1","238"],
["DX","O361932","Maternal care for other isoimmunization, third trimester, fetus 2","238"],
["DX","O361933","Maternal care for other isoimmunization, third trimester, fetus 3","238"],
["DX","O361934","Maternal care for other isoimmunization, third trimester, fetus 4","238"],
["DX","O361935","Maternal care for other isoimmunization, third trimester, fetus 5","238"],
["DX","O361939","Maternal care for other isoimmunization, third trimester, other fetus","238"],
["DX","O3623X0","Maternal care for hydrops fetalis, third trimester, not applicable or unspecified","238"],
["DX","O3623X1","Maternal care for hydrops fetalis, third trimester, fetus 1","238"],
["DX","O3623X2","Maternal care for hydrops fetalis, third trimester, fetus 2","238"],
["DX","O3623X3","Maternal care for hydrops fetalis, third trimester, fetus 3","238"],
["DX","O3623X4","Maternal care for hydrops fetalis, third trimester, fetus 4","238"],
["DX","O3623X5","Maternal care for hydrops fetalis, third trimester, fetus 5","238"],
["DX","O3623X9","Maternal care for hydrops fetalis, third trimester, other fetus","238"],
["DX","O365130","Maternal care for known or suspected placental insufficiency, third trimester, not applicable or unspecified","238"],
["DX","O365131","Maternal care for known or suspected placental insufficiency, third trimester, fetus 1","238"],
["DX","O365132","Maternal care for known or suspected placental insufficiency, third trimester, fetus 2","238"],
["DX","O365133","Maternal care for known or suspected placental insufficiency, third trimester, fetus 3","238"],
["DX","O365134","Maternal care for known or suspected placental insufficiency, third trimester, fetus 4","238"],
["DX","O365135","Maternal care for known or suspected placental insufficiency, third trimester, fetus 5","238"],
["DX","O365139","Maternal care for known or suspected placental insufficiency, third trimester, other fetus","238"],
["DX","O365930","Maternal care for other known or suspected poor fetal growth, third trimester, not applicable or unspecified","238"],
["DX","O365931","Maternal care for other known or suspected poor fetal growth, third trimester, fetus 1","238"],
["DX","O365932","Maternal care for other known or suspected poor fetal growth, third trimester, fetus 2","238"],
["DX","O365933","Maternal care for other known or suspected poor fetal growth, third trimester, fetus 3","238"],
["DX","O365934","Maternal care for other known or suspected poor fetal growth, third trimester, fetus 4","238"],
["DX","O365935","Maternal care for other known or suspected poor fetal growth, third trimester, fetus 5","238"],
["DX","O365939","Maternal care for other known or suspected poor fetal growth, third trimester, other fetus","238"],
["DX","O3663X0","Maternal care for excessive fetal growth, third trimester, not applicable or unspecified","238"],
["DX","O3663X1","Maternal care for excessive fetal growth, third trimester, fetus 1","238"],
["DX","O3663X2","Maternal care for excessive fetal growth, third trimester, fetus 2","238"],
["DX","O3663X3","Maternal care for excessive fetal growth, third trimester, fetus 3","238"],
["DX","O3663X4","Maternal care for excessive fetal growth, third trimester, fetus 4","238"],
["DX","O3663X5","Maternal care for excessive fetal growth, third trimester, fetus 5","238"],
["DX","O3663X9","Maternal care for excessive fetal growth, third trimester, other fetus","238"],
["DX","O3673X0","Maternal care for viable fetus in abdominal pregnancy, third trimester, not applicable or unspecified","238"],
["DX","O3673X1","Maternal care for viable fetus in abdominal pregnancy, third trimester, fetus 1","238"],
["DX","O3673X2","Maternal care for viable fetus in abdominal pregnancy, third trimester, fetus 2","238"],
["DX","O3673X3","Maternal care for viable fetus in abdominal pregnancy, third trimester, fetus 3","238"],
["DX","O3673X4","Maternal care for viable fetus in abdominal pregnancy, third trimester, fetus 4","238"],
["DX","O3673X5","Maternal care for viable fetus in abdominal pregnancy, third trimester, fetus 5","238"],
["DX","O3673X9","Maternal care for viable fetus in abdominal pregnancy, third trimester, other fetus","238"],
["DX","O368130","Decreased fetal movements, third trimester, not applicable or unspecified","238"],
["DX","O368131","Decreased fetal movements, third trimester, fetus 1","238"],
["DX","O368132","Decreased fetal movements, third trimester, fetus 2","238"],
["DX","O368133","Decreased fetal movements, third trimester, fetus 3","238"],
["DX","O368134","Decreased fetal movements, third trimester, fetus 4","238"],
["DX","O368135","Decreased fetal movements, third trimester, fetus 5","238"],
["DX","O368139","Decreased fetal movements, third trimester, other fetus","238"],
["DX","O368230","Fetal anemia and thrombocytopenia, third trimester, not applicable or unspecified","238"],
["DX","O368231","Fetal anemia and thrombocytopenia, third trimester, fetus 1","238"],
["DX","O368232","Fetal anemia and thrombocytopenia, third trimester, fetus 2","238"],
["DX","O368233","Fetal anemia and thrombocytopenia, third trimester, fetus 3","238"],
["DX","O368234","Fetal anemia and thrombocytopenia, third trimester, fetus 4","238"],
["DX","O368235","Fetal anemia and thrombocytopenia, third trimester, fetus 5","238"],
["DX","O368239","Fetal anemia and thrombocytopenia, third trimester, other fetus","238"],
["DX","O368330","Maternal care for abnormalities of the fetal heart rate or rhythm, third trimester, not applicable or unspecified","238"],
["DX","O368331","Maternal care for abnormalities of the fetal heart rate or rhythm, third trimester, fetus 1","238"],
["DX","O368332","Maternal care for abnormalities of the fetal heart rate or rhythm, third trimester, fetus 2","238"],
["DX","O368333","Maternal care for abnormalities of the fetal heart rate or rhythm, third trimester, fetus 3","238"],
["DX","O368334","Maternal care for abnormalities of the fetal heart rate or rhythm, third trimester, fetus 4","238"],
["DX","O368335","Maternal care for abnormalities of the fetal heart rate or rhythm, third trimester, fetus 5","238"],
["DX","O368339","Maternal care for abnormalities of the fetal heart rate or rhythm, third trimester, other fetus","238"],
["DX","O368930","Maternal care for other specified fetal problems, third trimester, not applicable or unspecified","238"],
["DX","O368931","Maternal care for other specified fetal problems, third trimester, fetus 1","238"],
["DX","O368932","Maternal care for other specified fetal problems, third trimester, fetus 2","238"],
["DX","O368933","Maternal care for other specified fetal problems, third trimester, fetus 3","238"],
["DX","O368934","Maternal care for other specified fetal problems, third trimester, fetus 4","238"],
["DX","O368935","Maternal care for other specified fetal problems, third trimester, fetus 5","238"],
["DX","O368939","Maternal care for other specified fetal problems, third trimester, other fetus","238"],
["DX","O3693X0","Maternal care for fetal problem, unspecified, third trimester, not applicable or unspecified","238"],
["DX","O3693X1","Maternal care for fetal problem, unspecified, third trimester, fetus 1","238"],
["DX","O3693X2","Maternal care for fetal problem, unspecified, third trimester, fetus 2","238"],
["DX","O3693X3","Maternal care for fetal problem, unspecified, third trimester, fetus 3","238"],
["DX","O3693X4","Maternal care for fetal problem, unspecified, third trimester, fetus 4","238"],
["DX","O3693X5","Maternal care for fetal problem, unspecified, third trimester, fetus 5","238"],
["DX","O3693X9","Maternal care for fetal problem, unspecified, third trimester, other fetus","238"],
["DX","O403XX0","Polyhydramnios, third trimester, not applicable or unspecified","238"],
["DX","O403XX1","Polyhydramnios, third trimester, fetus 1","238"],
["DX","O403XX2","Polyhydramnios, third trimester, fetus 2","238"],
["DX","O403XX3","Polyhydramnios, third trimester, fetus 3","238"],
["DX","O403XX4","Polyhydramnios, third trimester, fetus 4","238"],
["DX","O403XX5","Polyhydramnios, third trimester, fetus 5","238"],
["DX","O403XX9","Polyhydramnios, third trimester, other fetus","238"],
["DX","O4103X0","Oligohydramnios, third trimester, not applicable or unspecified","238"],
["DX","O4103X1","Oligohydramnios, third trimester, fetus 1","238"],
["DX","O4103X2","Oligohydramnios, third trimester, fetus 2","238"],
["DX","O4103X3","Oligohydramnios, third trimester, fetus 3","238"],
["DX","O4103X4","Oligohydramnios, third trimester, fetus 4","238"],
["DX","O4103X5","Oligohydramnios, third trimester, fetus 5","238"],
["DX","O4103X9","Oligohydramnios, third trimester, other fetus","238"],
["DX","O411030","Infection of amniotic sac and membranes, unspecified, third trimester, not applicable or unspecified","238"],
["DX","O411031","Infection of amniotic sac and membranes, unspecified, third trimester, fetus 1","238"],
["DX","O411032","Infection of amniotic sac and membranes, unspecified, third trimester, fetus 2","238"],
["DX","O411033","Infection of amniotic sac and membranes, unspecified, third trimester, fetus 3","238"],
["DX","O411034","Infection of amniotic sac and membranes, unspecified, third trimester, fetus 4","238"],
["DX","O411035","Infection of amniotic sac and membranes, unspecified, third trimester, fetus 5","238"],
["DX","O411039","Infection of amniotic sac and membranes, unspecified, third trimester, other fetus","238"],
["DX","O411230","Chorioamnionitis, third trimester, not applicable or unspecified","238"],
["DX","O411231","Chorioamnionitis, third trimester, fetus 1","238"],
["DX","O411232","Chorioamnionitis, third trimester, fetus 2","238"],
["DX","O411233","Chorioamnionitis, third trimester, fetus 3","238"],
["DX","O411234","Chorioamnionitis, third trimester, fetus 4","238"],
["DX","O411235","Chorioamnionitis, third trimester, fetus 5","238"],
["DX","O411239","Chorioamnionitis, third trimester, other fetus","238"],
["DX","O411430","Placentitis, third trimester, not applicable or unspecified","238"],
["DX","O411431","Placentitis, third trimester, fetus 1","238"],
["DX","O411432","Placentitis, third trimester, fetus 2","238"],
["DX","O411433","Placentitis, third trimester, fetus 3","238"],
["DX","O411434","Placentitis, third trimester, fetus 4","238"],
["DX","O411435","Placentitis, third trimester, fetus 5","238"],
["DX","O411439","Placentitis, third trimester, other fetus","238"],
["DX","O418X30","Other specified disorders of amniotic fluid and membranes, third trimester, not applicable or unspecified","238"],
["DX","O418X31","Other specified disorders of amniotic fluid and membranes, third trimester, fetus 1","238"],
["DX","O418X32","Other specified disorders of amniotic fluid and membranes, third trimester, fetus 2","238"],
["DX","O418X33","Other specified disorders of amniotic fluid and membranes, third trimester, fetus 3","238"],
["DX","O418X34","Other specified disorders of amniotic fluid and membranes, third trimester, fetus 4","238"],
["DX","O418X35","Other specified disorders of amniotic fluid and membranes, third trimester, fetus 5","238"],
["DX","O418X39","Other specified disorders of amniotic fluid and membranes, third trimester, other fetus","238"],
["DX","O4193X0","Disorder of amniotic fluid and membranes, unspecified, third trimester, not applicable or unspecified","238"],
["DX","O4193X1","Disorder of amniotic fluid and membranes, unspecified, third trimester, fetus 1","238"],
["DX","O4193X2","Disorder of amniotic fluid and membranes, unspecified, third trimester, fetus 2","238"],
["DX","O4193X3","Disorder of amniotic fluid and membranes, unspecified, third trimester, fetus 3","238"],
["DX","O4193X4","Disorder of amniotic fluid and membranes, unspecified, third trimester, fetus 4","238"],
["DX","O4193X5","Disorder of amniotic fluid and membranes, unspecified, third trimester, fetus 5","238"],
["DX","O4193X9","Disorder of amniotic fluid and membranes, unspecified, third trimester, other fetus","238"],
["DX","O43013","Fetomaternal placental transfusion syndrome, third trimester","238"],
["DX","O43023","Fetus-to-fetus placental transfusion syndrome, third trimester","238"],
["DX","O43103","Malformation of placenta, unspecified, third trimester","238"],
["DX","O43113","Circumvallate placenta, third trimester","238"],
["DX","O43123","Velamentous insertion of umbilical cord, third trimester","238"],
["DX","O43193","Other malformation of placenta, third trimester","238"],
["DX","O43213","Placenta accreta, third trimester","238"],
["DX","O43223","Placenta increta, third trimester","238"],
["DX","O43233","Placenta percreta, third trimester","238"],
["DX","O43813","Placental infarction, third trimester","238"],
["DX","O43893","Other placental disorders, third trimester","238"],
["DX","O4393","Unspecified placental disorder, third trimester","238"],
["DX","O4403","Complete placenta previa NOS or without hemorrhage, third trimester","238"],
["DX","O4413","Complete placenta previa with hemorrhage, third trimester","238"],
["DX","O4423","Partial placenta previa NOS or without hemorrhage, third trimester","238"],
["DX","O4433","Partial placenta previa with hemorrhage, third trimester","238"],
["DX","O4443","Low lying placenta NOS or without hemorrhage, third trimester","238"],
["DX","O4453","Low lying placenta with hemorrhage, third trimester","238"],
["DX","O45003","Premature separation of placenta with coagulation defect, unspecified, third trimester","238"],
["DX","O45013","Premature separation of placenta with afibrinogenemia, third trimester","238"],
["DX","O45023","Premature separation of placenta with disseminated intravascular coagulation, third trimester","238"],
["DX","O45093","Premature separation of placenta with other coagulation defect, third trimester","238"],
["DX","O458X3","Other premature separation of placenta, third trimester","238"],
["DX","O4593","Premature separation of placenta, unspecified, third trimester","238"],
["DX","O46003","Antepartum hemorrhage with coagulation defect, unspecified, third trimester","238"],
["DX","O46013","Antepartum hemorrhage with afibrinogenemia, third trimester","238"],
["DX","O46023","Antepartum hemorrhage with disseminated intravascular coagulation, third trimester","238"],
["DX","O46093","Antepartum hemorrhage with other coagulation defect, third trimester","238"],
["DX","O468X3","Other antepartum hemorrhage, third trimester","238"],
["DX","O4693","Antepartum hemorrhage, unspecified, third trimester","238"],
["DX","O4703","False labor before 37 completed weeks of gestation, third trimester","238"],
["DX","O6003","Preterm labor without delivery, third trimester","238"],
["DX","O7103","Rupture of uterus before onset of labor, third trimester","238"],
["DX","O88013","Air embolism in pregnancy, third trimester","238"],
["DX","O88113","Amniotic fluid embolism in pregnancy, third trimester","238"],
["DX","O88213","Thromboembolism in pregnancy, third trimester","238"],
["DX","O88313","Pyemic and septic embolism in pregnancy, third trimester","238"],
["DX","O88813","Other embolism in pregnancy, third trimester","238"],
["DX","O91013","Infection of nipple associated with pregnancy, third trimester","238"],
["DX","O91113","Abscess of breast associated with pregnancy, third trimester","238"],
["DX","O91213","Nonpurulent mastitis associated with pregnancy, third trimester","238"],
["DX","O92013","Retracted nipple associated with pregnancy, third trimester","238"],
["DX","O92113","Cracked nipple associated with pregnancy, third trimester","238"],
["DX","O98013","Tuberculosis complicating pregnancy, third trimester","238"],
["DX","O98113","Syphilis complicating pregnancy, third trimester","238"],
["DX","O98213","Gonorrhea complicating pregnancy, third trimester","238"],
["DX","O98313","Other infections with a predominantly sexual mode of transmission complicating pregnancy, third trimester","238"],
["DX","O98413","Viral hepatitis complicating pregnancy, third trimester","238"],
["DX","O98513","Other viral diseases complicating pregnancy, third trimester","238"],
["DX","O98613","Protozoal diseases complicating pregnancy, third trimester","238"],
["DX","O98713","Human immunodeficiency virus [HIV] disease complicating pregnancy, third trimester","238"],
["DX","O98813","Other maternal infectious and parasitic diseases complicating pregnancy, third trimester","238"],
["DX","O98913","Unspecified maternal infectious and parasitic disease complicating pregnancy, third trimester","238"],
["DX","O99013","Anemia complicating pregnancy, third trimester","238"],
["DX","O99113","Other diseases of the blood and blood-forming organs and certain disorders involving the immune mechanism complicating pregnancy, third trimester","238"],
["DX","O99213","Obesity complicating pregnancy, third trimester","238"],
["DX","O99283","Endocrine, nutritional and metabolic diseases complicating pregnancy, third trimester","238"],
["DX","O99313","Alcohol use complicating pregnancy, third trimester","238"],
["DX","O99323","Drug use complicating pregnancy, third trimester","238"],
["DX","O99333","Smoking (tobacco) complicating pregnancy, third trimester","238"],
["DX","O99343","Other mental disorders complicating pregnancy, third trimester","238"],
["DX","O99353","Diseases of the nervous system complicating pregnancy, third trimester","238"],
["DX","O99413","Diseases of the circulatory system complicating pregnancy, third trimester","238"],
["DX","O99513","Diseases of the respiratory system complicating pregnancy, third trimester","238"],
["DX","O99613","Diseases of the digestive system complicating pregnancy, third trimester","238"],
["DX","O99713","Diseases of the skin and subcutaneous tissue complicating pregnancy, third trimester","238"],
["DX","O99843","Bariatric surgery status complicating pregnancy, third trimester","238"],
["DX","O9A113","Malignant neoplasm complicating pregnancy, third trimester","238"],
["DX","O9A213","Injury, poisoning and certain other consequences of external causes complicating pregnancy, third trimester","238"],
["DX","O9A313","Physical abuse complicating pregnancy, third trimester","238"],
["DX","O9A413","Sexual abuse complicating pregnancy, third trimester","238"],
["DX","O9A513","Psychological abuse complicating pregnancy, third trimester","238"],
["DX","Z3403","Encounter for supervision of normal first pregnancy, third trimester","238"],
["DX","Z3483","Encounter for supervision of other normal pregnancy, third trimester","238"],
["DX","Z3493","Encounter for supervision of normal pregnancy, unspecified, third trimester","238"],
["DX","Z3A08","8 weeks gestation of pregnancy","56"],
["DX","Z3A09","9 weeks gestation of pregnancy","63"],
["DX","Z3A10","10 weeks gestation of pregnancy","70"],
["DX","Z3A11","11 weeks gestation of pregnancy","77"],
["DX","Z3A12","12 weeks gestation of pregnancy","84"],
["DX","Z3A13","13 weeks gestation of pregnancy","91"],
["DX","Z3A14","14 weeks gestation of pregnancy","98"],
["DX","Z3A15","15 weeks gestation of pregnancy","105"],
["DX","Z3A16","16 weeks gestation of pregnancy","112"],
["DX","Z3A17","17 weeks gestation of pregnancy","119"],
["DX","Z3A18","18 weeks gestation of pregnancy","126"],
["DX","Z3A19","19 weeks gestation of pregnancy","133"],
["DX","Z3A20","20 weeks gestation of pregnancy","140"],
["DX","Z3A21","21 weeks gestation of pregnancy","147"],
["DX","Z3A22","22 weeks gestation of pregnancy","154"],
["DX","Z3A23","23 weeks gestation of pregnancy","161"],
["DX","Z3A24","24 weeks gestation of pregnancy","168"],
["DX","Z3A25","25 weeks gestation of pregnancy","175"],
["DX","Z3A26","26 weeks gestation of pregnancy","182"],
["DX","Z3A27","27 weeks gestation of pregnancy","189"],
["DX","Z3A28","28 weeks gestation of pregnancy","196"],
["DX","Z3A29","29 weeks gestation of pregnancy","203"],
["DX","Z3A30","30 weeks gestation of pregnancy","210"],
["DX","Z3A31","31 weeks gestation of pregnancy","217"],
["DX","Z3A32","32 weeks gestation of pregnancy","224"],
["DX","Z3A33","33 weeks gestation of pregnancy","231"],
["DX","Z3A34","34 weeks gestation of pregnancy","238"],
["DX","Z3A35","35 weeks gestation of pregnancy","245"],
["DX","Z3A36","36 weeks gestation of pregnancy","252"],
["DX","Z3A37","37 weeks gestation of pregnancy","259"],
["DX","Z3A38","38 weeks gestation of pregnancy","266"],
["DX","Z3A39","39 weeks gestation of pregnancy","273"],
["DX","Z3A40","40 weeks gestation of pregnancy","280"],
["DX","Z3A41","41 weeks gestation of pregnancy","287"],
["DX","Z3A42","42 weeks gestation of pregnancy","294"],
["DX","Z3A49","Greater than 42 weeks gestation of pregnancy","294"]

                    ]

columns = ["Type_of_Code", "code",	"Description", "Time_in_days_string"]

    

# COMMAND ----------

#spark_lookUp = SparkSession.builder.appName('sparkdf').getOrCreate()
LookUpCodes_sdf = (
    spark.createDataFrame(LookUpCodes, columns)
    .select( "*",F.col("Time_in_days_string").cast('int').alias("Time_in_days"))
    .drop("Time_in_days_string")
)



# Dataframe for codes and time in days. This would be used later to estimate the start of the pregnancy. The codes that are in weeks are transformed back into days
                                                                     # For the codes that are in trimesters, the midpoint of the trimester in days is used       
display(LookUpCodes_sdf)


# COMMAND ----------

#spark.sql("""DROP TABLE IF EXISTS cdh_hv_mother_baby_covid_exploratory.time_to_delivery_run9""")
#LookUpCodes_sdf.write.mode("overwrite").saveAsTable("cdh_hv_mother_baby_covid_exploratory.time_to_delivery_run9") # Saving table for later use, it can be accessed by other analyst

# COMMAND ----------

# MAGIC %md ## Identify episodes of pregnancy and assign delivery codes

# COMMAND ----------

cols_med_claims = ['hvid', 'diagnosis_code', 'procedure_code', 'date_service', 'data_vendor', 'logical_delete_reason']


covid_mom_mc = spark.read.table("cdh_hv_mother_baby_covid.mom_medical_claims").select(*cols_med_claims)
covid_vaccine_mom_mc = spark.read.table("cdh_hv_mother_baby_covid_vaccine.mom_medical_claims").select(*cols_med_claims)
mom_comparator1 = spark.read.table("cdh_hv_mother_baby_comparator.mom_medical_claims").select(*cols_med_claims)
mom_comparator2 = spark.read.table("cdh_hv_mother_baby_mom_comparator2.mom_medical_claims").select(*cols_med_claims)
   

mc = covid_mom_mc.unionByName(covid_vaccine_mom_mc).unionByName(mom_comparator1).unionByName(mom_comparator2).dropDuplicates()#.createOrReplaceTempView("medical_claims_preg_all_MOMs_cohorts")

today = date.today().strftime('%m_%d_%Y')

# COMMAND ----------

columns_mc = ['hvid', 'diagnosis_code', 'procedure_code', 'date_service']

endpoint_codes = [
    'O7582', 'O80', 'Z370', 'Z371', 'Z379', '10D00Z0', '10D00Z1', 
    '10D00Z2', '10D07Z3', '10D07Z4', '10D07Z5', '10D07Z6', '10D07Z7', 
    '10D07Z8', '10E0XZZ', '765', '766', '767',  '768', '774', '775', 
    '783' , '784', '785', '786', '787', '788', '796', '797', '798', 
    '805', '806', '807'
]

unified_codes = (
    #spark.read.table("""cdh_hv_mother_baby_covid.mom_medical_claims""")
    mc
    .filter(ps20_med_cond) 
    .select(columns_mc)
    .withColumn("dxc_pc",F.coalesce("procedure_code","diagnosis_code"))
    .withColumnRenamed("date_service" ,"dx_date") 
)

unified_codes.cache()

pregnancy_endpoints = (
     unified_codes       
    .withColumn("dxc_pc_endpoint", 
                F.when(F.col("dxc_pc").isin(endpoint_codes), F.col("dxc_pc")).otherwise(None)
               )
    .withColumn("dxc_pc_endpoint_date", 
                F.when(F.col("dxc_pc").isin(endpoint_codes), F.col("dx_date")).otherwise(None)
               )
)

pregnancy_outcomes = (    
    unified_codes
    .select("hvid", "dxc_pc","dx_date")
    .join(spark_outcome_codes_sdf
          .select("code","Pregnancy_Outcome"), unified_codes.dxc_pc == spark_outcome_codes_sdf.code)
    .drop("code")
    .groupBy("hvid","dxc_pc","dx_date","Pregnancy_Outcome")
    .agg(
        F.collect_list("Pregnancy_Outcome").alias("allOucomes_endpoint")
    )
)

pregnancy_endpoints_outcomes = (
    pregnancy_endpoints.select("hvid","dxc_pc_endpoint","dxc_pc_endpoint_date")
    .join(pregnancy_outcomes, (pregnancy_endpoints.hvid == pregnancy_outcomes.hvid) & (F.datediff("dx_date", "dxc_pc_endpoint_date")).between(0,4))
    .drop(pregnancy_outcomes.hvid)
    .sort("hvid","dxc_pc_endpoint_date","dx_date")
)

windowSpec = Window.partitionBy("hvid","dxc_pc_endpoint_date","outcome_candidate_date").orderBy("Pregnancy_Outcome","outcome_candidate_date")

pregnancy_endpoints_outcomes_dedupled = (
    pregnancy_endpoints_outcomes
    .withColumnRenamed("dx_date", "outcome_candidate_date")
    .withColumn("rn",
                row_number().over(windowSpec)                
               )
    
    .filter("rn = 1")
)

#display(pregnancy_endpoints_outcomes_dedupled.where("hvid = '627a3e00365094159eb2b79126858e8b'"))
#display(pregnancy_endpoints_outcomes_dedupled.filter("rn = 1 and hvid = '0000066eb19be1d325378a2de9c3ee8e'"))




# COMMAND ----------

#allvisits_spf = spark.table("r_1_identified_outcome")
spark.sql("""DROP TABLE IF EXISTS cdh_hv_mother_baby_covid_exploratory.endpoints_outcomes_run9""")
pregnancy_endpoints_outcomes_dedupled.write.mode("overwrite").saveAsTable("cdh_hv_mother_baby_covid_exploratory.endpoints_outcomes_run9") # saving results

# COMMAND ----------

pregnancy_endpoints_outcomes_dedupled.unpersist()

# COMMAND ----------

""" 
    load the data in case we do not want to recompute all the data from before 

"""
allvisits_spf = pregnancy_endpoints_outcomes_dedupled
allvisits_spf = spark.read.table("""cdh_hv_mother_baby_covid_exploratory.endpoints_outcomes_run9""")


# COMMAND ----------

# MAGIC %md # Identifying outcome at delivery 

# COMMAND ----------

out_del = (
    allvisits_spf
    .withColumnRenamed("dxc_pc", "out_candidate_code")
    .withColumn('end_date_endpoint', F.date_add(F.col("outcome_candidate_date"), days=4))   
    .drop("rn")
)


# COMMAND ----------

# MAGIC %md #Endpoint ID and outcome
# MAGIC For defining an outcome to an endpoint. Then we will use the distance to fnd individual episodes

# COMMAND ----------


w2 = Window.partitionBy("hvid").orderBy(F.monotonically_increasing_id())
w3 = Window.partitionBy("hvid","endpointNum").orderBy("Pregnancy_Outcome","dxc_pc_endpoint_date","outcome_candidate_date")
endpointID = (
  out_del
    .sort("hvid","dxc_pc_endpoint_date", "outcome_candidate_date","Pregnancy_Outcome")
    .withColumn(
        "seq",
        (F.sequence("outcome_candidate_date", "end_date_endpoint")) 
    )
    .withColumn(
      "lag",
      F.arrays_overlap("seq",F.lag("seq").over(w2)) 
      
    )
    .withColumn(
      "flag",
      F.when( 
          (F.col("lag") | F.col("lag").isNull()),
        1
      ).otherwise(0)
    )
    .withColumn('flag_lag',
                     F.lag('flag').over(w2)
               )
    .withColumn('endpointNum',
              F.sum(F.when(F.col('flag') >= F.col('flag_lag'), 0).otherwise(1)).over(w2)
             ).drop('seq', 'lag', 'flag', 'flag_lag')
    #.withColumn('rankOutcome', 
    #            F.dense_rank().over(w3) #check what this is doing
    #           )
    .withColumn('row_num', F.row_number().over(w3) #check what this is doing
               )              
)

endpointID_outcome = endpointID.select("*").where('row_num = 1')


# COMMAND ----------

#display(endpointID.where("endpointNum > 2"))

# COMMAND ----------

"""Assigning an episode ID column to the data set"""

# Using Moll et all (2021) supplemental table 4 https://static-content.springer.com/esm/art%3A10.1007%2Fs40264-021-01113-8/MediaObjects/40264_2021_1113_MOESM1_ESM.pdf

numberEndPoints_ID = endpointID_outcome.select(
    "hvid",
    "endpointNum",
    concat_ws("_", "hvid", "endpointNum").alias("endpointNum_ID"),
    "outcome_candidate_date",
    "out_candidate_code",
    "Pregnancy_Outcome",
).withColumn(
    "est_start_preg_out_candidate_UB",
    F.when(
        F.col("Pregnancy_Outcome") == "A-LB", F.date_sub("outcome_candidate_date", 154)
    )
    .when(
        (F.col("Pregnancy_Outcome") == "B-SB")
        | (F.col("Pregnancy_Outcome") == "Unknown"),
        F.date_sub("outcome_candidate_date", 140),
    )
    .when(
        (F.col("Pregnancy_Outcome") == "C-ECT")
        | (F.col("Pregnancy_Outcome") == "D-AB"),
        F.date_sub("outcome_candidate_date", 42),
    )
    .when(
        (F.col("Pregnancy_Outcome") == "D-SAB"),
        F.date_sub("outcome_candidate_date", 28),
    ),
).withColumn(
    "est_start_preg_out_candidate_LB",
    F.when(
        F.col("Pregnancy_Outcome") == "A-LB", F.date_sub("outcome_candidate_date", 301)
    )
    .when(
        (F.col("Pregnancy_Outcome") == "B-SB")
        | (F.col("Pregnancy_Outcome") == "Unknown"),
        F.date_sub("outcome_candidate_date", 301),
    )
    .when(
        (F.col("Pregnancy_Outcome") == "C-ECT"),
        F.date_sub("outcome_candidate_date", 84),
    )
    .when(        
        (F.col("Pregnancy_Outcome") == "D-AB"),
        F.date_sub("outcome_candidate_date", 168),
    )
    .when(
        (F.col("Pregnancy_Outcome") == "D-SAB"),
        F.date_sub("outcome_candidate_date", 133),
    ),
)
#display(numberEndPoints_ID)

# COMMAND ----------

#display(numberEndPoints_ID.where("hvid = '265adc513934055d6ead852b3ea924f1'")) #SAB code

# COMMAND ----------

medClaims = (
    unified_codes
)

#display(unified_codes)

# COMMAND ----------


spark.sql("""DROP TABLE IF EXISTS cdh_hv_mother_baby_covid_exploratory.med_claims_run9""")
medClaims.write.mode("overwrite").saveAsTable("cdh_hv_mother_baby_covid_exploratory.med_claims_run9") # saving results

# COMMAND ----------

medClaims.unpersist()

# COMMAND ----------

medClaims = spark.read.table("cdh_hv_mother_baby_covid_exploratory.med_claims_run9")

# COMMAND ----------

GA_codes_list = list(LookUpCodes_sdf.select("code").toPandas()["code"])


# COMMAND ----------

# Before comparing time between episodes
# Look at gestational age codes in terms still births (>=20 weeks) and SAB (<20 weeks)

"""
    This piece of code makes a few things 
    
    Rename some of the columns to clarify they are from medical claims that we will join later. 
    The objective of such join is to find gestational age codes when possible.
    
    We take the endpoint as reference and find all the gestational age codes that are within 90 earlier than the outcome and 20 days after
    
    Note: abortions are unlikely to have gestational age related code, then we should substract the correct ammount of time manually (need to update this part)
 
"""

#w1 = Window.partitionBy("hvid").orderBy(endpoint_outcome_deduped_claims.outcome_candidate_date)

mapping = dict(zip(["dx_date", "dxc_pc"], ["dx_date_medClaim", "dxc_pc_medClaim"]))

medClaims2 = (
    medClaims.select(
                    [F.col(c).alias(mapping.get(c, c)) for c in medClaims.columns] # filter here using GA codes (pass it as a list to improve efficiency of filter when joining)
                    )
).filter(F.col("dxc_pc_medClaim").isin(GA_codes_list))

endpoint_outcome_deduped_claims = (
    numberEndPoints_ID.join(
        medClaims2, numberEndPoints_ID.hvid == medClaims2.hvid, "left" # getting all patient's claims
    )
    .drop(medClaims2.hvid)
    .drop("diagnosis_code")
    .drop("procedure_code")
)

endpoint_outcome_deduped_claims_GA = ( # finding all GA related codes and doing left join so we can keep track of those who did not have a GA code
    endpoint_outcome_deduped_claims.join(
        LookUpCodes_sdf,
        endpoint_outcome_deduped_claims.dxc_pc_medClaim == LookUpCodes_sdf.code,
        "left", # need left join because px may not have GA code
    )
    # The GA code should be earlier than endpoint/outcome date but because it is claims data it may be assigned after outcome for example (also if it is assign after outcome it could mean the pregnancy is still in progress at the time)
    .withColumnRenamed("code", "GA_code")
    .drop("Description")
    .drop("Trimester")
    .drop("Type_of_Code")
)


endpoint_outcome_deduped_claims_GA = endpoint_outcome_deduped_claims_GA.selectExpr( # Substract time in days to ontain estimated start of pregnancy
    "*", "date_sub(outcome_candidate_date, cast(time_in_days as int)) as est_start_date_temp"
)  # estimating start of the pregnancy using GA codes based on outcome date

endpoint_outcome_deduped_claims = (
    endpoint_outcome_deduped_claims_GA.withColumn(
        "date_diff_outcome_GA_claim",
        F.datediff(F.col("outcome_candidate_date"), F.col("dx_date_medClaim")),
    )# how far are we looking for data
    .filter(F.col("date_diff_outcome_GA_claim") <= 30) # midpoint of thid trimester is week 34, meaning 238 days. 280-238 days is 42 days. Making it 50 to capture third trimster codes
    .filter(F.col("date_diff_outcome_GA_claim") >= -20)
    # Getting time
    .dropDuplicates()
    .drop("Trimester_string")
    .drop("dxc_pc_medClaim")
    .drop("out_candidate_code")
    .withColumn(
        "consistency_l_back_period",
        F.when(
            F.col("est_start_date_temp")
            .between(
                F.col("est_start_preg_out_candidate_LB"),
                F.col("est_start_preg_out_candidate_UB"),
            ),
            1,
        ).otherwise(0),
    )
    .withColumn("GA_code_priority", # maybe I do not need the now number, just assign a number to claims with Z3A codes like 1 and any other GA code as 2 (making more reliable 1 and if if does not find use anything else (largest so it goes earlier, or maybe median?))
                F.when(F.col("GA_code").like("Z3A%"),1)
                .when((~F.col("GA_code").like("Z3A%")),2)
                .otherwise(3)                      
               )
    .withColumn("preffered_GA_est_start_preg", # creating window partition to select the most suitable code for estimating start of pregnancy 
               F.row_number().over(Window.partitionBy("endpointNum_ID").orderBy(F.col("GA_code_priority").asc(), F.col("Time_in_days").desc(),F.abs(F.col("date_diff_outcome_GA_claim")).asc()))               
               )
    .filter("preffered_GA_est_start_preg = 1")
    .withColumn("cases_preg", #check on upper and lower bounds for discrepancies, accept the ones that are within the bounds, flag the ones that are outside for possible outcome reassignment or errors
               F.when((F.col("consistency_l_back_period") == 1) & (F.col("GA_code_priority") == 1), 'accept_ZA_code')
                .when((F.col("consistency_l_back_period") == 0) & (F.col("GA_code_priority") == 1), 'accept_ZA_code_possible_conflict')
                .when((F.col("consistency_l_back_period") == 1) & (F.col("GA_code_priority") == 2), 'candidate1_inputation') # consistent but with some reservetations in the espisode
                .when((F.col("consistency_l_back_period") == 0) & (F.col("GA_code_priority") == 2), 'definetely_inpute') # no consistent, maybe error in claim coding 
                .otherwise("another_case")
               )
    .withColumnRenamed("outcome_candidate_date", "dx_date_outcome")  
    .withColumnRenamed("est_start_date_temp","est_start_date")
)

#note: look at the proportion of endpoins that have a GA related code, that may let me know how complete the data is so I could adjust tomewindow from endpoint accordingly...

# COMMAND ----------

#display(
#    endpoint_outcome_deduped_claims.where("""hvid in ('007c9dd1740b897d3ed20ed8d5d9894a',
#                                              '0136eb8c57818ac99cc4b908cd20ebe5',
#                                              '123e32cd18f66a32e8738abef397cec4',
#                                              '4a76f635a7de86d4bf89b5239236fe88',
#                                              '107d5a37918c0b337a626bb81996c9e9',


                                             #'08462c8757fef59942e15d6fd2980ddd',
                                             #'0b79578c3a9bb8130ee27e6144db5057')""").sort("endpointNum_ID")
#)


# COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS cdh_hv_mother_baby_covid_exploratory.test_endpoint_outcome_deduped_run9""")
endpoint_outcome_deduped_claims.write.mode("overwrite").format('parquet').saveAsTable("cdh_hv_mother_baby_covid_exploratory.test_endpoint_outcome_deduped_run9")

# COMMAND ----------

endpoint_outcome_deduped_claims.unpersist()

# COMMAND ----------

endpoint_outcome_deduped_claims = spark.read.table("cdh_hv_mother_baby_covid_exploratory.test_endpoint_outcome_deduped_run9")

# COMMAND ----------

endpoint_outcome_deduped_claims.where("est_start_date is null").count()


# COMMAND ----------

endpoint_outcome_deduped_claims.cache()

# COMMAND ----------


"""
    Overall this piece of code first creates a partition and organizes the hvid sorting them by date.
    
    Then, for every patient based on the 'current' outcome and the 'next' outcome (and their corresponding dates)
    the dates that belong to a single episode are grouped together and including them into a single episode.
    
    Every episode is assigned a number 
"""

w2 = Window.partitionBy("hvid").orderBy(F.monotonically_increasing_id())

numberOfEpisodes_endPoints = (
  endpoint_outcome_deduped_claims
    .sort("hvid", "dx_date_outcome")
    .withColumn("seq",
                (F.sequence("est_start_date", "dx_date_outcome"))
               )
    .withColumn("lag_overlap",
                F.arrays_overlap("seq",F.lag("seq").over(w2))
               )
    .withColumn("lag_intersect_date_n_days",
               F.size(F.array_intersect("seq", F.lag("seq").over(w2)))
               )
    .withColumn(
      "flag_overlap",
      F.when(
        
          ((F.col("lag_overlap") == 'false') | (F.col("lag_overlap").isNull()) | F.col("lag_intersect_date_n_days").between(1,5)), # maximun allowed overlap is half of a quarter (must start at 1)
        1
      ).otherwise(0)
    )
    .withColumn('flag_lag_overlap',
                     F.lag('flag_overlap').over(w2))
    .withColumn('pregNum_temp',
              F.sum(F.when(F.col('flag_overlap') >= F.col('flag_lag_overlap'), 1).otherwise(0)).over(w2)
             )#.drop('seq', 'lag', 'flag', 'flag_lag') 
    .withColumn("pregNum", F.col("pregNum_temp")+F.lit(1))
    .drop('seq')    
    .groupBy("hvid","pregNum")
     .agg(       
       F.collect_list("est_start_date").alias("start_dates"),
       F.max("est_start_date").alias("est_start_date"),  
       F.collect_set("endpointNum_ID").alias("possibleEndpoints"),  
       F.collect_list("dx_date_outcome").alias("dx_date_outcomes"),
       F.max('dx_date_outcome').alias('outcome_date'),              
       F.collect_list("Pregnancy_Outcome").alias("Pregnancy_Outcomes"),  
       F.last('Pregnancy_Outcome').alias('Pregnancy_Outcome'),
       F.collect_list("cases_preg").alias("cases_preg"),
       F.collect_list("Time_in_days").alias("times_in_days"),
       F.size(F.collect_set("Pregnancy_Outcome").alias("times_in_days")).alias("n_outcomes_c")  
     )
    
)

#display(
#    numberOfEpisodes_endPoints
#    .withColumn("preg_lenght",
#              F.datediff("outcome_date","est_start_date")
             # )    
    #.groupBy("Pregnancy_Outcome","preg_lenght").count()
#)



#numberOfEpisodes_endPoints.filter("num_outcomes_per_epmore_than_an_outcome > 1").count()



# COMMAND ----------

display(
    numberOfEpisodes_endPoints
    .withColumn("preg_lenght",
              F.datediff("outcome_date","est_start_date")
              )
    #.where("Pregnancy_Outcome in ('B-SB','D-SAB') ")
    .groupBy("Pregnancy_Outcome").count()
)

# COMMAND ----------

"""
    This piece of code adjust outcomes based on gestational age, in this case we reassign abortions to be less than 20 weeks and stillbirth greater or equal to 20 weeks 
    
    SAB and SB, combination of episodes 
    
"""

codes_sab_sb_redef = (
    numberOfEpisodes_endPoints.withColumn(
        "l_of_preg", F.datediff("outcome_date", "est_start_date")
    )
    .withColumn(
        "new_adjusted_outcome",
        F.when(
            (F.col("l_of_preg") < 140) & (F.col("Pregnancy_Outcome") == "B-SB"),
            "D-SAB",
        )
        .when(
            (F.col("l_of_preg") >= 140) & (F.col("Pregnancy_Outcome") == "D-SAB"),
            "B-SB",
        )        
        .otherwise(F.col("Pregnancy_Outcome")),
    )
    .withColumn(
        "flag_ab_sb",
        F.when(
            F.col("new_adjusted_outcome") != F.col("Pregnancy_Outcome"), 1
        ).otherwise(0),
    )
)

# COMMAND ----------

endpointID_outcome = (
    codes_sab_sb_redef
    .withColumnRenamed("outcome_candidate_date", "dx_date_outcome")
    .drop("Pregnancy_outcome")  
    .withColumnRenamed("new_adjusted_outcome", "Pregnancy_Outcome")
)


# COMMAND ----------

display(
    endpointID_outcome
    .groupBy("cases_preg", "Pregnancy_Outcome")
    .count()
)

# COMMAND ----------

# MAGIC %md ## Imputing GA to endpoints that are missing 

# COMMAND ----------

# impute start of pregnancy based on outcome

# Based on informaion given in Bailey's Wallace algorithm and moll definition of minimum pregnancy term plus second trimester midpoint code (133 days)

# Here we only need to impute the live births and ectopic pregnancies because there are instances where LB have codes that are not as reliable for GA, so it is preferred to inpite them in this case. We need to flag in the next steps situation where 1) episode lenght is inconsitent (<133 days) or they conflict with subsequent episidodes

# I does not make sense to input unknow outcome because we do not know what we would be imputing, also SAB, AB and SB are already rearranged at this step based in GA. Thus proceed to flag conflicting codes between episodes

endpointID_outcome2 = (
    endpointID_outcome
    .select("*", concat_ws('_', 'hvid', 'pregNum').alias('episode_ID'))
    .withColumn(
        "est_start_date_temp",
        F.when(
            (F.array_contains(F.col("cases_preg"), "definetely_inpute") == True)
            & (F.col("Pregnancy_Outcome") == "A-LB")
            & (F.col("n_outcomes_c") == 1)  # most likely to be an actual LB
            & (F.col("l_of_preg").between(133, 154)),
            F.date_sub(F.col("outcome_date"), 168),
        )
        .when(
            (F.col("Pregnancy_Outcome") == "C-ECT")
            & (
                ~(F.col("l_of_preg") <= 84) == True
            ),  # do an array contains for pregnancies that have a LB, and ECT in the same outcome
            F.date_sub(F.col("outcome_date"), 56),
        )
        .otherwise(F.col("est_start_date")),
    )
    .withColumn("l_of_preg2", F.datediff("outcome_date", "est_start_date_temp"))
    .drop("l_of_preg")
    .withColumnRenamed("l_of_preg2", "l_of_preg")
    .withColumn(
        "inconsistent_LB", #Basically, unfeasible LB
        F.when(
            (F.col("l_of_preg") < 130) & (F.col("Pregnancy_Outcome") == "A-LB"), 1
        ).otherwise(0),
    )
)


display(
    endpointID_outcome2
    #.groupBy("inconsistent_LB").count()
)

# COMMAND ----------

"""Checking for preliminary counts"""
display(endpointID_outcome2.groupBy('Pregnancy_Outcome').count().sort('count',ascending = False)) 

# COMMAND ----------

"""
    This piece of code looks at the current outcome and the previous outcome and flags conflicting distances between episodes
     
    All paiwise comparisson are listed here

"""
window_func = Window.partitionBy("hvid").orderBy(
    F.col("hvid").asc(),
    F.col("pregNum").asc(),
    F.col("Pregnancy_Outcome").asc(),
)

cleanOutcome_0 = (
    endpointID_outcome2.withColumn(
        "lag_dx_date",
        F.lag("outcome_date").over(window_func),
    )
    .withColumn(
        "lag_outcome",
        F.lag("Pregnancy_Outcome").over(window_func),
    )
    .withColumn("date_diff", F.datediff("outcome_date", "lag_dx_date"))
    .withColumn(
        "cftng_outcome",
        F.when( ############## LB ###########################
            (F.col("Pregnancy_Outcome") == "A-LB")
            & (F.col("lag_outcome") == "A-LB")
            & (F.col("date_diff") <= 182),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "A-LB")
            & (F.col("lag_outcome") == "B-SB")
            & (F.col("date_diff") <= 168),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "A-LB")
            & (F.col("lag_outcome") == "Unknown")
            & (F.col("date_diff") <= 168),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "A-LB")
            & (F.col("lag_outcome") == "C-ECT")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "A-LB")
            & (F.col("lag_outcome") == "D-AB")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "A-LB")
            & (F.col("lag_outcome") == "D-SAB")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when( ########################## SB ############################################
            (F.col("Pregnancy_Outcome") == "B-SB")
            & (F.col("lag_outcome") == "A-LB")
            & (F.col("date_diff") <= 182),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "B-SB")
            & (F.col("lag_outcome") == "B-SB")
            & (F.col("date_diff") <= 168),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "B-SB")
            & (F.col("lag_outcome") == "Unknown")
            & (F.col("date_diff") <= 168),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "B-SB")
            & (F.col("lag_outcome") == "C-ECT")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "B-SB")
            & (F.col("lag_outcome") == "D-AB")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "B-SB")
            & (F.col("lag_outcome") == "D-SAB")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when( ######################### Unknown ########################
            (F.col("Pregnancy_Outcome") == "Unknown")
            & (F.col("lag_outcome") == "A-LB")
            & (F.col("date_diff") <= 182),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "Unknown")
            & (F.col("lag_outcome") == "B-SB")
            & (F.col("date_diff") <= 168),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "Unknown")
            & (F.col("lag_outcome") == "Unknown")
            & (F.col("date_diff") <= 168),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "Unknown")
            & (F.col("lag_outcome") == "C-ECT")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "Unknown")
            & (F.col("lag_outcome") == "D-AB")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "Unknown")
            & (F.col("lag_outcome") == "D-SAB")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when( ################################### Ectopic ###############################################
            (F.col("Pregnancy_Outcome") == "C-ECT")
            & (F.col("lag_outcome") == "A-LB")
            & (F.col("date_diff") <= 182),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "C-ECT")
            & (F.col("lag_outcome") == "B-SB")
            & (F.col("date_diff") <= 168),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "C-ECT")
            & (F.col("lag_outcome") == "Unknown")
            & (F.col("date_diff") <= 168),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "C-ECT")
            & (F.col("lag_outcome") == "C-ECT")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "C-ECT")
            & (F.col("lag_outcome") == "D-AB")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "C-ECT")
            & (F.col("lag_outcome") == "D-SAB")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when( ##################################### AB #####################################
            (F.col("Pregnancy_Outcome") == "D-AB")
            & (F.col("lag_outcome") == "A-LB")
            & (F.col("date_diff") <= 182),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "D-AB")
            & (F.col("lag_outcome") == "B-SB")
            & (F.col("date_diff") <= 168),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "D-AB")
            & (F.col("lag_outcome") == "Unknown")
            & (F.col("date_diff") <= 168),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "D-AB")
            & (F.col("lag_outcome") == "C-ECT")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "D-AB")
            & (F.col("lag_outcome") == "D-AB")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "D-AB")
            & (F.col("lag_outcome") == "D-SAB")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when( ############################################## SAB ######################################
            (F.col("Pregnancy_Outcome") == "D-SAB")
            & (F.col("lag_outcome") == "A-LB")
            & (F.col("date_diff") <= 182),
            1,
        )  ##
        .when(
            (F.col("Pregnancy_Outcome") == "D-SAB")
            & (F.col("lag_outcome") == "B-SB")
            & (F.col("date_diff") <= 168),
            1,
        )  ##
        .when(
            (F.col("Pregnancy_Outcome") == "D-SAB")
            & (F.col("lag_outcome") == "Unknown")
            & (F.col("date_diff") <= 168),
            1,
        )  ##
        .when(
            (F.col("Pregnancy_Outcome") == "D-SAB")
            & (F.col("lag_outcome") == "C-ECT")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "D-SAB")
            & (F.col("lag_outcome") == "D-AB")
            & (F.col("date_diff") <= 56),
            1,
        )
        .when(
            (F.col("Pregnancy_Outcome") == "D-SAB")
            & (F.col("lag_outcome") == "D-SAB")
            & (F.col("date_diff") <= 56),
            1,
        )
        .otherwise(0),
    ).filter("inconsistent_LB = 0") # this is filtering out all LB pregnancies that are less than 130 days
)
display(cleanOutcome_0)

# COMMAND ----------

# MAGIC %md Joining with adult hvid, sometimes the codes are assigned to the babies. Some of the conflicting are due to that

# COMMAND ----------

database = [ "cdh_hv_mother_baby_covid", "cdh_hv_mother_baby_covid_vaccine","cdh_hv_mother_baby_comparator","cdh_hv_mother_baby_mom_comparator2"]


masterset_data = []

for i, val in enumerate(database):
    if (val in ["cdh_hv_mother_baby_covid", "cdh_hv_mother_baby_covid_vaccine"]):
        version = '@v8'
    if (val == 'cdh_hv_mother_baby_comparator'):
        version = '@v4'
    if (val == 'cdh_hv_mother_baby_mom_comparator2'):
        version = '@v2'
    masterset_data_data_temp = (
        spark.read.table(val+".mom_masterset"+version)
        .select("adult_hvid",
                "adult_gender",
                "adult_index_age_range",
                "adult_race",
                "child_hvid",
                "child_gender",
                "child_dob",
                "child_dob_qual"
               )             
    )#.withColumn('source',F.lit(val))
    
    masterset_data.append(masterset_data_data_temp)  
    
demographics = reduce(DataFrame.unionAll, masterset_data)

cleanOutcome = demographics.select(F.col("adult_hvid").alias("hvid")).join(cleanOutcome_0, ['hvid']).dropDuplicates()

# COMMAND ----------

display(
    cleanOutcome.groupBy("Pregnancy_Outcome", "inconsistent_LB", "cftng_outcome")
    .count()
    .sort("count", ascending=False)
)

display(
    cleanOutcome.groupBy("Pregnancy_Outcome")
    .count()
    .sort("count", ascending=False)
)

# COMMAND ----------

print("A-LB")
display(cleanOutcome.select("l_of_preg").where("Pregnancy_Outcome = 'A-LB'").summary())
print("B-SB")
display(cleanOutcome.select("l_of_preg").where("Pregnancy_Outcome = 'B-SB'").summary())
print("C-ECT")
display(cleanOutcome.select("l_of_preg").where("Pregnancy_Outcome = 'C-ECT'").summary())
print("D-SAB")
display(cleanOutcome.select("l_of_preg").where("Pregnancy_Outcome = 'D-SAB'").summary())
print("D-AB")
display(cleanOutcome.select("l_of_preg").where("Pregnancy_Outcome = 'D-AB'").summary())
print("Unknown")
display(cleanOutcome.select("l_of_preg").where("Pregnancy_Outcome = 'Unknown'").summary())

# track longest and shortest period of pregnancy

# COMMAND ----------

cleanOutcome.dropDuplicates(['episode_ID']).count()

# COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS cdh_hv_mother_baby_covid_exploratory.pregnancies_MOM_all_cohorts_run9""")
cleanOutcome.write.mode("overwrite").saveAsTable("cdh_hv_mother_baby_covid_exploratory.pregnancies_MOM_all_cohorts_run9_{}".format(today))

# COMMAND ----------

# MAGIC %md #Testing LB accuracy

# COMMAND ----------

display(demographics.select("adult_hvid", "child_hvid", "child_dob", "child_dob_qual").where("child_dob_qual = '1 BIRTH EVENT OBSERVED'"))

# COMMAND ----------

identified_preg_test = (
    spark.read.table("cdh_hv_mother_baby_covid_exploratory.pregnancies_mom_all_cohorts_run9_02_13_2023")
    .select("hvid", "episode_ID", "est_start_date","outcome_date","pregnancy_outcome","cftng_outcome","inconsistent_LB")
)

display(identified_preg_test.select(F.min("outcome_date")))

# COMMAND ----------

birth_events_mom = (
    demographics
    .select("adult_hvid", "child_hvid", "child_dob", "child_dob_qual").where("child_dob_qual = '1 BIRTH EVENT OBSERVED' and child_dob >= '2019-01-01'")
    .withColumn("rn",
                row_number().over(Window.partitionBy("adult_hvid").orderBy("adult_hvid","child_dob"))
               )
    .dropDuplicates(["adult_hvid","child_hvid","child_dob"])
)

display(birth_events_mom)

# COMMAND ----------

#from pregnancy algo to moms_data set
match_preg_algo_birth_mom = (
    identified_preg_test.join(birth_events_mom, identified_preg_test.hvid == birth_events_mom.adult_hvid)
    .drop("adult_hvid")
    .withColumn('date_diff',
                F.datediff(F.col("outcome_date"),F.col("child_dob"))
               )
    .dropDuplicates()
    .withColumn("flag_match",
                F.when(F.col("date_diff").between(-30,30),1).otherwise(0)               
               )
    .sort("hvid","episode_ID","outcome_date","child_dob")
    .filter("flag_match = 1")
    
)

display(match_preg_algo_birth_mom)

# COMMAND ----------

adult_deliveries = match_preg_algo_birth_mom.dropDuplicates()

display(match_preg_algo_birth_mom.groupBy("pregnancy_outcome").count())

adult_deliveries.count()

# COMMAND ----------

birth_events_mom.count()

# COMMAND ----------

adult_deliveries.count()/birth_events_mom.count()

# COMMAND ----------

#from moms data ser to the algo

notMatched_preg_id = (
    birth_events_mom.join(adult_deliveries,  birth_events_mom.adult_hvid == identified_preg_test.hvid, 'leftanti')        
)

display(notMatched_preg_id)
notMatched_preg_id.count()

# COMMAND ----------

notMatched_preg_id.count()/birth_events_mom.count()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql select min(date_service), max(date_service) from hive_metastore.cdh_hv_mother_baby_covid.mom_medical_claims
