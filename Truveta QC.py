# Databricks notebook source
spark.sql("use edav_prd_cdh.cdh_truveta_stage")

# COMMAND ----------

len(spark.catalog.listTables("edav_prd_cdh.cdh_truveta_stage"))

# COMMAND ----------

# MAGIC %md # Bodysitemap

# COMMAND ----------

bsm = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.bodysitemap
""")
bsm.createOrReplaceTempView('bsm')

# COMMAND ----------

print(bsm.count(), len(bsm.columns))

# COMMAND ----------

# MAGIC %md # Bodysitemaphistory

# COMMAND ----------

bsmh = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.bodysitemaphistory
""")
bsmh.createOrReplaceTempView('bsmh')

# COMMAND ----------

print(bsmh.count(), len(bsmh.columns))

# COMMAND ----------

# MAGIC %md # Claim

# COMMAND ----------

claim = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.claim
""")
claim.createOrReplaceTempView('claim')

# COMMAND ----------

print(claim.count(), len(claim.columns))

# COMMAND ----------

display(claim)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(personid)) as countp, count(distinct(id)) as counti
# MAGIC from claim

# COMMAND ----------

# MAGIC %md # Claimline

# COMMAND ----------

claimline = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.claimline
""")
claimline.createOrReplaceTempView('claimline')

# COMMAND ----------

print(claimline.count(), len(claimline.columns))

# COMMAND ----------

# MAGIC %md # Claimlinecodes

# COMMAND ----------

claimlinecodes = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.claimlinecodes
""")
claimlinecodes.createOrReplaceTempView('claimlinecodes')

# COMMAND ----------

print(claimlinecodes.count(), len(claimlinecodes.columns))

# COMMAND ----------

# MAGIC %md # Concept

# COMMAND ----------

con = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.concept
""")
con.createOrReplaceTempView('con')

# COMMAND ----------

spark.sql("REFRESH TABLE con")

# COMMAND ----------

print(con.count(), len(con.columns))

# COMMAND ----------

display(con)

# COMMAND ----------

con.distinct().count()

# COMMAND ----------

from pyspark.sql.functions import countDistinct
con.select(countDistinct("ConceptId")).show()

# COMMAND ----------

# MAGIC %md # Conceptlink

# COMMAND ----------

conl = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.conceptlink
""")
conl.createOrReplaceTempView('conl')

# COMMAND ----------

spark.sql("REFRESH TABLE conl")

# COMMAND ----------

print(conl.count(), len(conl.columns))

# COMMAND ----------

# MAGIC %md # Condition

# COMMAND ----------

condition = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.condition
""")
condition.createOrReplaceTempView('condition')

# COMMAND ----------

print(condition.count(), len(condition.columns))

# COMMAND ----------

# MAGIC %md # Conditioncodeconceptmap

# COMMAND ----------

conditionccm = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.conditioncodeconceptmap
""")
conditionccm.createOrReplaceTempView('conditionccm')


# COMMAND ----------

print(conditionccm.count(), len(conditionccm.columns))

# COMMAND ----------

# MAGIC %md # Conditioncodeconceptmaphistory

# COMMAND ----------

conditionccmh = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.conditioncodeconceptmaphistory
""")
conditionccmh.createOrReplaceTempView('conditionccmh')

# COMMAND ----------

print(conditionccmh.count(), len(conditionccmh.columns))

# COMMAND ----------

# MAGIC %md # Conditioncodes

# COMMAND ----------

conditionc = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.conditioncodes
""")
conditionc.createOrReplaceTempView('conditionc')

# COMMAND ----------

print(conditionc.count(), len(conditionc.columns))

# COMMAND ----------

# MAGIC %md # Deviceuse

# COMMAND ----------

du = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.deviceuse
""")
du.createOrReplaceTempView('du')

# COMMAND ----------

print(du.count(), len(du.columns))

# COMMAND ----------

# MAGIC %md # Deviceusecodeconceptmap

# COMMAND ----------

duccm = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.deviceusecodeconceptmap
""")
duccm.createOrReplaceTempView('duccm')

# COMMAND ----------

print(duccm.count(), len(duccm.columns))

# COMMAND ----------

# MAGIC %md # Deviceusecodeconceptmaphistory

# COMMAND ----------

duccmh = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.deviceusecodeconceptmaphistory
""")
duccmh.createOrReplaceTempView('duccmh')

# COMMAND ----------

print(duccmh.count(), len(duccmh.columns))

# COMMAND ----------

# MAGIC %md # Encounter

# COMMAND ----------

en = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.encounter
""")
du.createOrReplaceTempView('en')

# COMMAND ----------

print(en.count(), len(en.columns))

# COMMAND ----------

# MAGIC %md # Encounterparticipant

# COMMAND ----------

enp = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.encounterparticipant
""")
enp.createOrReplaceTempView('enp')

# COMMAND ----------

print(enp.count(), len(enp.columns))

# COMMAND ----------

# MAGIC %md # Eventreference

# COMMAND ----------

er = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.eventreference
""")
er.createOrReplaceTempView('er')

# COMMAND ----------

print(er.count(), len(er.columns))

# COMMAND ----------

# MAGIC %md # Extendedproperty

# COMMAND ----------

ex = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.extendedproperty
""")
ex.createOrReplaceTempView('ex')

# COMMAND ----------

print(ex.count(), len(ex.columns))

# COMMAND ----------

# MAGIC %md # Extract

# COMMAND ----------

extract = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.extract
""")
extract.createOrReplaceTempView('extract')

# COMMAND ----------

print(extract.count(), len(extract.columns))

# COMMAND ----------

# MAGIC %md # Extractschema

# COMMAND ----------

extractsch = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.extractschema
""")
extractsch.createOrReplaceTempView('extractsch')

# COMMAND ----------

print(extractsch.count(), len(extractsch.columns))

# COMMAND ----------

# MAGIC %md # Familymemberhistory

# COMMAND ----------

fam = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.familymemberhistory
""")
fam.createOrReplaceTempView('fam')

# COMMAND ----------

print(fam.count(), len(fam.columns))

# COMMAND ----------

# MAGIC %md # Familymemberhistorycodeconceptmap

# COMMAND ----------

famccm = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.familymemberhistorycodeconceptmap
""")
famccm.createOrReplaceTempView('famccm')

# COMMAND ----------

print(famccm.count(), len(famccm.columns))

# COMMAND ----------

# MAGIC %md # Familymemberhistorycodeconceptmaphistory

# COMMAND ----------

famccmh = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.familymemberhistorycodeconceptmaphistory
""")
famccmh.createOrReplaceTempView('famccmh')

# COMMAND ----------

print(famccmh.count(), len(famccmh.columns))

# COMMAND ----------

# MAGIC %md # Familymemberhistoryoutcomeconceptmap

# COMMAND ----------

famocm = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.familymemberhistoryoutcomeconceptmap
""")
famocm.createOrReplaceTempView('famocm')

# COMMAND ----------

print(famocm.count(), len(famocm.columns))

# COMMAND ----------

# MAGIC %md # Familymemberhistoryoutcomeconceptmaphistory

# COMMAND ----------

famocmh = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.familymemberhistoryoutcomeconceptmaphistory
""")
famocmh.createOrReplaceTempView('famocmh')

# COMMAND ----------

print(famocmh.count(), len(famocmh.columns))

# COMMAND ----------

# MAGIC %md # Imaginginstance

# COMMAND ----------

imgins = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.imaginginstance
""")
imgins.createOrReplaceTempView('imgins')

# COMMAND ----------

print(imgins.count(), len(imgins.columns))

# COMMAND ----------

# MAGIC %md # Imagingproperty

# COMMAND ----------

imgpro = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.imagingproperty
""")
imgpro.createOrReplaceTempView('imgpro')

# COMMAND ----------

print(imgpro.count(), len(imgpro.columns))

# COMMAND ----------

# MAGIC %md # Imagingseries

# COMMAND ----------

imgs = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.imagingseries
""")
imgs.createOrReplaceTempView('imgs')

# COMMAND ----------

print(imgs.count(), len(imgs.columns))

# COMMAND ----------

# MAGIC %md # Imagingstudy

# COMMAND ----------

imgstu = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.imagingstudy
""")
imgstu.createOrReplaceTempView('imgstu')

# COMMAND ----------

print(imgstu.count(), len(imgstu.columns))

# COMMAND ----------

# MAGIC %md # Immunization

# COMMAND ----------

im = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.immunization
""")
im.createOrReplaceTempView('im')

# COMMAND ----------

print(im.count(), len(im.columns))

# COMMAND ----------

# MAGIC %md # Immunizationcodeconceptmap

# COMMAND ----------

imccm = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.immunizationcodeconceptmap
""")
imccm.createOrReplaceTempView('imccm')

# COMMAND ----------

print(imccm.count(), len(imccm.columns))

# COMMAND ----------

# MAGIC %md # Immunizationcodeconceptmaphistory

# COMMAND ----------

imccmh = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.immunizationcodeconceptmaphistory
""")
imccmh.createOrReplaceTempView('imccmh')

# COMMAND ----------

print(imccmh.count(), len(imccmh.columns))

# COMMAND ----------

# MAGIC %md # Immunizationcodes

# COMMAND ----------

imc = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.immunizationcodes
""")
imc.createOrReplaceTempView('imc')

# COMMAND ----------

print(imc.count(), len(imc.columns))

# COMMAND ----------

# MAGIC %md # Labresults

# COMMAND ----------

labr = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.labresult
""")
labr.createOrReplaceTempView('labr')

# COMMAND ----------

print(labr.count(), len(labr.columns))

# COMMAND ----------

# MAGIC %md # Labresultscodeconceptmap

# COMMAND ----------

labrccm = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.labresultcodeconceptmap
""")
labrccm.createOrReplaceTempView('labrccm')

# COMMAND ----------

print(labrccm.count(), len(labrccm.columns))

# COMMAND ----------

# MAGIC %md #Labresultscodeconceptmaphistory

# COMMAND ----------

labrccmh = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.labresultcodeconceptmaphistory
""")
labrccmh.createOrReplaceTempView('labrccmh')

# COMMAND ----------

print(labrccmh.count(), len(labrccmh.columns))

# COMMAND ----------

# MAGIC %md # Labresultscodes

# COMMAND ----------

labrc = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.labresultcodes
""")
labrc.createOrReplaceTempView('labrc')

# COMMAND ----------

print(labrc.count(), len(labrc.columns))

# COMMAND ----------

# MAGIC %md # Location

# COMMAND ----------

loc = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.location
""")
loc.createOrReplaceTempView('loc')

# COMMAND ----------

print(loc.count(), len(loc.columns))

# COMMAND ----------

# MAGIC %md # Medicationadherencereasonconceptmap

# COMMAND ----------

medarcm = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.medicationadherencereasonconceptmap
""")
medarcm.createOrReplaceTempView('medarcm')

# COMMAND ----------

print(medarcm.count(), len(medarcm.columns))

# COMMAND ----------

# MAGIC %md # Medicationadherencereasonconceptmaphistory

# COMMAND ----------

medarcmh = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.medicationadherencereasonconceptmaphistory
""")
medarcmh.createOrReplaceTempView('medarcmh')

# COMMAND ----------

print(medarcmh.count(), len(medarcmh.columns))

# COMMAND ----------

# MAGIC %md # Medicationadminstration

# COMMAND ----------

med = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.medicationadministration
""")
med.createOrReplaceTempView('med')

# COMMAND ----------

print(med.count(), len(med.columns))

# COMMAND ----------

# MAGIC %md # Medicationcodeconceptmap

# COMMAND ----------

medccm = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.medicationcodeconceptmap
""")
medccm.createOrReplaceTempView('medccm')

# COMMAND ----------

print(medccm.count(), len(medccm.columns))

# COMMAND ----------

# MAGIC %md # Medicationcodeconceptmaphistory

# COMMAND ----------

medccmh = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.medicationcodeconceptmaphistory
""")
medccmh.createOrReplaceTempView('medccmh')

# COMMAND ----------

print(medccmh.count(), len(medccmh.columns))

# COMMAND ----------

# MAGIC %md # Medicationcodes

# COMMAND ----------

medc = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.medicationcodes
""")
medc.createOrReplaceTempView('medc')

# COMMAND ----------

print(medc.count(), len(medc.columns))

# COMMAND ----------

# MAGIC %md # Medicationdispense

# COMMAND ----------

medd = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.medicationdispense
""")
medd.createOrReplaceTempView('medd')

# COMMAND ----------

print(medd.count(), len(medd.columns))

# COMMAND ----------

# MAGIC %md #Medicationrequest

# COMMAND ----------

medr = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.medicationrequest
""")
medr.createOrReplaceTempView('medr')

# COMMAND ----------

print(medr.count(), len(medr.columns))

# COMMAND ----------

# MAGIC %md # Medicationstatement

# COMMAND ----------

medstate = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.medicationstatement
""")
medstate.createOrReplaceTempView('medstate')

# COMMAND ----------

print(medstate.count(), len(medstate.columns))

# COMMAND ----------

# MAGIC %md # Note

# COMMAND ----------

note = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.note
""")
note.createOrReplaceTempView('note')

# COMMAND ----------

print(note.count(), len(note.columns))

# COMMAND ----------

# MAGIC %md # Observation

# COMMAND ----------

obs = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.observation
""")
obs.createOrReplaceTempView('obs')

# COMMAND ----------

obs.columns

# COMMAND ----------

print(obs.count(), len(obs.columns))

# COMMAND ----------

# MAGIC %md # Observationcodeconceptmap

# COMMAND ----------

obsccm = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.observationcodeconceptmap
""")
obsccm.createOrReplaceTempView('obsccm')

# COMMAND ----------

print(obsccm.count(), len(obsccm.columns))

# COMMAND ----------

# MAGIC %md # Observationconceptmaphistory

# COMMAND ----------

obsccmh = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.observationcodeconceptmaphistory
""")
obsccmh.createOrReplaceTempView('obsccmh')

# COMMAND ----------

print(obsccmh.count(), len(obsccmh.columns))

# COMMAND ----------

# MAGIC %md # Observationcomponent

# COMMAND ----------

obsc = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.observationcomponent
""")
obsc.createOrReplaceTempView('obsc')

# COMMAND ----------

print(obsc.count(), len(obsc.columns))

# COMMAND ----------

# MAGIC %md # Patient

# COMMAND ----------

pat = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.patient
""")
pat.createOrReplaceTempView('pat')

# COMMAND ----------

print(pat.count(), len(pat.columns))

# COMMAND ----------

# MAGIC %md # Person

# COMMAND ----------

pers = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.person
""")
pers.createOrReplaceTempView('pers')

# COMMAND ----------

print(pers.count(), len(pers.columns))

# COMMAND ----------

# MAGIC %md # Persondeathfacts

# COMMAND ----------

perd = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.persondeathfact
""")
perd.createOrReplaceTempView('perd')

# COMMAND ----------

print(perd.count(), len(perd.columns))

# COMMAND ----------

# MAGIC %md # Personepctlink

# COMMAND ----------

perel = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.personepctlink
""")
perel.createOrReplaceTempView('perel')

# COMMAND ----------

print(perel.count(), len(perel.columns))

# COMMAND ----------

# MAGIC %md # Personlocation

# COMMAND ----------

perl = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.personlocation
""")
perl.createOrReplaceTempView('perl')

# COMMAND ----------

print(perl.count(), len(perl.columns))

# COMMAND ----------

# MAGIC %md # Personrace

# COMMAND ----------

perr = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.personrace
""")
perr.createOrReplaceTempView('perr')

# COMMAND ----------

print(perr.count(), len(perr.columns))

# COMMAND ----------

# MAGIC %md # Persontag

# COMMAND ----------

pert = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.persontag
""")
pert.createOrReplaceTempView('pert')

# COMMAND ----------

print(pert.count(), len(pert.columns))

# COMMAND ----------

# MAGIC %md # Practitioner

# COMMAND ----------

pra = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.practitioner
""")
pra.createOrReplaceTempView('pra')

# COMMAND ----------

print(pra.count(), len(pra.columns))

# COMMAND ----------

# MAGIC %md # Practitionerqualification

# COMMAND ----------

praq = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.practitionerqualification
""")
praq.createOrReplaceTempView('praq')

# COMMAND ----------

print(praq.count(), len(praq.columns))

# COMMAND ----------

# MAGIC %md # Procedure

# COMMAND ----------

pro = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.procedure
""")
pro.createOrReplaceTempView('pro')

# COMMAND ----------

print(pro.count(), len(pro.columns))

# COMMAND ----------

# MAGIC %md # Procedurecodeconceptmap

# COMMAND ----------

proccm = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.procedurecodeconceptmap
""")
proccm.createOrReplaceTempView('proccm')

# COMMAND ----------

print(proccm.count(), len(proccm.columns))

# COMMAND ----------

# MAGIC %md # Procedurecodeconceptmaphistory

# COMMAND ----------

proccmh = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.procedurecodeconceptmaphistory
""")
proccmh.createOrReplaceTempView('proccmh')

# COMMAND ----------

print(proccmh.count(), len(proccmh.columns))

# COMMAND ----------

# MAGIC %md # Procedurecodes

# COMMAND ----------

proc = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.procedurecodes
""")
proc.createOrReplaceTempView('proc')

# COMMAND ----------

print(proc.count(), len(proc.columns))

# COMMAND ----------

# MAGIC %md # Searchresults_dob

# COMMAND ----------

sea = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.searchresult_dob
""")
sea.createOrReplaceTempView('sea')

# COMMAND ----------

print(sea.count(), len(sea.columns))

# COMMAND ----------

# MAGIC %md # Servicerequest

# COMMAND ----------

sr = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.servicerequest
""")
sr.createOrReplaceTempView('sr')

# COMMAND ----------

print(sr.count(), len(sr.columns))

# COMMAND ----------

# MAGIC %md # Servicerequestcodeconceptmap

# COMMAND ----------

srccm = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.servicerequestcodeconceptmap
""")
srccm.createOrReplaceTempView('srccm')

# COMMAND ----------

print(srccm.count(), len(srccm.columns))

# COMMAND ----------

# MAGIC %md # Servicerequestcodeconceptmaphistory

# COMMAND ----------

srccmh = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.servicerequestcodeconceptmaphistory
""")
srccmh.createOrReplaceTempView('srccmh')

# COMMAND ----------

print(srccmh.count(), len(srccmh.columns))

# COMMAND ----------

# MAGIC %md # Socialdeterminantsofhealth

# COMMAND ----------

soc = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.socialdeterminantsofhealth
""")
soc.createOrReplaceTempView('soc')

# COMMAND ----------

print(soc.count(), len(soc.columns))

# COMMAND ----------

# MAGIC %md # Temporalorder

# COMMAND ----------

tem = spark.sql("""
select *
from edav_prd_cdh.cdh_truveta_stage.temporalorder
""")
tem.createOrReplaceTempView('tem')

# COMMAND ----------

print(tem.count(), len(tem.columns))