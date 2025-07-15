# Databricks notebook source
# MAGIC %pip install striprtf

# COMMAND ----------

rtftext=r'{\rtf1\ansi\ansicpg1252\deff0\deftab720{\fonttbl{\f0\fcharset1 Tahoma;}}{\colortbl\red0\green0\blue0;}\plain\f0\fs16\cf0 \plain\f0\fs28\cf0\b Osborne, Casey \plain\f0\fs20\cf0 02/01/1962 \par |Office/Outpatient Visit\par |\plain\f0\fs20\cf0\b Visit Date: \plain\f0\fs20\cf0 Fri, Nov 18, 2022 09:01 am\par |\plain\f0\fs20\cf0\b Provider: \plain\f0\fs20\cf0 Owusu-Addo, Yaw A, MD (Supervisor: Owusu-Addo, Yaw A, MD; Assistant: Smith, Marissa, MA)\par |\plain\f0\fs20\cf0\b Location: \plain\f0\fs20\cf0 Piedmont Adult & Pediatric Medicine Associates, PA\par |\par |Electronically signed by Yaw Owusu-Addo, MD on 11/24/2022 03:24:15 PM \par |\plain\f0\fs28\cf0\b Subjective:\plain\f0\fs20\cf0 \par |\par |\plain\f0\fs20\cf0\b\ul CC: \plain\f0\fs20\cf0 Mr. Osborne is a 60 year old Black or African American male. Follow Up A1c\par |\par |\plain\f0\fs20\cf0\b\ul HPI: \plain\f0\fs20\cf0 \par | \par |Mr. Osborne presents with a diagnosis of chronic cough. This was diagnosed several months ago. The course has been stable and nonprogressive. Associated symptoms include\plain\f0\fs20\cf0\b cough\plain\f0\fs20\cf0 . He denies abdominal pain, fever, sore throat or vomiting. \par | \par |Dx with type 2 diabetes mellitus with hyperglycemia; specifically, this is type 2, non-insulin requiring diabetes. Compliance with treatment has been good; he takes his medication as directed and follows up as directed. He denies experiencing any diabetes related symptoms. \par |Tobacco screen: Current smoker. Current meds include an oral hypoglycemic. Not applicable He does not perform home blood glucose monitoring. Most recent lab results include glycohemoglobin 7.3%. \par |\plain\f0\fs20\cf0 \plain\f0\fs20\cf0\b\ul Allergies: \plain\f0\fs20\cf0 \par |Last Reviewed on 6/22/2021 08:18 AM by McSwain, Teresa I\par |Bydureon BCise: Itching of skin \par |Bydureon: \par |\par |\plain\f0\fs20\cf0 \plain\f0\fs28\cf0\b Objective:\plain\f0\fs20\cf0 \par |\par |\plain\f0\fs20\cf0\b\ul Vitals: \plain\f0\fs20\cf0 \par |\par |\plain\f0\fs20\cf0 \plain\f0\fs20\cf0\ul Current: \plain\f0\fs20\cf0 11/18/2022 9:01:59 AM\par |Ht: 5 ft, 11 in; Wt: 279 lbs; \plain\f0\fs20\cf0\b BMI: 38.9\plain\f0\fs20\cf0 T: 97.9 F (temporal); \plain\f0\fs20\cf0\b BP: 139\plain\f0\fs20\cf0 /74 mm Hg\plain\f0\fs20\cf0\b (right arm, sitting)\plain\f0\fs20\cf0 ; P: 84 bpm (finger clip, sitting); sCr: 0.89 mg/dL; GFR: 104.14\plain\f0\fs20\cf0\b O2 Sat: 97 %\plain\f0\fs20\cf0 \plain\f0\fs20\cf0\b (room air)\plain\f0\fs20\cf0 \par |\par |\plain\f0\fs20\cf0 \plain\f0\fs20\cf0\b\ul Exams: \plain\f0\fs20\cf0 \par |\par |GENERAL: well developed, well nourished, in no apparent distress \par |EYES: pupils and irises are normal; \par |E/N/T: masked\par |NECK: Neck is supple with full range of motion; \par |RESPIRATORY: normal respiratory rate and pattern with no distress; normal breath sounds with no rales, rhonchi, wheezes or rubs; \par |CARDIOVASCULAR: normal rate and rhythm without murmurs; normal S1 and S2 heart sounds with no S3, S4, rubs, or clicks;; normal rate; regular rhythm; normal S1 and S2 heartsounds with no S3 or S4; no murmurs \par |GASTROINTESTINAL: normal bowel sounds; no masses or tenderness; \par |MUSCULOSKELETAL: normal gait; grossly normal tone and muscle strength; \par |NEUROLOGIC: non-focal \par |PSYCHIATRIC: mental status: alert and oriented x 3; appropriate affect and demeanor; \par |\par |\plain\f0\fs20\cf0 \plain\f0\fs28\cf0\b Assessment: \plain\f0\fs20\cf0 \par |\par |R05.3 Chronic cough \par |E11.65 Type 2 diabetes mellitus with hyperglycemia \par |F17.210 Nicotine dependence, cigarettes, uncomplicated \par |N42.9 Disorder of prostate, unspecified \par |\par |\plain\f0\fs28\cf0\b ORDERS: \plain\f0\fs20\cf0 \par |\par |\plain\f0\fs20\cf0 \plain\f0\fs20\cf0\b\ul Radiology/Test Orders: \plain\f0\fs20\cf0 \par | 71020 Radiologic examination, chest, two views, frontal and lateral (Send-Out) \par |\par |\plain\f0\fs20\cf0\b\ul Lab Orders: \plain\f0\fs20\cf0 \par | 80053 Comprehensive metabolic panel (Send-Out) \par | 80061 Lipid panel (Send-Out) \par | 83036 Hemoglobin; glycosylated (A1C) (Send-Out) \par | 84153 Prostate specific antigen, total (Send-Out) \par | 84403 Testosterone; total (Send-Out) \par |\par |\plain\f0\fs20\cf0\b\ul Procedures Ordered: \plain\f0\fs20\cf0 \par | RFRAD Radiology referral, imaging or testing (Send-Out) \par |\par |\plain\f0\fs20\cf0 \par |\par |\plain\f0\fs28\cf0\b Plan: \plain\f0\fs20\cf0 \par |\par |\plain\f0\fs20\cf0\b Chronic cough\plain\f0\fs20\cf0 \par |\par |RADIOLOGY: I have ordered a chest x-ray (PA and lateral). \par |\par |\plain\f0\fs20\cf0 \plain\f0\fs20\cf0\ul Orders: \plain\f0\fs20\cf0 \par | RFRAD Radiology referral, imaging or testing (Send-Out) \par | 71020 Radiologic examination, chest, two views, frontal and lateral (Send-Out) \par |\par |\plain\f0\fs20\cf0 \plain\f0\fs20\cf0\b Type 2 diabetes mellitus with hyperglycemia\plain\f0\fs20\cf0 \par |\par |RECOMMENDATIONS: instructed in use of glucometer ( check glucose daily at random intervals ), adherance to a 2000 calorie ADA diet, weight loss, a graduated exercise program, HgbA1C level checked quarterly, urine microalbumin test yearly, LDL cholesterol test, daily foot self-inspection, lower blood pressure, and annual eye exams. \par |FOLLOW-UP: Schedule a follow-up visit in 3 months. \par |\par |STATUS: Diabetes is well controlled. No additional self-management resources or referrals are required \par |\par |\plain\f0\fs20\cf0 \plain\f0\fs20\cf0\ul Orders: \plain\f0\fs20\cf0 \par | 80053 Comprehensive metabolic panel (Send-Out) \par | 80061 Lipid panel (Send-Out) \par | 83036 Hemoglobin; glycosylated (A1C) (Send-Out) \par |\par |\plain\f0\fs20\cf0 \plain\f0\fs20\cf0\b Nicotine dependence, cigarettes, uncomplicated\plain\f0\fs20\cf0 \par |\par |\plain\f0\fs20\cf0 \plain\f0\fs20\cf0\ul Patient Education Handouts: \plain\f0\fs20\cf0 \par | Smoking Cessation \par |\par |\plain\f0\fs20\cf0\b Disorder of prostate, unspecified\plain\f0\fs20\cf0 \par |\par |\plain\f0\fs20\cf0 \plain\f0\fs20\cf0\ul Orders: \plain\f0\fs20\cf0 \par | 84153 Prostate specific antigen, total (Send-Out) \par | 84403 Testosterone; total (Send-Out) \par |\par |\plain\f0\fs20\cf0 \par |\plain\f0\fs20\cf0 \plain\f0\fs28\cf0\b Patient Recommendations:\plain\f0\fs20\cf0 \par |\par |For Type 2 diabetes mellitus with hyperglycemia:\par |Obtain instruction in the proper use of a home blood glucose monitor. Follow a diabetic diet as directed. You should be aiming for 2000 calories a day. Attempt weight loss as directed. Maintain an exercise regimen as directed. Have blood checked regularly for long term glucose control (a test called "hemoglobin A1C"). In your case, the hemoglobin A1C should be checked every 3 months. Have your urine regularly tested to check for protein, which is an early sign of diabetic kidney problems. Get this urine protein test done every year. Examine your feet daily. Small cuts and injuries often go unnoticed and can lead to serious infections. Have an annual eye exam. Schedule a follow-up visit in 3 months. \par |\par |\par |\plain\f0\fs28\cf0\b Charge Capture: \plain\f0\fs20\cf0 \par |\par |\plain\f0\fs20\cf0\b\ul Primary Diagnosis: \plain\f0\fs20\cf0 \par |R05.3 Chronic cough \par |\par | \plain\f0\fs20\cf0\ul Orders:\plain\f0\fs20\cf0 \par | 99213 Established patient outpatient visit, Low MDM and/or 20-29 minutes (In-House) \par |\par |E11.65 Type 2 diabetes mellitus with hyperglycemia \par |\plain\f0\fs20\cf0 F17.210 Nicotine dependence, cigarettes, uncomplicated \par |\plain\f0\fs20\cf0 N42.9 Disorder of prostate, unspecified \par |\plain\f0\fs20\cf0 \par |\plain\f0\fs20\cf0 \par }'

# COMMAND ----------

print(rtftext)

# COMMAND ----------


import re

basicRtfPattern = re.compile(r"\{\*?\\[^{}]+}|[{}]|\\\n?[A-Za-z]+\n?(?:-?\d+)?[ ]?")
                x=re.compile(r"\{\*?\\[^{}]+}|[{}]|\\\n?[A-Za-z]+\n?(?:-?\d+)?[ ]?")

newLineSlashesPattern = re.compile(r"\\\n")
ctrlCharPattern = re.compile(r"\\f[0-9]+")
ctrlCharPattern=re.compile(r"\\[A-Za-z0-9]+")
t=re.compile(r"\f0\fs\d\d(.*?)\\")

# For rtf, we remove the control word and then braces
jsrtf_result = x.findall(rtftext)
print(jsrtf_result)

#jsrtf_result = re.sub(re.sub(text,ctrlCharPattern,""),basicRtfPattern,"")
#jsrtf_result = re.sub(re.sub(re.sub(text,ctrlCharPattern,""),basicRtfPattern,""),newLineSlashesPattern, "\n"  )


# COMMAND ----------


from striprtf.striprtf import rtf_to_text

# COMMAND ----------


print(rtf_to_text(rtftext))

# COMMAND ----------

# MAGIC %md
# MAGIC for row in spark.sql("select 'patient notes and info' patient_notes from edav_prd_cdh.edav_prd_cdh.cdh_abfm_phi.patient limit 10").collect():
# MAGIC      print(row['patient_notes'])
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC use edav_prd_cdh.edav_prd_cdh.cdh_abfm_phi;
# MAGIC show tables
