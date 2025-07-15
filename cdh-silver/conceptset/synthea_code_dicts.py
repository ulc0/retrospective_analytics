
#lab specific
NEGATIVE_COVID_DESC='Not~detected~(qualifier~value)'


othercodes={
#lab specific
"NEGATIVE_COVID_DESC":['Not~detected~(qualifier~value)'],
"COVID_CODE":['840539006',],
"NEGATIVE_COVID_LAB_CODE":['94531-1',],
"COMPLETED_ISOLATION_CODE":['736376001',],
# same as below "COMPLETED_ISOLATION_REASONCODE":['840539006',],
"INPATIENT_REASONCODE":['840539006',],
"INPATIENT_CODE":['1505002'],
"ICD_CODE":['305351004',],
"VENT_CODE":['26763009','449071006',],
#"VENT_CODE":['449071006',],
"COVID_LAB_CODE":[ '48065-7','26881-3','2276-4', '2532-0', '731-0',  '14804-9',],}

OUTCOMES_DICT=  {'Sepsis': ['770349000'], 'Respiratory~Failure': ['65710008'], 
            'Acute~Respiratory~Distress~Syndrome': ['67782005'], 'Heart~Failure': ['84114007'],
            'Septic~Shock': ['76571007'], 'Coagulopathy':[ '234466008'], 
            'Acute~Cardiac~Injury': ['86175003'],
            'Acute~Kidney~Injury':[ '40095003']}
# prior dictionary is extension of this 
#ANNOTATIONS_OUTCOMES_DICT=  annotation_outcomes = {'Sepsis': '770349000', 'ARDS': '67782005'}

symptom_map = {'Conjunctival~Congestion':[ '246677007'], 
'Nasal_Congestion':[ '68235000'], 'Headache': ['25064002'],
               'Cough': ['49727002'], 'Sore~Throat': ['267102003'], 'Sputum~Production':[ '248595008'],
               'Fatigue': ['84229001'], 'Hemoptysis': ['66857006'], 'Shortness~of~Breath': ['267036007'],
               'Nausea': ['422587007'], 'Diarrhea': ['267060006'], 'Muscle~Pain': ['68962001'],
               'Joint~Pain':[ '57676002'], 'Chills': ['43724002'], 'Loss~of~Taste': ['36955009']}
#invert this dictionary? no, t
COVID_LAB_MAP= {'48065-7': 'D-dimer', '2276-4': 'Serum Ferritin',
                    '89579-7': 'High Sensitivity Cardiac Troponin I',
                    '26881-3': 'IL-6', '731-0': 'Lymphocytes',
                    '14804-9': 'Lactate dehydrogenase'}
