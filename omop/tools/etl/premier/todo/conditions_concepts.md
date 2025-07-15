* common\intermediate\5_write_measurements.py
```mermaid
flowchart 
    patcpt --HCPCS-38000275-->stage_procedures_temp 
    paticd_proc --ICD10PCS-->stage_procedures_temp 
    stage_procedures_temp --key-->measurement_temp 

    genlab --LOINC-->stage_lab_temp  
    vitals --LOINC-->stage_lab_temp  
    stage_lab_temp--key-->measurement_temp  

    stage_conditions_temp--key-->measurement_temp  
end
```

* premier\todo\4b3_create_labs_meas
* premier\todo\4c3_create_procedures_meas
