-- Databricks notebook source
-- Single query for specific visualization
-- Note: this is averaging all pain observations per state-day, some residents have more observations than others...

-- CREATE OR REPLACE TABLE cdh_pointclickcare_exploratory.qxv3_test_extended AS

SELECT z.State, dayofmonth(y.OutcomeDate) AS Day, x.ResidentId, count(distinct(x.ResidentId)) AS numResidents, avg(x.ObservedValue) AS avgPain,
avg(y.CMI) as avgCMI, avg(y.ADL) AS avgADL
    FROM 
    cdh_pointclickcare.factobservedpainlevel AS x
    INNER JOIN cdh_pointclickcare.factdailyoutcome AS y ON
        x.ResidentId = y.ResidentId AND
        x.FacilityId = y.FacilityId AND
        x.ClientId = y.ClientId AND
        x.ObservationDateId = y.OutcomeDateId
        INNER JOIN cdh_pointclickcare.dimfacility AS z ON
        x.FacilityId = z.FacilityId
        
        WHERE year(x.ObservationDateTime) = 2019 AND month(x.ObservationDateTime) = 1
        GROUP BY z.State, y.OutcomeDate, x.ResidentId WITH ROLLUP
        HAVING isnotnull(x.ResidentId)
        ORDER BY z.State, Day;
