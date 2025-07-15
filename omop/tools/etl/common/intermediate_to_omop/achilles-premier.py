# Databricks notebook source
dbutils.widgets.text("source_catalog",defaultValue="edav_stg_cdh")
dbutils.widgets.text("source_schema",defaultValue="cdh_premier_v2")
#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("etl_catalog",defaultValue="edav_stg_cdh")
dbutils.widgets.text("etl_schema",defaultValue="cdh_premier_omop_etl")
dbutils.widgets.text("omop_catalog",defaultValue="edav_stg_cdh")
dbutils.widgets.text("omop_schema",defaultValue="cdh_premier_omop")
dbutils.widgets.text("results_catalog",defaultValue="edav_stg_cdh")
dbutils.widgets.text("results_schema",defaultValue="cdh_premier_atlas_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_0	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_0	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_3	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_4	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_5	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_10	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_11	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_12	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_101	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_102	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_103	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_104	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_104	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempObs_105	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1statsView_105	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_105	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_105	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1rawData_106	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_106	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_106	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_107	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_107	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_108	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_109	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_110	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_111	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_112	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_113	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1temp_dates_116	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_116	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_117 	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_119	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_200	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_201	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_202	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_203	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_203	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_204	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_206	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_206	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_207	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_209	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_210	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_212	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_213	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_213	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_220	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_221	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_225	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_226	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_300	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_301	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_303 	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_325 	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_400	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_401	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_402	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_403	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_403	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_404	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_405	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1rawData_406	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_406	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_406	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_414	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_415	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_416	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_420	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_425	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_500	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_501	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_502	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_504	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_505	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_506	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_506	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_511	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_512	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_512	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_513	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_513	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_514	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_514	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_515	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_515	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_525 	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_600	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_601	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_602	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_603	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_603	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_604	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_605	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1rawData_606	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_606	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_606	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_620	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_625	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_630	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_691	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_700	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_701	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_702	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_703	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_703	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_704	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_705	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1rawData_706	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_706	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_706	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_715	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_715	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_716	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_716	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_717	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_717	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_720	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_725	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_791	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_800	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_801	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_802	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_803	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_803	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_804	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_805	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1rawData_806	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_806	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_806	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_807	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_814	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1overallStats_815	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1statsView_815	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_815	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_815	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_820	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_822	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_823	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_825	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_826	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_827	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_891	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_900	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_901	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_902	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_903	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_903	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_904	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1rawData_906	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_906	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_906	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_907	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_907	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_920	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1000	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1001	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1002	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_1003	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1003	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1004	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1rawData_1006	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_1006	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1006	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_1007	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1007	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1020	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1100	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1101	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1102	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1103	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1200	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1201	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1202	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1203	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1300	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1301	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1302	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_1303	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1303	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1304	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_1306	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1306	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1312	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_1313	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1313	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1320	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1321	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1325	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1326	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_1406	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1406	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_1407	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1407	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1408	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1temp_dates_1409	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1409	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1temp_dates_1410	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1410	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1411	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1412	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1413	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1425 	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1800	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1801	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1802	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_1803	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1803	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1804	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1805	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1rawData_1806	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_1806	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1806	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1807	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1811	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1814	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1statsView_1815	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_1815	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1815	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1overallStats_1816	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1statsView_1816	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_1816	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1816	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1overallStats_1817	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1statsView_1817	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_1817	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1817	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1rawData_1818	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1818	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1819	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1820	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1821	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1822	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1823	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1825	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1826	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1827	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1891	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1900	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2000	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2001	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2002	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2003	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1conoc 	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1drexp 	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1dvexp 	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1msmt 	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1death 	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1prococ 	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1obs 	;
# MAGIC  drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2004 	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2100	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2101	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2102	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2104	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2105	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1rawData_2106	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1tempResults_2106	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_2106	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2120	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2125	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2191	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2200	;
# MAGIC drop table if exists  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2201	;
# MAGIC -- drop table if exists  ${results_catalog}.${results_schema}.achilles_results	;
# MAGIC -- drop table if exists  ${results_catalog}.${results_schema}.achilles_results_dist	;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_0;
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_0
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 0 as analysis_id, CAST('PREMIER-EDAV-DEV' AS STRING) as stratum_1, CAST('1.7.2' AS STRING) as stratum_2, 
# MAGIC CAST(CURRENT_DATE AS STRING) as stratum_3,
# MAGIC cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC COUNT(distinct person_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_0
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_0;
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_0
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 0 as analysis_id, CAST('PREMIER-EDAV-DEV' AS STRING) as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC COUNT(distinct person_id) as count_value, 
# MAGIC  cast(null as float) as min_value,
# MAGIC  cast(null as float) as max_value,
# MAGIC  cast(null as float) as avg_value,
# MAGIC  cast(null as float) as stdev_value,
# MAGIC  cast(null as float) as median_value,
# MAGIC  cast(null as float) as p10_value,
# MAGIC  cast(null as float) as p25_value,
# MAGIC  cast(null as float) as p75_value,
# MAGIC  cast(null as float) as p90_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_0
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC drop table if exists ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1;
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1 as analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC COUNT(distinct person_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 2 as analysis_id, 
# MAGIC CAST(gender_concept_id AS STRING) as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC COUNT(distinct person_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person
# MAGIC group by GENDER_CONCEPT_ID;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_3
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 3 as analysis_id, CAST(year_of_birth AS STRING) as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC COUNT(distinct person_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person
# MAGIC group by YEAR_OF_BIRTH;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_3
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_4
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 4 as analysis_id, CAST(RACE_CONCEPT_ID AS STRING) as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC COUNT(distinct person_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person
# MAGIC group by RACE_CONCEPT_ID;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_4
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_5
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 5 as analysis_id, CAST(ETHNICITY_CONCEPT_ID AS STRING) as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC COUNT(distinct person_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person
# MAGIC group by ETHNICITY_CONCEPT_ID;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_5
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_10
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 10 as analysis_id, CAST(year_of_birth AS STRING) as stratum_1,
# MAGIC  CAST(gender_concept_id AS STRING) as stratum_2,
# MAGIC  cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(distinct person_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person
# MAGIC group by YEAR_OF_BIRTH, gender_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_10
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_11
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 11 as analysis_id, CAST(P.year_of_birth AS STRING) as stratum_1,
# MAGIC  CAST(P.gender_concept_id AS STRING) as stratum_2,
# MAGIC  cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(distinct P.person_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person P
# MAGIC where not exists
# MAGIC (
# MAGIC  select 1
# MAGIC  from ${omop_catalog}.${omop_schema}.death D
# MAGIC  where P.person_id = D.person_id
# MAGIC )
# MAGIC group by P.YEAR_OF_BIRTH, P.gender_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_11
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_12
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 12 as analysis_id, CAST(RACE_CONCEPT_ID AS STRING) as stratum_1, CAST(ETHNICITY_CONCEPT_ID AS STRING) as stratum_2, 
# MAGIC cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC COUNT(distinct person_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person
# MAGIC group by RACE_CONCEPT_ID,ETHNICITY_CONCEPT_ID;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_12
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_101
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC  select
# MAGIC  year(op1.index_date) - p1.YEAR_OF_BIRTH as stratum_1,
# MAGIC  COUNT(p1.person_id) as count_value
# MAGIC  from ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join (select person_id, MIN(observation_period_start_date) as index_date from ${omop_catalog}.${omop_schema}.observation_period group by PERSON_ID) op1
# MAGIC  on p1.PERSON_ID = op1.PERSON_ID
# MAGIC  group by year(op1.index_date) - p1.YEAR_OF_BIRTH
# MAGIC )
# MAGIC  SELECT
# MAGIC 101 as analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2,
# MAGIC  cast(null as STRING) as stratum_3,
# MAGIC  cast(null as STRING) as stratum_4,
# MAGIC  cast(null as STRING) as stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_101
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_102
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC  select
# MAGIC  p1.gender_concept_id as stratum_1,
# MAGIC  year(op1.index_date) - p1.YEAR_OF_BIRTH as stratum_2,
# MAGIC  COUNT(p1.person_id) as count_value
# MAGIC  from ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join (select person_id, MIN(observation_period_start_date) as index_date from ${omop_catalog}.${omop_schema}.observation_period group by PERSON_ID) op1
# MAGIC  on p1.PERSON_ID = op1.PERSON_ID
# MAGIC  group by p1.gender_concept_id, year(op1.index_date) - p1.YEAR_OF_BIRTH)
# MAGIC  SELECT
# MAGIC 102 as analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) as stratum_1,
# MAGIC  cast(stratum_2 as STRING) as stratum_2,
# MAGIC  cast(null as STRING) as stratum_3,
# MAGIC  cast(null as STRING) as stratum_4,
# MAGIC  cast(null as STRING) as stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_102
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_103
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData (person_id, age_value)  AS (
# MAGIC select p.person_id, 
# MAGIC  MIN(YEAR(observation_period_start_date)) - P.YEAR_OF_BIRTH as age_value
# MAGIC  from ${omop_catalog}.${omop_schema}.person p
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.observation_period op on p.person_id = op.person_id
# MAGIC  group by p.person_id, p.year_of_birth
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select CAST(avg(1.0 * age_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(age_value) AS FLOAT) as stdev_value,
# MAGIC  min(age_value) as min_value,
# MAGIC  max(age_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM rawData
# MAGIC ),
# MAGIC ageStats (age_value, total, rn) as
# MAGIC (
# MAGIC  select age_value, COUNT(*) as total, row_number() over (order by age_value) as rn
# MAGIC  from rawData
# MAGIC  group by age_value
# MAGIC ),
# MAGIC ageStatsPrior (age_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.age_value, s.total, sum(p.total) as accumulated
# MAGIC  from ageStats s
# MAGIC  join ageStats p on p.rn <= s.rn
# MAGIC  group by s.age_value, s.total, s.rn
# MAGIC ),
# MAGIC tempResults as
# MAGIC (
# MAGIC  select 103 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then age_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then age_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then age_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then age_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then age_value end) as p90_value
# MAGIC  --INTO #tempResults
# MAGIC  from ageStatsPrior p
# MAGIC  CROSS JOIN overallStats o
# MAGIC  GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC )
# MAGIC  SELECT
# MAGIC analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5, 
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC tempResults;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_103
# MAGIC  ZORDER BY count_value;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_104
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData (gender_concept_id, age_value)  AS (
# MAGIC  select p.gender_concept_id, MIN(YEAR(observation_period_start_date)) - P.YEAR_OF_BIRTH as age_value
# MAGIC  from ${omop_catalog}.${omop_schema}.person p
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.observation_period op on p.person_id = op.person_id
# MAGIC  group by p.person_id,p.gender_concept_id, p.year_of_birth
# MAGIC ),
# MAGIC overallStats (gender_concept_id, avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select gender_concept_id,
# MAGIC  CAST(avg(1.0 * age_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(age_value) AS FLOAT) as stdev_value,
# MAGIC  min(age_value) as min_value,
# MAGIC  max(age_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM rawData
# MAGIC  group by gender_concept_id
# MAGIC ),
# MAGIC ageStats (gender_concept_id, age_value, total, rn) as
# MAGIC (
# MAGIC  select gender_concept_id, age_value, COUNT(*) as total, row_number() over (order by age_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by gender_concept_id, age_value
# MAGIC ),
# MAGIC ageStatsPrior (gender_concept_id, age_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.gender_concept_id, s.age_value, s.total, sum(p.total) as accumulated
# MAGIC  from ageStats s
# MAGIC  join ageStats p on s.gender_concept_id = p.gender_concept_id and p.rn <= s.rn
# MAGIC  group by s.gender_concept_id, s.age_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 104 as analysis_id,
# MAGIC  CAST(o.gender_concept_id AS STRING) as stratum_1,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then age_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then age_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then age_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then age_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then age_value end) as p90_value
# MAGIC FROM
# MAGIC ageStatsPrior p
# MAGIC join overallStats o on p.gender_concept_id = o.gender_concept_id
# MAGIC GROUP BY o.gender_concept_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_104
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_104
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_104
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_104
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_104;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_104;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempObs_105
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC count_value, rn 
# MAGIC FROM
# MAGIC (
# MAGIC  select datediff(day,op.observation_period_start_date,op.observation_period_end_date) as count_value,
# MAGIC  ROW_NUMBER() over (PARTITION by op.person_id order by op.observation_period_start_date asc) as rn
# MAGIC  from ${omop_catalog}.${omop_schema}.observation_period op
# MAGIC ) A
# MAGIC where rn = 1;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempObs_105
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1statsView_105
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC count_value, COUNT(*) as total, row_number() over (order by count_value) as rn
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempObs_105
# MAGIC group by count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_105
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH overallStats (avg_value, stdev_value, min_value, max_value, total)  AS (
# MAGIC  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from ${results_catalog}.${results_schema}.xpi8onh1tempObs_105
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from ${results_catalog}.${results_schema}.xpi8onh1statsView_105 s
# MAGIC  join ${results_catalog}.${results_schema}.xpi8onh1statsView_105 p on p.rn <= s.rn
# MAGIC  group by s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 105 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN overallStats o
# MAGIC GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_105
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_105
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id,
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5, count_value,
# MAGIC min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_105
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_105
# MAGIC  ZORDER BY count_value;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempObs_105;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempObs_105;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1statsView_105;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1statsView_105;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_105;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_105;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(gender_concept_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1rawData_106
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC p.gender_concept_id, op.count_value
# MAGIC FROM
# MAGIC (
# MAGIC  select person_id, datediff(day,op.observation_period_start_date,op.observation_period_end_date) as count_value,
# MAGIC  ROW_NUMBER() over (PARTITION by op.person_id order by op.observation_period_start_date asc) as rn
# MAGIC  from ${omop_catalog}.${omop_schema}.observation_period op
# MAGIC ) op
# MAGIC JOIN ${omop_catalog}.${omop_schema}.person p on op.person_id = p.person_id
# MAGIC where op.rn = 1
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1rawData_106
# MAGIC  ZORDER BY gender_concept_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(gender_concept_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_106
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH overallStats (gender_concept_id, avg_value, stdev_value, min_value, max_value, total)  AS (
# MAGIC  select gender_concept_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_106
# MAGIC  group by gender_concept_id
# MAGIC ),
# MAGIC statsView (gender_concept_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select gender_concept_id, count_value, COUNT(*) as total, row_number() over (order by count_value) as rn
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_106
# MAGIC  group by gender_concept_id, count_value
# MAGIC ),
# MAGIC priorStats (gender_concept_id,count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.gender_concept_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.gender_concept_id = p.gender_concept_id and p.rn <= s.rn
# MAGIC  group by s.gender_concept_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 106 as analysis_id,
# MAGIC  CAST(o.gender_concept_id AS STRING) as gender_concept_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.gender_concept_id = o.gender_concept_id
# MAGIC GROUP BY o.gender_concept_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_106
# MAGIC  ZORDER BY gender_concept_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_106
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, gender_concept_id as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_106
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_106
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1rawData_106;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1rawData_106;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_106;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_106;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(age_decile)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_107
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData (age_decile, count_value)  AS (
# MAGIC  select floor((year(op.OBSERVATION_PERIOD_START_DATE) - p.YEAR_OF_BIRTH)/10) as age_decile,
# MAGIC  datediff(day,op.observation_period_start_date,op.observation_period_end_date) as count_value
# MAGIC  FROM
# MAGIC  (
# MAGIC  select person_id, 
# MAGIC  op.observation_period_start_date,
# MAGIC  op.observation_period_end_date,
# MAGIC  ROW_NUMBER() over (PARTITION by op.person_id order by op.observation_period_start_date asc) as rn
# MAGIC  from ${omop_catalog}.${omop_schema}.observation_period op
# MAGIC  ) op
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.person p on op.person_id = p.person_id
# MAGIC  where op.rn = 1
# MAGIC ),
# MAGIC overallStats (age_decile, avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select age_decile,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC  group by age_decile
# MAGIC ),
# MAGIC statsView (age_decile, count_value, total, rn) as
# MAGIC (
# MAGIC  select age_decile,
# MAGIC  count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by age_decile, count_value
# MAGIC ),
# MAGIC priorStats (age_decile,count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.age_decile, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.age_decile = p.age_decile and p.rn <= s.rn
# MAGIC  group by s.age_decile, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 107 as analysis_id,
# MAGIC  CAST(o.age_decile AS STRING) as age_decile,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.age_decile = o.age_decile
# MAGIC GROUP BY o.age_decile, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_107
# MAGIC  ZORDER BY age_decile;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_107
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, age_decile as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_107
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_107
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_107;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_107;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_108
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC  select
# MAGIC  floor(datediff(day,op1.observation_period_start_date,op1.observation_period_end_date)/30) as stratum_1,
# MAGIC  COUNT(distinct p1.person_id) as count_value
# MAGIC  from ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join
# MAGIC  (select person_id,
# MAGIC  OBSERVATION_PERIOD_START_DATE,
# MAGIC  OBSERVATION_PERIOD_END_DATE,
# MAGIC  ROW_NUMBER() over (PARTITION by person_id order by observation_period_start_date asc) as rn1
# MAGIC  from ${omop_catalog}.${omop_schema}.observation_period
# MAGIC  ) op1
# MAGIC  on p1.PERSON_ID = op1.PERSON_ID
# MAGIC  where op1.rn1 = 1
# MAGIC  group by floor(datediff(day,op1.observation_period_start_date,op1.observation_period_end_date)/30)
# MAGIC )
# MAGIC  SELECT
# MAGIC 108 as analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2,
# MAGIC  cast(null as STRING) as stratum_3,
# MAGIC  cast(null as STRING) as stratum_4,
# MAGIC  cast(null as STRING) as stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_108
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_109
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH century   AS (SELECT  CAST('19' as STRING) num union select '20' num), 
# MAGIC tens as (select '0' num union select '1' num union select '2' num union select '3' num union select '4' num union select '5' num union select '6' num union select '7' num union select '8' num union select '9' num),
# MAGIC ones as (select '0' num union select '1' num union select '2' num union select '3' num union select '4' num union select '5' num union select '6' num union select '7' num union select '8' num union select '9' num),
# MAGIC months as (select '01' as num union select '02' num union select '03' num union select '04' num union select '05' num union select '06' num union select '07' num union select '08' num union select '09' num union select '10' num union select '11' num union select '12' num),
# MAGIC date_keys as (select concat(century.num, tens.num, ones.num,months.num) obs_month from century cross join tens cross join ones cross join months),
# MAGIC -- From date_keys, we just need each year and the first and last day of each year
# MAGIC ymd as (
# MAGIC select cast(left(obs_month,4) as integer) as obs_year,
# MAGIC  min(cast(right(left(obs_month,6),2) as integer)) as month_start,
# MAGIC  1 as day_start,
# MAGIC  max(cast(right(left(obs_month,6),2) as integer)) as month_end,
# MAGIC  31 as day_end
# MAGIC  from date_keys
# MAGIC  where right(left(obs_month,6),2) in ('01','12')
# MAGIC  group by left(obs_month,4)
# MAGIC ),
# MAGIC -- This gives us each year and the first and last day of the year 
# MAGIC year_ranges as (
# MAGIC select obs_year,
# MAGIC  to_date(cast(obs_year as string) || '-' || cast(month_start as string) || '-' || cast(day_start as string)) obs_year_start,
# MAGIC  to_date(cast(obs_year as string) || '-' || cast(month_end as string) || '-' || cast(day_end as string)) obs_year_end
# MAGIC  from ymd
# MAGIC  where obs_year >= (select min(year(observation_period_start_date)) from ${omop_catalog}.${omop_schema}.observation_period)
# MAGIC  and obs_year <= (select max(year(observation_period_start_date)) from ${omop_catalog}.${omop_schema}.observation_period)
# MAGIC ) 
# MAGIC  SELECT
# MAGIC 109 AS analysis_id, 
# MAGIC  CAST(yr.obs_year AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2, 
# MAGIC  CAST(NULL AS STRING) AS stratum_3, 
# MAGIC  CAST(NULL AS STRING) AS stratum_4, 
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(DISTINCT op.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation_period op
# MAGIC CROSS JOIN 
# MAGIC  year_ranges yr
# MAGIC WHERE
# MAGIC  op.observation_period_start_date <= yr.obs_year_start
# MAGIC AND
# MAGIC  op.observation_period_end_date >= yr.obs_year_end
# MAGIC GROUP BY 
# MAGIC  yr.obs_year;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_109
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_110
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 110 as analysis_id, 
# MAGIC  CAST(t1.obs_month AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(distinct op1.PERSON_ID) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation_period op1
# MAGIC join 
# MAGIC (
# MAGIC  SELECT DISTINCT 
# MAGIC  YEAR(observation_period_start_date)*100 + MONTH(observation_period_start_date) AS obs_month,
# MAGIC  to_date(cast(YEAR(observation_period_start_date) as string) || '-' || cast(MONTH(observation_period_start_date) as string) || '-' || cast(1 as string))
# MAGIC  AS obs_month_start,
# MAGIC  last_day(observation_period_start_date) AS obs_month_end
# MAGIC  FROM ${omop_catalog}.${omop_schema}.observation_period
# MAGIC ) t1 on op1.observation_period_start_date <= t1.obs_month_start
# MAGIC  and op1.observation_period_end_date >= t1.obs_month_end
# MAGIC group by t1.obs_month;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_110
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_111
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC  select
# MAGIC  YEAR(observation_period_start_date)*100 + month(OBSERVATION_PERIOD_START_DATE) as stratum_1,
# MAGIC  COUNT(distinct op1.PERSON_ID) as count_value
# MAGIC  from
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op1
# MAGIC  group by YEAR(observation_period_start_date)*100 + month(OBSERVATION_PERIOD_START_DATE)
# MAGIC )
# MAGIC  SELECT
# MAGIC 111 as analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2,
# MAGIC  cast(null as STRING) as stratum_3,
# MAGIC  cast(null as STRING) as stratum_4,
# MAGIC  cast(null as STRING) as stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_111
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_112
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC  select
# MAGIC  YEAR(observation_period_end_date)*100 + month(observation_period_end_date) as stratum_1,
# MAGIC  COUNT(distinct op1.PERSON_ID) as count_value
# MAGIC  from
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op1
# MAGIC  group by YEAR(observation_period_end_date)*100 + month(observation_period_end_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 112 as analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2,
# MAGIC  cast(null as STRING) as stratum_3,
# MAGIC  cast(null as STRING) as stratum_4,
# MAGIC  cast(null as STRING) as stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_112
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_113
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 113 as analysis_id, 
# MAGIC  CAST(op1.num_periods AS STRING) as stratum_1, 
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(distinct op1.PERSON_ID) as count_value
# MAGIC FROM
# MAGIC (select person_id, COUNT(OBSERVATION_period_start_date) as num_periods from ${omop_catalog}.${omop_schema}.observation_period group by PERSON_ID) op1
# MAGIC group by op1.num_periods;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_113
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1temp_dates_116
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC distinct 
# MAGIC  YEAR(observation_period_start_date) as obs_year 
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation_period
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_116
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC  select
# MAGIC  t1.obs_year as stratum_1,
# MAGIC  p1.gender_concept_id as stratum_2,
# MAGIC  floor((t1.obs_year - p1.year_of_birth)/10) as stratum_3,
# MAGIC  COUNT(distinct p1.PERSON_ID) as count_value
# MAGIC  from
# MAGIC  ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op1
# MAGIC  on p1.person_id = op1.person_id
# MAGIC  ,
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1temp_dates_116 t1
# MAGIC  where year(op1.OBSERVATION_PERIOD_START_DATE) <= t1.obs_year
# MAGIC  and year(op1.OBSERVATION_PERIOD_END_DATE) >= t1.obs_year
# MAGIC  group by t1.obs_year,
# MAGIC  p1.gender_concept_id,
# MAGIC  floor((t1.obs_year - p1.year_of_birth)/10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 116 as analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) as stratum_1,
# MAGIC  cast(stratum_2 as STRING) as stratum_2,
# MAGIC  cast(stratum_3 as STRING) as stratum_3,
# MAGIC  cast(null as STRING) as stratum_4,
# MAGIC  cast(null as STRING) as stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_116
# MAGIC  ZORDER BY stratum_1;
# MAGIC TRUNCATE TABLE ${results_catalog}.${results_schema}.xpi8onh1temp_dates_116;
# MAGIC DROP TABLE ${results_catalog}.${results_schema}.xpi8onh1temp_dates_116;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- start of refactored analysis 117
# MAGIC
# MAGIC CREATE TABLE  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_117_1
# MAGIC USING DELTA
# MAGIC cluster by (date_begin, date_end)
# MAGIC AS
# MAGIC WITH century  AS 
# MAGIC   ( SELECT  CAST('19' as STRING) num union select '20' num),
# MAGIC   tens as 
# MAGIC   (
# MAGIC     select '0' num union select '1' num union select '2' num union select '3' num union       select '4' num union select '5' num union select '6' num union select '7' num union select '8' num union select '9' num
# MAGIC   ),
# MAGIC   ones as 
# MAGIC   (
# MAGIC     select '0' num union select '1' num union select '2' num union select '3' num union select '4' num union select '5' num union select '6' num union select '7' num union select '8' num union select '9' num
# MAGIC   ),
# MAGIC   months as 
# MAGIC   (
# MAGIC       select '01' as num union select '02' num union select '03' num union select '04' num union select '05' num union select '06' num union select '07' num union select '08' num union select '09' num union select '10' num union select '11' num union select '12' num
# MAGIC   ),
# MAGIC   op_dates as 
# MAGIC   (
# MAGIC     select 
# MAGIC         min(observation_period_start_date) as min_date, 
# MAGIC         max(observation_period_start_date) as max_date 
# MAGIC     from ${omop_catalog}.${omop_schema}.observation_period
# MAGIC   ),
# MAGIC  date_keys as 
# MAGIC  (
# MAGIC     select 
# MAGIC       cast(concat(century.num, tens.num, ones.num,months.num) as int) ints,
# MAGIC       cast(concat(century.num, tens.num, ones.num)||'-'||months.num||'-01' as date) as date_begin,
# MAGIC       last_day(cast(concat(century.num, tens.num, ones.num)||'-'||months.num||'-01' as date)) as date_end
# MAGIC     from op_dates, century 
# MAGIC     cross join tens 
# MAGIC     cross join ones 
# MAGIC     cross join months
# MAGIC     where (cast(concat(century.num, tens.num, ones.num)||'-'||months.num||'-01' as date) + INTERVAL 1 month) >= op_dates.min_date
# MAGIC     and (last_day(cast(concat(century.num, tens.num, ones.num)||'-'||months.num||'-01' as date)) + INTERVAL 1 month) <= op_dates.max_date
# MAGIC  )
# MAGIC  select * from date_keys;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 117 part 2
# MAGIC
# MAGIC CREATE TABLE  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_117_2
# MAGIC USING DELTA
# MAGIC cluster by (ints, person_id)
# MAGIC AS
# MAGIC select distinct t2.ints, op2.person_id
# MAGIC  from ${omop_catalog}.${omop_schema}.observation_period op2,  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_117_1 t2
# MAGIC  where op2.observation_period_start_date <= t2.date_end
# MAGIC  and IF(op2.observation_period_end_date > CURRENT_DATE, CURRENT_DATE, op2.observation_period_end_date) >= t2.date_begin

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 117 part 3
# MAGIC
# MAGIC CREATE TABLE  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_117_3
# MAGIC USING DELTA
# MAGIC cluster by (ints)
# MAGIC AS
# MAGIC SELECT
# MAGIC  CAST(op1.ints AS STRING) as ints,
# MAGIC  COALESCE(COUNT(op1.PERSON_ID),0) as count_value
# MAGIC FROM  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_117_2 op1
# MAGIC group by op1.ints

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 117 part 4
# MAGIC
# MAGIC CREATE TABLE  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_117
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC  117 as analysis_id,
# MAGIC  CAST(t1.ints AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  coalesce(op1.count_value, 0) as count_value
# MAGIC FROM  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_117_1 t1
# MAGIC  left join  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_117_3 op1 on t1.ints = op1.ints

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 117 cleanup temp tables
# MAGIC
# MAGIC drop table  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_117_1;
# MAGIC drop table  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_117_2;
# MAGIC drop table  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_117_3;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_119
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 119 as analysis_id,
# MAGIC  CAST(op1.period_type_concept_id AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(*) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation_period op1
# MAGIC group by op1.period_type_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_119
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_200
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 200 AS analysis_id,
# MAGIC  CAST(vo.visit_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(DISTINCT vo.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vo.person_id = op.person_id
# MAGIC AND 
# MAGIC  vo.visit_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  vo.visit_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  vo.visit_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_200
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_201
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 201 AS analysis_id,
# MAGIC  CAST(vo.visit_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(vo.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vo.person_id = op.person_id
# MAGIC AND 
# MAGIC  vo.visit_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  vo.visit_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  vo.visit_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_201
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_202
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  vo.visit_concept_id AS stratum_1,
# MAGIC  YEAR(vo.visit_start_date) * 100 + MONTH(vo.visit_start_date) AS stratum_2,
# MAGIC  COUNT(DISTINCT vo.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vo.person_id = op.person_id
# MAGIC AND 
# MAGIC  vo.visit_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  vo.visit_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  vo.visit_concept_id,
# MAGIC  YEAR(vo.visit_start_date) * 100 + MONTH(vo.visit_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 202 as analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_202
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_203
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(person_id, count_value)  AS (
# MAGIC SELECT 
# MAGIC  vo.person_id,
# MAGIC  COUNT(DISTINCT vo.visit_concept_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vo.person_id = op.person_id
# MAGIC AND 
# MAGIC  vo.visit_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  vo.visit_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  vo.person_id
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC ),
# MAGIC statsView (count_value, total, rn) as
# MAGIC (
# MAGIC  select count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by count_value
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on p.rn <= s.rn
# MAGIC  group by s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 203 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN overallStats o
# MAGIC GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_203
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_203
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_203
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_203
# MAGIC  ZORDER BY count_value;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_203;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_203;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_204
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  vo.visit_concept_id AS stratum_1,
# MAGIC  YEAR(vo.visit_start_date) AS stratum_2,
# MAGIC  p.gender_concept_id AS stratum_3,
# MAGIC  FLOOR((YEAR(vo.visit_start_date) - p.year_of_birth) / 10) AS stratum_4,
# MAGIC  COUNT(DISTINCT p.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_occurrence vo 
# MAGIC ON 
# MAGIC  p.person_id = vo.person_id
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vo.person_id = op.person_id
# MAGIC AND 
# MAGIC  vo.visit_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  vo.visit_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  vo.visit_concept_id,
# MAGIC  YEAR(vo.visit_start_date),
# MAGIC  p.gender_concept_id,
# MAGIC  FLOOR((YEAR(vo.visit_start_date) - p.year_of_birth) / 10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 204 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 as STRING) AS stratum_2,
# MAGIC  CAST(stratum_3 as STRING) AS stratum_3,
# MAGIC  CAST(stratum_4 as STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_204
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_206
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData (stratum1_id, stratum2_id, count_value)  AS (
# MAGIC SELECT 
# MAGIC  v.visit_concept_id AS stratum1_id,
# MAGIC  p.gender_concept_id AS stratum2_id,
# MAGIC  v.visit_start_year - p.year_of_birth AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  vo.person_id,
# MAGIC  vo.visit_concept_id,
# MAGIC  MIN(YEAR(vo.visit_start_date)) AS visit_start_year
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  vo.person_id = op.person_id
# MAGIC  AND 
# MAGIC  vo.visit_start_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  vo.visit_start_date <= op.observation_period_end_date
# MAGIC  GROUP BY 
# MAGIC  vo.person_id,
# MAGIC  vo.visit_concept_id
# MAGIC  ) v
# MAGIC ON 
# MAGIC  p.person_id = v.person_id
# MAGIC ),
# MAGIC overallStats (stratum1_id, stratum2_id, avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select stratum1_id,
# MAGIC  stratum2_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM rawData
# MAGIC  group by stratum1_id, stratum2_id
# MAGIC ),
# MAGIC statsView (stratum1_id, stratum2_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select stratum1_id, stratum2_id, count_value, COUNT(*) as total, row_number() over (partition by stratum1_id, stratum2_id order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by stratum1_id, stratum2_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum1_id, stratum2_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 206 as analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
# MAGIC GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_206
# MAGIC  ZORDER BY stratum1_id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_206
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
# MAGIC cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_206
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_206
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_206;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_206;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_207
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 207 as analysis_id, 
# MAGIC  cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(vo1.PERSON_ID) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.visit_occurrence vo1
# MAGIC  left join ${omop_catalog}.${omop_schema}.person p1
# MAGIC  on p1.person_id = vo1.person_id
# MAGIC where p1.person_id is null;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_209
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 209 as analysis_id,
# MAGIC  cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(vo1.PERSON_ID) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.visit_occurrence vo1
# MAGIC  left join ${omop_catalog}.${omop_schema}.care_site cs1
# MAGIC  on vo1.care_site_id = cs1.care_site_id
# MAGIC where vo1.care_site_id is not null
# MAGIC  and cs1.care_site_id is null;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_210
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 210 AS analysis_id,
# MAGIC  CAST(NULL AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC LEFT JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vo.person_id = op.person_id
# MAGIC AND 
# MAGIC  vo.visit_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  vo.visit_start_date <= op.observation_period_end_date
# MAGIC WHERE 
# MAGIC  op.person_id IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_212
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC  select
# MAGIC  YEAR(visit_start_date) as stratum_1,
# MAGIC  p1.gender_concept_id as stratum_2,
# MAGIC  floor((year(visit_start_date) - p1.year_of_birth)/10) as stratum_3,
# MAGIC  COUNT(distinct p1.PERSON_ID) as count_value
# MAGIC  from ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join
# MAGIC  ${omop_catalog}.${omop_schema}.visit_occurrence vo1
# MAGIC  on p1.person_id = vo1.person_id
# MAGIC  group by
# MAGIC  YEAR(visit_start_date),
# MAGIC  p1.gender_concept_id,
# MAGIC  floor((year(visit_start_date) - p1.year_of_birth)/10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 212 as analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) as stratum_1,
# MAGIC  cast(stratum_2 as STRING) as stratum_2,
# MAGIC  cast(stratum_3 as STRING) as stratum_3,
# MAGIC  cast(null as STRING) as stratum_4,
# MAGIC  cast(null as STRING) as stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_212
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_213
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(stratum_id, count_value)  AS (
# MAGIC  select visit_concept_id AS stratum_id, datediff(day,visit_start_date,visit_end_date) as count_value
# MAGIC  from ${omop_catalog}.${omop_schema}.visit_occurrence vo inner join 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op on vo.person_id = op.person_id
# MAGIC  -- only include events that occur during observation period
# MAGIC  where vo.visit_start_date >= op.observation_period_start_date and
# MAGIC  COALESCE(vo.visit_end_date,vo.visit_start_date) <= op.observation_period_end_date
# MAGIC ),
# MAGIC overallStats (stratum_id, avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select stratum_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM rawData
# MAGIC  group by stratum_id
# MAGIC ),
# MAGIC statsView (stratum_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select stratum_id, count_value, COUNT(*) as total, row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by stratum_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum_id = p.stratum_id and p.rn <= s.rn
# MAGIC  group by s.stratum_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 213 as analysis_id,
# MAGIC  CAST(o.stratum_id AS STRING) AS stratum_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum_id = o.stratum_id
# MAGIC GROUP BY o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_213
# MAGIC  ZORDER BY stratum_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_213
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum_id as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_213
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_213
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_213;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_213;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_220
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(vo.visit_start_date) * 100 + MONTH(vo.visit_start_date) AS stratum_1,
# MAGIC  COUNT(vo.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vo.person_id = op.person_id
# MAGIC AND 
# MAGIC  vo.visit_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  vo.visit_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  YEAR(vo.visit_start_date) * 100 + MONTH(vo.visit_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 220 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_220
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_221
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(vo.visit_start_date) AS stratum_1,
# MAGIC  COUNT(DISTINCT vo.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vo.person_id = op.person_id
# MAGIC AND 
# MAGIC  vo.visit_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  vo.visit_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  YEAR(vo.visit_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 221 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_221
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_225
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 225 AS analysis_id,
# MAGIC  CAST(vo.visit_source_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vo.person_id = op.person_id
# MAGIC AND 
# MAGIC  vo.visit_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  vo.visit_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  visit_source_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_225
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_226
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 226 as analysis_id, 
# MAGIC  CAST(v.visit_concept_id AS STRING) as stratum_1,
# MAGIC  v.cdm_table as stratum_2,
# MAGIC  cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  v.record_count as count_value
# MAGIC FROM
# MAGIC (
# MAGIC  select 'drug_exposure' cdm_table, coalesce(visit_concept_id,0) visit_concept_id, count(*) record_count
# MAGIC  from ${omop_catalog}.${omop_schema}.drug_exposure t
# MAGIC  left join ${omop_catalog}.${omop_schema}.visit_occurrence v on t.visit_occurrence_id = v.visit_occurrence_id
# MAGIC  group by visit_concept_id
# MAGIC  union
# MAGIC  select 'condition_occurrence' cdm_table, coalesce(visit_concept_id,0) visit_concept_id, count(*) record_count
# MAGIC  from ${omop_catalog}.${omop_schema}.condition_occurrence t
# MAGIC  left join ${omop_catalog}.${omop_schema}.visit_occurrence v on t.visit_occurrence_id = v.visit_occurrence_id
# MAGIC  group by visit_concept_id
# MAGIC  union
# MAGIC  select 'device_exposure' cdm_table, coalesce(visit_concept_id,0) visit_concept_id, count(*) record_count
# MAGIC  from ${omop_catalog}.${omop_schema}.device_exposure t
# MAGIC  left join ${omop_catalog}.${omop_schema}.visit_occurrence v on t.visit_occurrence_id = v.visit_occurrence_id
# MAGIC  group by visit_concept_id
# MAGIC  union
# MAGIC  select 'procedure_occurrence' cdm_table, coalesce(visit_concept_id,0) visit_concept_id, count(*) record_count
# MAGIC  from ${omop_catalog}.${omop_schema}.procedure_occurrence t
# MAGIC  left join ${omop_catalog}.${omop_schema}.visit_occurrence v on t.visit_occurrence_id = v.visit_occurrence_id
# MAGIC  group by visit_concept_id
# MAGIC  union
# MAGIC  select 'measurement' cdm_table, coalesce(visit_concept_id,0) visit_concept_id, count(*) record_count
# MAGIC  from ${omop_catalog}.${omop_schema}.measurement t
# MAGIC  left join ${omop_catalog}.${omop_schema}.visit_occurrence v on t.visit_occurrence_id = v.visit_occurrence_id
# MAGIC  group by visit_concept_id
# MAGIC  union
# MAGIC  select 'observation' cdm_table, coalesce(visit_concept_id,0) visit_concept_id, count(*) record_count
# MAGIC  from ${omop_catalog}.${omop_schema}.observation t
# MAGIC  left join ${omop_catalog}.${omop_schema}.visit_occurrence v on t.visit_occurrence_id = v.visit_occurrence_id
# MAGIC  group by visit_concept_id
# MAGIC ) v;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_300
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 300 as analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC COUNT(distinct provider_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.provider;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_301
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 301 as analysis_id,
# MAGIC CAST(specialty_concept_id AS STRING) as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC COUNT(distinct provider_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.provider
# MAGIC group by specialty_CONCEPT_ID;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_301
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_303 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 303 as analysis_id,
# MAGIC  cast(p.specialty_concept_id AS STRING) AS stratum_1,
# MAGIC  cast(vo.visit_concept_id AS STRING) AS stratum_2,
# MAGIC  cast(null as STRING) as stratum_3,
# MAGIC  cast(null as STRING) as stratum_4,
# MAGIC  cast(null as STRING) as stratum_5, 
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.provider p
# MAGIC  join ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  on vo.provider_id = p.provider_id
# MAGIC  group by p.specialty_concept_id, visit_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_303 
# MAGIC   ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_325 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 325 as analysis_id,
# MAGIC  cast(specialty_source_concept_id AS STRING) AS stratum_1,
# MAGIC  cast(null AS STRING) AS stratum_2,
# MAGIC  cast(null as STRING) as stratum_3,
# MAGIC  cast(null as STRING) as stratum_4,
# MAGIC  cast(null as STRING) as stratum_5, 
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.provider
# MAGIC  group by specialty_source_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_325 
# MAGIC   ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_400
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 400 AS analysis_id,
# MAGIC  CAST(co.condition_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(DISTINCT co.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  co.person_id = op.person_id
# MAGIC AND 
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  co.condition_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_400
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_401
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 401 AS analysis_id,
# MAGIC  CAST(co.condition_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(co.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  co.person_id = op.person_id
# MAGIC AND 
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  co.condition_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_401
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_402
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  co.condition_concept_id AS stratum_1,
# MAGIC  YEAR(co.condition_start_date) * 100 + MONTH(co.condition_start_date) AS stratum_2,
# MAGIC  COUNT(DISTINCT co.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  co.person_id = op.person_id
# MAGIC AND 
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  co.condition_concept_id,
# MAGIC  YEAR(co.condition_start_date) * 100 + MONTH(co.condition_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 402 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_402
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_403
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(person_id, count_value)  AS (
# MAGIC SELECT 
# MAGIC  co.person_id,
# MAGIC  COUNT(DISTINCT co.condition_concept_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  co.person_id = op.person_id
# MAGIC AND 
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  co.person_id
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC ),
# MAGIC statsView (count_value, total, rn) as
# MAGIC (
# MAGIC  select count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by count_value
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on p.rn <= s.rn
# MAGIC  group by s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 403 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN overallStats o
# MAGIC GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_403
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_403
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_403
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_403
# MAGIC  ZORDER BY count_value;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_403;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_403;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_404
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  co.condition_concept_id AS stratum_1,
# MAGIC  YEAR(co.condition_start_date) AS stratum_2,
# MAGIC  p.gender_concept_id AS stratum_3,
# MAGIC  FLOOR((YEAR(co.condition_start_date) - p.year_of_birth) / 10) AS stratum_4,
# MAGIC  COUNT(DISTINCT p.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.condition_occurrence co 
# MAGIC ON 
# MAGIC  p.person_id = co.person_id
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  co.person_id = op.person_id
# MAGIC AND 
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  co.condition_concept_id,
# MAGIC  YEAR(co.condition_start_date),
# MAGIC  p.gender_concept_id,
# MAGIC  FLOOR((YEAR(co.condition_start_date) - p.year_of_birth) / 10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 404 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(stratum_3 AS STRING) AS stratum_3,
# MAGIC  CAST(stratum_4 AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_404
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_405
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 405 AS analysis_id,
# MAGIC  CAST(co.condition_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(co.condition_type_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(co.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  co.person_id = op.person_id
# MAGIC AND 
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  co.condition_CONCEPT_ID,
# MAGIC  co.condition_type_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_405
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(subject_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1rawData_406
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC c.condition_concept_id AS subject_id,
# MAGIC  p.gender_concept_id,
# MAGIC  (c.condition_start_year - p.year_of_birth) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  co.person_id,
# MAGIC  co.condition_concept_id,
# MAGIC  MIN(YEAR(co.condition_start_date)) AS condition_start_year
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  co.person_id = op.person_id
# MAGIC  AND 
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC  GROUP BY 
# MAGIC  co.person_id,
# MAGIC  co.condition_concept_id
# MAGIC  ) c 
# MAGIC ON 
# MAGIC  p.person_id = c.person_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1rawData_406
# MAGIC  ZORDER BY subject_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_406
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH overallStats (stratum1_id, stratum2_id, avg_value, stdev_value, min_value, max_value, total)  AS (
# MAGIC  select subject_id as stratum1_id,
# MAGIC  gender_concept_id as stratum2_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_406
# MAGIC  group by subject_id, gender_concept_id
# MAGIC ),
# MAGIC statsView (stratum1_id, stratum2_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, COUNT(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_406
# MAGIC  group by subject_id, gender_concept_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum1_id, stratum2_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 406 as analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
# MAGIC GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_406
# MAGIC  ZORDER BY stratum1_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_406
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
# MAGIC cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_406
# MAGIC ;
# MAGIC
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_406
# MAGIC  ZORDER BY stratum_1;
# MAGIC  
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_406;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_406;
# MAGIC truncate Table ${results_catalog}.${results_schema}.xpi8onh1rawData_406;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1rawData_406;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_414
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 414 AS analysis_id,
# MAGIC  CAST(co.condition_status_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  co.person_id = op.person_id
# MAGIC AND 
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  co.condition_status_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_414
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_415
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 415 AS analysis_id,
# MAGIC  CAST(co.condition_type_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  co.person_id = op.person_id
# MAGIC AND 
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  co.condition_type_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_415
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_416
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 416 AS analysis_id,
# MAGIC  CAST(co.condition_status_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(co.condition_type_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  co.person_id = op.person_id
# MAGIC AND 
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  co.condition_status_concept_id, co.condition_type_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_416
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_420
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(co.condition_start_date) * 100 + MONTH(co.condition_start_date) AS stratum_1,
# MAGIC  COUNT(co.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  co.person_id = op.person_id
# MAGIC AND 
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  YEAR(co.condition_start_date) * 100 + MONTH(co.condition_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 420 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_420
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_425
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 425 AS analysis_id,
# MAGIC  CAST(co.condition_source_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  co.person_id = op.person_id
# MAGIC AND 
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  co.condition_source_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_425
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_500
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 500 AS analysis_id,
# MAGIC  CAST(d.cause_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(DISTINCT d.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.death d
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  d.person_id = op.person_id
# MAGIC AND 
# MAGIC  d.death_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  d.death_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  d.cause_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_500
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_501
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 501 AS analysis_id,
# MAGIC  CAST(d.cause_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(d.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.death d
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  d.person_id = op.person_id
# MAGIC AND 
# MAGIC  d.death_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  d.death_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  d.cause_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_501
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_502
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(d.death_date) * 100 + MONTH(d.death_date) AS stratum_1,
# MAGIC  COUNT(DISTINCT d.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.death d
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  d.person_id = op.person_id
# MAGIC AND 
# MAGIC  d.death_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  d.death_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  YEAR(d.death_date) * 100 + MONTH(d.death_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 502 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_502
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_504
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(d.death_date) AS stratum_1,
# MAGIC  p.gender_concept_id AS stratum_2,
# MAGIC  FLOOR((YEAR(d.death_date) - p.year_of_birth) / 10) AS stratum_3,
# MAGIC  COUNT(DISTINCT p.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.death d 
# MAGIC ON 
# MAGIC  p.person_id = d.person_id
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  d.person_id = op.person_id
# MAGIC AND 
# MAGIC  d.death_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  d.death_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  YEAR(d.death_date),
# MAGIC  p.gender_concept_id,
# MAGIC  FLOOR((YEAR(d.death_date) - p.year_of_birth) / 10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 504 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(stratum_3 AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_504
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_505
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 505 AS analysis_id,
# MAGIC  CAST(d.death_type_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(d.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.death d
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  d.person_id = op.person_id
# MAGIC AND 
# MAGIC  d.death_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  d.death_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  d.death_type_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_505
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_506
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(stratum_id, count_value)  AS (
# MAGIC SELECT 
# MAGIC  p.gender_concept_id AS stratum_id,
# MAGIC  d.death_year - p.year_of_birth AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  d.person_id,
# MAGIC  MIN(YEAR(d.death_date)) AS death_year
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.death d
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  d.person_id = op.person_id
# MAGIC  AND 
# MAGIC  d.death_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  d.death_date <= op.observation_period_end_date 
# MAGIC  GROUP BY 
# MAGIC  d.person_id
# MAGIC  ) d
# MAGIC ON 
# MAGIC  p.person_id = d.person_id
# MAGIC ),
# MAGIC overallStats (stratum_id, avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select stratum_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM rawData
# MAGIC  group by stratum_id
# MAGIC ),
# MAGIC statsView (stratum_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select stratum_id, count_value, COUNT(*) as total, row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by stratum_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum_id = p.stratum_id and p.rn <= s.rn
# MAGIC  group by s.stratum_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 506 as analysis_id,
# MAGIC  CAST(o.stratum_id AS STRING) AS stratum_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum_id = o.stratum_id
# MAGIC GROUP BY o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_506
# MAGIC  ZORDER BY stratum_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_506
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum_id as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_506
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_506
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_506;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_506;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_511
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 511 AS analysis_id,
# MAGIC  CAST(NULL AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(count_value) AS count_value,
# MAGIC  MIN(count_value) AS min_value,
# MAGIC  MAX(count_value) AS max_value,
# MAGIC  CAST(AVG(1.0 * count_value) AS FLOAT) AS avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) AS stdev_value,
# MAGIC  MAX(CASE WHEN p1 <= 0.50 THEN count_value ELSE - 9999 END) AS median_value,
# MAGIC  MAX(CASE WHEN p1 <= 0.10 THEN count_value ELSE - 9999 END) AS p10_value,
# MAGIC  MAX(CASE WHEN p1 <= 0.25 THEN count_value ELSE - 9999 END) AS p25_value,
# MAGIC  MAX(CASE WHEN p1 <= 0.75 THEN count_value ELSE - 9999 END) AS p75_value,
# MAGIC  MAX(CASE WHEN p1 <= 0.90 THEN count_value ELSE - 9999 END) AS p90_value
# MAGIC FROM
# MAGIC (
# MAGIC SELECT 
# MAGIC  datediff(day,d.death_date,co.max_date) AS count_value,
# MAGIC  1.0 * (ROW_NUMBER() OVER (ORDER BY datediff(day,d.death_date,co.max_date))) / (COUNT(*) OVER () + 1) AS p1
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.death d
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  co.person_id,
# MAGIC  MAX(co.condition_start_date) AS max_date
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  co.person_id = op.person_id
# MAGIC  AND 
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  co.condition_start_date <= op.observation_period_end_date 
# MAGIC  GROUP BY 
# MAGIC  co.person_id
# MAGIC  ) co 
# MAGIC ON d.person_id = co.person_id
# MAGIC  ) t1;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_511
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_512
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(count_value)  AS (
# MAGIC SELECT 
# MAGIC  datediff(day,d.death_date,de.max_date) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.death d
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  de.person_id,
# MAGIC  MAX(de.drug_exposure_start_date) AS max_date
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  de.person_id = op.person_id
# MAGIC  AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date 
# MAGIC  GROUP BY 
# MAGIC  de.person_id
# MAGIC  ) de
# MAGIC ON 
# MAGIC  d.person_id = de.person_id
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC ),
# MAGIC statsView (count_value, total, rn) as
# MAGIC (
# MAGIC  select count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by count_value
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on p.rn <= s.rn
# MAGIC  group by s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 512 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN overallStats o
# MAGIC GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_512
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_512
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_512
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_512
# MAGIC  ZORDER BY count_value;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_512;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_512;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_513
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(count_value)  AS (
# MAGIC SELECT 
# MAGIC  datediff(day,d.death_date,vo.max_date) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.death d
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  vo.person_id,
# MAGIC  MAX(vo.visit_start_date) AS max_date
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  vo.person_id = op.person_id
# MAGIC  AND 
# MAGIC  vo.visit_start_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  vo.visit_start_date <= op.observation_period_end_date 
# MAGIC  GROUP BY 
# MAGIC  vo.person_id
# MAGIC  ) vo
# MAGIC ON 
# MAGIC  d.person_id = vo.person_id
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC ),
# MAGIC statsView (count_value, total, rn) as
# MAGIC (
# MAGIC  select count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by count_value
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on p.rn <= s.rn
# MAGIC  group by s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 513 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN overallStats o
# MAGIC GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_513
# MAGIC  ZORDER BY count_value;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_513
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_513
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_513
# MAGIC  ZORDER BY count_value;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_513;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_513;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_514
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(count_value)  AS (
# MAGIC SELECT 
# MAGIC  datediff(day,d.death_date,po.max_date) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.death d
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  po.person_id,
# MAGIC  MAX(po.procedure_date) AS max_date
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.procedure_occurrence po
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  po.person_id = op.person_id
# MAGIC  AND 
# MAGIC  po.procedure_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  po.procedure_date <= op.observation_period_end_date 
# MAGIC  GROUP BY 
# MAGIC  po.person_id
# MAGIC  ) po
# MAGIC ON 
# MAGIC  d.person_id = po.person_id
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC ),
# MAGIC statsView (count_value, total, rn) as
# MAGIC (
# MAGIC  select count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by count_value
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on p.rn <= s.rn
# MAGIC  group by s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 514 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN overallStats o
# MAGIC GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_514
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_514
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_514
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_514
# MAGIC  ZORDER BY count_value;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_514;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_514;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_515
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(count_value)  AS (
# MAGIC SELECT 
# MAGIC  datediff(day,d.death_date,o.max_date) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.death d
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  o.person_id,
# MAGIC  MAX(o.observation_date) AS max_date
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.observation o
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  o.person_id = op.person_id
# MAGIC  AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  o.observation_date <= op.observation_period_end_date 
# MAGIC  GROUP BY 
# MAGIC  o.person_id
# MAGIC  ) o
# MAGIC ON 
# MAGIC  d.person_id = o.person_id
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC ),
# MAGIC statsView (count_value, total, rn) as
# MAGIC (
# MAGIC  select count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by count_value
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on p.rn <= s.rn
# MAGIC  group by s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 515 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN overallStats o
# MAGIC GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_515
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_515
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_515
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_515
# MAGIC  ZORDER BY count_value;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_515;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_515;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_525 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 525 as analysis_id,
# MAGIC  cast(cause_source_concept_id AS STRING) AS stratum_1,
# MAGIC  cast(null AS STRING) AS stratum_2,
# MAGIC  cast(null as STRING) as stratum_3,
# MAGIC  cast(null as STRING) as stratum_4,
# MAGIC  cast(null as STRING) as stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.death
# MAGIC  group by cause_source_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_525 
# MAGIC   ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_600
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 600 AS analysis_id,
# MAGIC  CAST(po.procedure_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(DISTINCT po.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.procedure_occurrence po
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  po.person_id = op.person_id
# MAGIC AND 
# MAGIC  po.procedure_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  po.procedure_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  po.procedure_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_600
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_601
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 601 AS analysis_id,
# MAGIC  CAST(po.procedure_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(po.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.procedure_occurrence po
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  po.person_id = op.person_id
# MAGIC AND 
# MAGIC  po.procedure_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  po.procedure_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  po.procedure_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_601
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_602
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  po.procedure_concept_id AS stratum_1,
# MAGIC  YEAR(po.procedure_date) * 100 + MONTH(po.procedure_date) AS stratum_2,
# MAGIC  COUNT(DISTINCT po.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.procedure_occurrence po
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  po.person_id = op.person_id
# MAGIC AND 
# MAGIC  po.procedure_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  po.procedure_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  po.procedure_concept_id,
# MAGIC  YEAR(po.procedure_date) * 100 + MONTH(po.procedure_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 602 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_602
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_603
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(count_value)  AS (
# MAGIC SELECT 
# MAGIC  COUNT(DISTINCT po.procedure_concept_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.procedure_occurrence po
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  po.person_id = op.person_id
# MAGIC AND 
# MAGIC  po.procedure_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  po.procedure_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  po.person_id
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC ),
# MAGIC statsView (count_value, total, rn) as
# MAGIC (
# MAGIC  select count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by count_value
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on p.rn <= s.rn
# MAGIC  group by s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 603 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN overallStats o
# MAGIC GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_603
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_603
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_603
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_603
# MAGIC  ZORDER BY count_value;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_603;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_603;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_604
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  po.procedure_concept_id AS stratum_1,
# MAGIC  YEAR(po.procedure_date) AS stratum_2,
# MAGIC  p.gender_concept_id AS stratum_3,
# MAGIC  FLOOR((YEAR(po.procedure_date) - p.year_of_birth) / 10) AS stratum_4,
# MAGIC  COUNT(DISTINCT p.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.procedure_occurrence po 
# MAGIC ON 
# MAGIC  p.person_id = po.person_id
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  po.person_id = op.person_id
# MAGIC AND 
# MAGIC  po.procedure_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  po.procedure_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  po.procedure_concept_id,
# MAGIC  YEAR(po.procedure_date),
# MAGIC  p.gender_concept_id,
# MAGIC  FLOOR((YEAR(po.procedure_date) - p.year_of_birth) / 10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 604 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(stratum_3 AS STRING) AS stratum_3,
# MAGIC  CAST(stratum_4 AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_604
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_605
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 605 AS analysis_id,
# MAGIC  CAST(po.procedure_CONCEPT_ID AS STRING) AS stratum_1,
# MAGIC  CAST(po.procedure_type_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(po.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.procedure_occurrence po
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  po.person_id = op.person_id
# MAGIC AND 
# MAGIC  po.procedure_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  po.procedure_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  po.procedure_CONCEPT_ID,
# MAGIC  po.procedure_type_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_605
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(subject_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1rawData_606
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC po.procedure_concept_id AS subject_id,
# MAGIC  p.gender_concept_id,
# MAGIC  po.procedure_start_year - p.year_of_birth AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  po.person_id,
# MAGIC  po.procedure_concept_id,
# MAGIC  MIN(YEAR(po.procedure_date)) AS procedure_start_year
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.procedure_occurrence po
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  po.person_id = op.person_id
# MAGIC  AND 
# MAGIC  po.procedure_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  po.procedure_date <= op.observation_period_end_date
# MAGIC  GROUP BY 
# MAGIC  po.person_id,
# MAGIC  po.procedure_concept_id
# MAGIC  ) po 
# MAGIC ON 
# MAGIC  p.person_id = po.person_id
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1rawData_606
# MAGIC  ZORDER BY subject_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_606
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH overallStats (stratum1_id, stratum2_id, avg_value, stdev_value, min_value, max_value, total)  AS (
# MAGIC  select subject_id as stratum1_id,
# MAGIC  gender_concept_id as stratum2_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_606
# MAGIC  group by subject_id, gender_concept_id
# MAGIC ),
# MAGIC statsView (stratum1_id, stratum2_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, COUNT(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_606
# MAGIC  group by subject_id, gender_concept_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum1_id, stratum2_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 606 as analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
# MAGIC GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_606
# MAGIC  ZORDER BY stratum1_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_606
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
# MAGIC cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_606
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_606
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_606;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_606;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1rawData_606;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1rawData_606;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_620
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(po.procedure_date) * 100 + MONTH(po.procedure_date) AS stratum_1,
# MAGIC  COUNT(po.person_id) AS count_value
# MAGIC FROM
# MAGIC  ${omop_catalog}.${omop_schema}.procedure_occurrence po
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  po.person_id = op.person_id
# MAGIC AND 
# MAGIC  po.procedure_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  po.procedure_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  YEAR(po.procedure_date)*100 + MONTH(po.procedure_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 620 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_620
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_625
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 625 AS analysis_id,
# MAGIC  CAST(po.procedure_source_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.procedure_occurrence po
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  po.person_id = op.person_id
# MAGIC AND 
# MAGIC  po.procedure_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  po.procedure_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  po.procedure_source_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_625
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_630
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 630 AS analysis_id,
# MAGIC  CAST(NULL AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.procedure_occurrence po
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  po.person_id = op.person_id
# MAGIC AND 
# MAGIC  po.procedure_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  po.procedure_date <= op.observation_period_end_date;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_691
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 691 AS analysis_id,
# MAGIC  CAST(po.procedure_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(po.prc_cnt AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  SUM(COUNT(po.person_id)) OVER (PARTITION BY po.procedure_concept_id ORDER BY po.prc_cnt DESC) AS count_value
# MAGIC FROM
# MAGIC (
# MAGIC  SELECT 
# MAGIC  po.procedure_concept_id,
# MAGIC  COUNT(po.procedure_occurrence_id) AS prc_cnt,
# MAGIC  po.person_id
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.procedure_occurrence po
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  po.person_id = op.person_id
# MAGIC  AND 
# MAGIC  po.procedure_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  po.procedure_date <= op.observation_period_end_date
# MAGIC  GROUP BY 
# MAGIC  po.person_id,
# MAGIC  po.procedure_concept_id
# MAGIC  ) po
# MAGIC GROUP BY 
# MAGIC  po.procedure_concept_id,
# MAGIC  po.prc_cnt;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_691
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_700
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 700 AS analysis_id,
# MAGIC  CAST(de.drug_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(DISTINCT de.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  de.drug_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_700
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_701
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 701 AS analysis_id,
# MAGIC  CAST(de.drug_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(de.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  de.drug_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_701
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_702
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  de.drug_concept_id AS stratum_1,
# MAGIC  YEAR(de.drug_exposure_start_date) * 100 + MONTH(de.drug_exposure_start_date) AS stratum_2,
# MAGIC  COUNT(DISTINCT de.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  de.drug_concept_id,
# MAGIC  YEAR(de.drug_exposure_start_date) * 100 + MONTH(de.drug_exposure_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 702 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_702
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_703
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(count_value)  AS (
# MAGIC SELECT 
# MAGIC  COUNT(DISTINCT de.drug_concept_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  de.person_id
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC ),
# MAGIC statsView (count_value, total, rn) as
# MAGIC (
# MAGIC  select count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by count_value
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on p.rn <= s.rn
# MAGIC  group by s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 703 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN overallStats o
# MAGIC GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_703
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_703
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_703
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_703
# MAGIC  ZORDER BY count_value;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_703;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_703;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_704
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  de.drug_concept_id AS stratum_1,
# MAGIC  YEAR(de.drug_exposure_start_date) AS stratum_2,
# MAGIC  p.gender_concept_id AS stratum_3,
# MAGIC  FLOOR((YEAR(de.drug_exposure_start_date) - p.year_of_birth) / 10) AS stratum_4,
# MAGIC  COUNT(DISTINCT p.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_exposure de 
# MAGIC ON 
# MAGIC  p.person_id = de.person_id
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  de.drug_concept_id,
# MAGIC  YEAR(de.drug_exposure_start_date),
# MAGIC  p.gender_concept_id,
# MAGIC  FLOOR((YEAR(de.drug_exposure_start_date) - p.year_of_birth) / 10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 704 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(stratum_3 AS STRING) AS stratum_3,
# MAGIC  CAST(stratum_4 AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_704
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_705
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 705 AS analysis_id,
# MAGIC  CAST(de.drug_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(de.drug_type_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(de.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  de.drug_CONCEPT_ID,
# MAGIC  de.drug_type_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_705
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(subject_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1rawData_706
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC de.drug_concept_id AS subject_id,
# MAGIC  p.gender_concept_id,
# MAGIC  de.drug_start_year - p.year_of_birth AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  de.person_id,
# MAGIC  de.drug_concept_id,
# MAGIC  MIN(YEAR(de.drug_exposure_start_date)) AS drug_start_year
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  de.person_id = op.person_id
# MAGIC  AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC  GROUP BY 
# MAGIC  de.person_id,
# MAGIC  de.drug_concept_id
# MAGIC  ) de 
# MAGIC ON 
# MAGIC  p.person_id = de.person_id
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1rawData_706
# MAGIC  ZORDER BY subject_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_706
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH overallStats (stratum1_id, stratum2_id, avg_value, stdev_value, min_value, max_value, total)  AS (
# MAGIC  select subject_id as stratum1_id,
# MAGIC  gender_concept_id as stratum2_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_706
# MAGIC  group by subject_id, gender_concept_id
# MAGIC ),
# MAGIC statsView (stratum1_id, stratum2_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, COUNT(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_706
# MAGIC  group by subject_id, gender_concept_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum1_id, stratum2_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 706 as analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
# MAGIC GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_706
# MAGIC  ZORDER BY stratum1_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_706
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
# MAGIC cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_706
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_706
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1rawData_706;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1rawData_706;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_706;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_706;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_715
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(stratum_id, count_value)  AS (
# MAGIC SELECT 
# MAGIC  de.drug_concept_id AS stratum_id,
# MAGIC  de.days_supply AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC WHERE 
# MAGIC  de.days_supply IS NOT NULL
# MAGIC ),
# MAGIC overallStats (stratum_id, avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select stratum_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM rawData
# MAGIC  group by stratum_id
# MAGIC ),
# MAGIC statsView (stratum_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select stratum_id, count_value, COUNT(*) as total, row_number() over (partition by stratum_id order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by stratum_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum_id = p.stratum_id and p.rn <= s.rn
# MAGIC  group by s.stratum_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 715 as analysis_id,
# MAGIC  CAST(o.stratum_id AS STRING) AS stratum_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum_id = o.stratum_id
# MAGIC GROUP BY o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_715
# MAGIC  ZORDER BY stratum_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_715
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum_id as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_715
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_715
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_715;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_715;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_716
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(stratum_id, count_value)  AS (
# MAGIC SELECT 
# MAGIC  de.drug_concept_id AS stratum_id,
# MAGIC  de.refills AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC WHERE 
# MAGIC  de.refills IS NOT NULL
# MAGIC ),
# MAGIC overallStats (stratum_id, avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select stratum_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM rawData
# MAGIC  group by stratum_id
# MAGIC ),
# MAGIC statsView (stratum_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select stratum_id, count_value, COUNT(*) as total, row_number() over (partition by stratum_id order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by stratum_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum_id = p.stratum_id and p.rn <= s.rn
# MAGIC  group by s.stratum_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 716 as analysis_id,
# MAGIC  CAST(o.stratum_id AS STRING) AS stratum_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum_id = o.stratum_id
# MAGIC GROUP BY o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_716
# MAGIC  ZORDER BY stratum_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_716
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum_id as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_716
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_716
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_716;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_716;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_717
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(stratum_id, count_value)  AS (
# MAGIC SELECT 
# MAGIC  de.drug_concept_id AS stratum_id,
# MAGIC  CAST(de.quantity AS FLOAT) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC WHERE 
# MAGIC  de.quantity IS NOT NULL
# MAGIC ),
# MAGIC overallStats (stratum_id, avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select stratum_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM rawData
# MAGIC  group by stratum_id
# MAGIC ),
# MAGIC statsView (stratum_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select stratum_id, count_value, COUNT(*) as total, row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by stratum_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum_id = p.stratum_id and p.rn <= s.rn
# MAGIC  group by s.stratum_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 717 as analysis_id,
# MAGIC  CAST(o.stratum_id AS STRING) AS stratum_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum_id = o.stratum_id
# MAGIC GROUP BY o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_717
# MAGIC  ZORDER BY stratum_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_717
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum_id as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_717
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_717
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_717;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_717;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_720
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(de.drug_exposure_start_date) * 100 + MONTH(de.drug_exposure_start_date) AS stratum_1,
# MAGIC  COUNT(de.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  YEAR(de.drug_exposure_start_date)*100 + MONTH(de.drug_exposure_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 720 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_720
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_725
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 725 AS analysis_id,
# MAGIC  CAST(de.drug_source_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  de.drug_source_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_725
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_791
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 791 AS analysis_id,
# MAGIC  CAST(de.drug_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(de.drg_cnt AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  SUM(COUNT(de.person_id)) OVER (PARTITION BY de.drug_concept_id ORDER BY de.drg_cnt DESC) AS count_value
# MAGIC FROM
# MAGIC (
# MAGIC  SELECT 
# MAGIC  de.drug_concept_id,
# MAGIC  COUNT(de.drug_exposure_id) AS drg_cnt,
# MAGIC  de.person_id
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  de.person_id = op.person_id
# MAGIC  AND 
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC  GROUP BY 
# MAGIC  de.person_id,
# MAGIC  de.drug_concept_id
# MAGIC  ) de
# MAGIC GROUP BY 
# MAGIC  de.drug_concept_id, 
# MAGIC  de.drg_cnt;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_791
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_800
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 800 AS analysis_id,
# MAGIC  CAST(o.observation_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(DISTINCT o.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation o
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  o.person_id = op.person_id
# MAGIC AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  o.observation_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_800
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_801
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 801 AS analysis_id,
# MAGIC  CAST(o.observation_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(o.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation o
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  o.person_id = op.person_id
# MAGIC AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  o.observation_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_801
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_802
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  o.observation_concept_id AS stratum_1,
# MAGIC  YEAR(o.observation_date) * 100 + MONTH(o.observation_date) AS stratum_2,
# MAGIC  COUNT(DISTINCT o.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.observation o
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  o.person_id = op.person_id
# MAGIC AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  o.observation_concept_id,
# MAGIC  YEAR(o.observation_date) * 100 + MONTH(o.observation_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 802 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_802
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_803
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(count_value)  AS (
# MAGIC SELECT 
# MAGIC  COUNT(DISTINCT o.observation_concept_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.observation o
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  o.person_id = op.person_id
# MAGIC AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  o.person_id
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC ),
# MAGIC statsView (count_value, total, rn) as
# MAGIC (
# MAGIC  select count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by count_value
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on p.rn <= s.rn
# MAGIC  group by s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 803 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN overallStats o
# MAGIC GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_803
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_803
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_803
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_803
# MAGIC  ZORDER BY count_value;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_803;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_803;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_804
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  o.observation_concept_id AS stratum_1,
# MAGIC  YEAR(o.observation_date) AS stratum_2,
# MAGIC  p.gender_concept_id AS stratum_3,
# MAGIC  FLOOR((YEAR(o.observation_date) - p.year_of_birth) / 10) AS stratum_4,
# MAGIC  COUNT(DISTINCT p.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation o 
# MAGIC ON 
# MAGIC  p.person_id = o.person_id
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  o.person_id = op.person_id
# MAGIC AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  o.observation_concept_id,
# MAGIC  YEAR(o.observation_date),
# MAGIC  p.gender_concept_id,
# MAGIC  FLOOR((YEAR(o.observation_date) - p.year_of_birth) / 10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 804 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(stratum_3 AS STRING) AS stratum_3,
# MAGIC  CAST(stratum_4 AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_804
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_805
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 805 AS analysis_id,
# MAGIC  CAST(o.observation_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(o.observation_type_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(o.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation o
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  o.person_id = op.person_id
# MAGIC AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  o.observation_concept_id,
# MAGIC  o.observation_type_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_805
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(subject_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1rawData_806
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC o.observation_concept_id AS subject_id,
# MAGIC  p.gender_concept_id,
# MAGIC  o.observation_start_year - p.year_of_birth AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  o.person_id,
# MAGIC  o.observation_concept_id,
# MAGIC  MIN(YEAR(o.observation_date)) AS observation_start_year
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.observation o
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  o.person_id = op.person_id
# MAGIC  AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC  GROUP BY 
# MAGIC  o.person_id,
# MAGIC  o.observation_concept_id
# MAGIC  ) o
# MAGIC ON 
# MAGIC  p.person_id = o.person_id
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1rawData_806
# MAGIC  ZORDER BY subject_id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_806
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH overallStats (stratum1_id, stratum2_id, avg_value, stdev_value, min_value, max_value, total)  AS (
# MAGIC  select subject_id as stratum1_id,
# MAGIC  gender_concept_id as stratum2_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_806
# MAGIC  group by subject_id, gender_concept_id
# MAGIC ),
# MAGIC statsView (stratum1_id, stratum2_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, COUNT(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_806
# MAGIC  group by subject_id, gender_concept_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum1_id, stratum2_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 806 as analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
# MAGIC GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_806
# MAGIC  ZORDER BY stratum1_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_806
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
# MAGIC cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_806
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_806
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1rawData_806;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1rawData_806;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_806;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_806;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_807
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 807 AS analysis_id,
# MAGIC  CAST(o.observation_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(o.unit_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(o.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation o
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  o.person_id = op.person_id
# MAGIC AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  o.observation_CONCEPT_ID,
# MAGIC  o.unit_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_807
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_814
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 814 as analysis_id, 
# MAGIC  cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(o1.PERSON_ID) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation o1
# MAGIC where o1.value_as_number is null
# MAGIC  and o1.value_as_string is null
# MAGIC  and o1.value_as_concept_id is null;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1overallStats_815
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC o.subject_id AS stratum1_id,
# MAGIC  o.unit_concept_id AS stratum2_id,
# MAGIC  CAST(AVG(1.0 * o.count_value) AS FLOAT) AS avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) AS stdev_value,
# MAGIC  MIN(o.count_value) AS min_value,
# MAGIC  MAX(o.count_value) AS max_value,
# MAGIC  COUNT(*) AS total
# MAGIC FROM
# MAGIC (
# MAGIC  SELECT 
# MAGIC  o.observation_concept_id AS subject_id,
# MAGIC  o.unit_concept_id,
# MAGIC  CAST(o.value_as_number AS FLOAT) AS count_value
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.observation o
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  o.person_id = op.person_id
# MAGIC  AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC  WHERE 
# MAGIC  o.unit_concept_id IS NOT NULL
# MAGIC  AND 
# MAGIC  o.value_as_number IS NOT NULL
# MAGIC  ) o
# MAGIC GROUP BY 
# MAGIC  o.subject_id,
# MAGIC  o.unit_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1overallStats_815
# MAGIC  ZORDER BY stratum1_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1statsView_815
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC o.subject_id AS stratum1_id,
# MAGIC  o.unit_concept_id AS stratum2_id,
# MAGIC  o.count_value,
# MAGIC  COUNT(*) AS total,
# MAGIC  ROW_NUMBER() OVER (PARTITION BY o.subject_id,o.unit_concept_id ORDER BY o.count_value) AS rn
# MAGIC FROM
# MAGIC (
# MAGIC  SELECT 
# MAGIC  o.observation_concept_id AS subject_id,
# MAGIC  o.unit_concept_id,
# MAGIC  CAST(o.value_as_number AS FLOAT) AS count_value
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.observation o
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  o.person_id = op.person_id
# MAGIC  AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC  WHERE 
# MAGIC  o.unit_concept_id IS NOT NULL
# MAGIC  AND 
# MAGIC  o.value_as_number IS NOT NULL
# MAGIC  ) o
# MAGIC GROUP BY 
# MAGIC  o.subject_id,
# MAGIC  o.unit_concept_id,
# MAGIC  o.count_value;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1statsView_815
# MAGIC  ZORDER BY stratum1_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_815
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH priorStats (stratum1_id, stratum2_id, count_value, total, accumulated)  AS (
# MAGIC  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from ${results_catalog}.${results_schema}.xpi8onh1statsView_815 s
# MAGIC  join ${results_catalog}.${results_schema}.xpi8onh1statsView_815 p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 815 as analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join ${results_catalog}.${results_schema}.xpi8onh1overallStats_815 o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
# MAGIC GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_815
# MAGIC  ZORDER BY stratum1_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_815
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
# MAGIC cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_815
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_815
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1overallStats_815;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1overallStats_815;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1statsView_815;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1statsView_815;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_815;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_815;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_820
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(o.observation_date) * 100 + MONTH(o.observation_date) AS stratum_1,
# MAGIC  COUNT(o.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.observation o
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  o.person_id = op.person_id
# MAGIC AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  YEAR(o.observation_date) * 100 + MONTH(o.observation_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 820 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_820
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_822
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 822 AS analysis_id,
# MAGIC  CAST(o.observation_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(o.value_as_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation o
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  o.person_id = op.person_id
# MAGIC AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  o.observation_concept_id,
# MAGIC  o.value_as_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_822
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------


