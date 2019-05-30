---Monthcount import: https://platform.civisanalytics.com/spa/#/imports/23588454
---Budget tables import: https://platform.civisanalytics.com/spa/#/imports/21208802
---Staging table import: https://platform.civisanalytics.com/spa/#/imports/13823492
---GPEA Universe Workflow: https://platform.civisanalytics.com/spa/#/workflows/2958
---Analytics extract tables create: https://platform.civisanalytics.com/spa/#/scripts/sql/20929562


----New Donor
CREATE TEMP TABLE supporter_count_newdonor AS 
select * from
(SELECT        
a.Region, a.DebitYear, to_char(a.DebitDate,'Mon') as DebitMonth,a.DebitDate, a.ConstituentID,a.Type,c.Programme as Source, c.Resource, c.Team, a.CampaignId, c.Name, a.Amount, a.RGJoinDate, 
MAX(CASE WHEN b.DebitDate < TO_DATE(TO_CHAR(a.DebitYear,'0000')||'-'||TO_CHAR(DATE_PART('month',a.DebitDate),'00')||'-'||TO_CHAR(01,'00'), ' YYYY- MM- DD') THEN b.DebitDate ELSE NULL END) AS Expr1, 1 AS Count
FROM            
(select * from gpea_analytics.extract_opportunity where success=1) a
INNER JOIN
(select * from gpea_analytics.extract_opportunity where success=1) b ON a.AccountId = b.AccountId AND a.DebitDate >= b.DebitDate 
LEFT OUTER JOIN gpea_analytics.extract_campaign c ON a.CampaignId = c.CampaignID
inner join gpea_analytics.extract_contact d on a.ConstituentID=d.ConstituentID
WHERE        
(b.DebitDate >=TO_DATE(TO_CHAR(date_part('year',GETDATE() - 10) - 3,'0000')||'-'||TO_CHAR( 01,'00')||'-'||TO_CHAR( 01,'00'), ' YYYY- MM- DD'))
GROUP BY 
a.DebitYear, a.DebitMonth, a.ConstituentID, a.Type, a.CampaignId, a.RGJoinDate,a.DebitDate, c.Programme, a.Amount, a.Region, c.Team, c.Resource, c.Name
HAVING        
(a.RGJoinDate IS NULL 
OR 
a.RGJoinDate <= a.DebitDate) 
AND 
(a.DebitDate >= TO_DATE(TO_CHAR(date_part('year',GETDATE() - 10) - 1,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR( 01,'00'), ' YYYY- MM- DD')))z
where z.Expr1<TO_DATE(TO_CHAR(z.DebitYear - 1,'0000')||'-'||TO_CHAR(DATE_PART('month',z.DebitDate),'00')||'-'||TO_CHAR( 01,'00'), ' YYYY- MM- DD')
OR (z.Expr1 IS NULL);

-----CurrentDonor
Create Temp Table supporter_count_currentdonor AS
select * from 
(select 
b.Region, date_part(year,(DATEADD(Month, a.MonthCount - 1, b.DebitDate))) AS CurrentYear, to_char((DATEADD(month, a.MonthCount - 1, b.DebitDate)),'Mon') AS CurrentMonth, to_char(MAX(DATEADD(month, a.MonthCount - 1, b.DebitDate)),'YYYY-MM-DD') AS CurrentDate, b.ConstituentID, MAX(b.Type) AS Type, c.Programme AS Source, 0 AS Count
FROM            
gpea_staging.monthcount a
CROSS JOIN
gpea_analytics.extract_opportunity b
left join 
(select x.constituentID, x.minDebitdate,y.CampaignID,z.Programme from 
(select constituentID, min(debitdate) As minDebitdate from gpea_analytics.extract_opportunity where Success=1 group by constituentID) x
left join gpea_analytics.extract_opportunity y on x.constituentID=y.ConstituentID and x.minDebitdate=y.DebitDate
inner join gpea_analytics.extract_campaign z on z.CampaignId=y.CampaignID) c on b.ConstituentID=c.ConstituentID
inner join gpea_analytics.extract_contact d on b.ConstituentID=d.ConstituentID
WHERE        
(b.DebitDate >= TO_DATE(TO_CHAR(date_part(year,(GETDATE() - 10)) - 3,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), ' YYYY- MM- DD')) 
AND (GETDATE() >= TO_DATE(TO_CHAR(date_part(year,DATEADD(Month, a.MonthCount, b.DebitDate)),'0000')||'-'||TO_CHAR(date_part(month,(DATEADD(Month, a.MonthCount, b.DebitDate))),'00')||'-'||TO_CHAR(01,'00'), ' YYYY- MM- DD')) 
AND (a.MonthCount <= 12) and b.success=1
GROUP BY
date_part(year,(DATEADD(Month, a.MonthCount - 1, b.DebitDate))), to_char((DATEADD(month, a.MonthCount - 1, b.DebitDate)),'Mon'), b.ConstituentID, date_part(month, (DATEADD(month, a.MonthCount - 1, b.DebitDate))), b.Region,c.Programme
HAVING        
(date_part(year,(DATEADD(Month, a.MonthCount - 1, b.DebitDate))) >= date_part(year,(GETDATE() - 10)) - 1));

---LapsedDonor
create temp table supporter_count_lapseddonor as 
select * from
(select Region,debityear+1 as debityear,debitmonth, TO_DATE(TO_CHAR(DebitYear + 1,'0000')||'-'||TO_CHAR(date_part(month,DebitDate),'00')||'-'||TO_CHAR(01,'00'), ' YYYY- MM- DD') AS Date,ConstituentID,type,source,
amount,count from
(SELECT        
a.Region, a.DebitYear, to_char(a.DebitDate,'Mon') as DebitMonth, a.DebitDate, a.ConstituentID, a.Type, c.Programme AS Source, a.Amount, 1 AS Count, 
MIN(CASE WHEN b.DebitDate > a.Debitdate THEN b.DebitDate ELSE NULL END) AS Expr2
FROM            
(select * from gpea_analytics.extract_opportunity where success=1) a
INNER JOIN
(select * from gpea_analytics.extract_opportunity where success=1) b ON a.AccountId = b.AccountId AND a.DebitDate <= b.DebitDate
left join 
(select x.constituentID, x.minDebitdate,y.CampaignID,z.Programme from 
(select constituentID, min(debitdate) As minDebitdate from gpea_analytics.extract_opportunity where Success=1 group by constituentID) x
left join gpea_analytics.extract_opportunity y on x.constituentID=y.ConstituentID and x.minDebitdate=y.DebitDate
inner join gpea_analytics.extract_campaign z on z.CampaignId=y.CampaignID) c on a.ConstituentID=c.ConstituentID
inner join gpea_analytics.extract_contact d on a.ConstituentID=d.ConstituentID
WHERE        
(a.DebitDate >= TO_DATE(TO_CHAR(date_part(year,GETDATE() - 10)-3,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), ' YYYY- MM- DD'))
GROUP BY 
a.DebitYear, a.DebitMonth, a.ConstituentID, a.Type, a.Amount, TO_DATE(TO_CHAR(date_part(year,(DATEADD(month, 13, a.DebitDate))),'0000')||'-'||TO_CHAR(date_part(month,(DATEADD(month, 13,a.DebitDate))),'00')||'-'||TO_CHAR(01,'00'), ' YYYY- MM- DD'), a.Region,a.DebitDate,c.Programme
HAVING       
(TO_DATE(TO_CHAR(date_part(year,(DATEADD(month, 13, a.DebitDate))),'0000')||'-'||TO_CHAR(date_part(month,(DATEADD(month, 13,a.DebitDate))),'00')||'-'||TO_CHAR(01,'00'), ' YYYY- MM- DD') <= GETDATE())) x
where
(Expr2 >= TO_DATE(TO_CHAR(date_part(year,(DATEADD(month, 13, DebitDate))),'0000')||'-'||TO_CHAR(date_part(month,(DATEADD(month, 13,DebitDate))),'00')||'-'||TO_CHAR(01,'00'), ' YYYY- MM- DD')) 
OR
(Expr2 IS NULL));

---Canceled(Inactive):
create temp table supporter_count_canceleddonor as 
select * from 
(select date_part(year,(DATEADD(Month, 1, b.lastdebitsuccessdate))) AS Year,  to_char(DATEADD(month, 1, b.lastdebitsuccessdate), 'Mon') AS CurrentMonth, b.lastdebitsuccessdate as Date,
b.constituentID, 'Regular' as Type, c.programme as Source, b.region
 from gpea_analytics.extract_regulargiving b
 left join 
(select x.constituentID, x.minDebitdate,y.CampaignID,z.Programme from 
(select constituentID, min(debitdate) As minDebitdate from gpea_analytics.extract_opportunity where Success=1 group by constituentID) x
left join gpea_analytics.extract_opportunity y on x.constituentID=y.ConstituentID and x.minDebitdate=y.DebitDate
inner join gpea_analytics.extract_campaign z on z.CampaignId=y.CampaignID) c on b.ConstituentID=c.ConstituentID
where b.rgstatus='Inactive' and date_part(year,(DATEADD(Month, 1, b.lastdebitsuccessdate)))<=date_part(year,(GETDATE())) and date_part(month,DATEADD(month, 1, b.lastdebitsuccessdate))<=date_part(month,getdate()));

---DonatedDonor:
create temp table supporter_count_donateddonor  as 
select * from
(select date_part(year,a.paiddate) as Year, to_char(a.paiddate,'Mon') as Month, a.paiddate as date, b.constituentID, 'Regular' as Type, c.programme as Source, b.region
from gpea_analytics.extract_regulargiving b
inner join gpea_analytics.extract_transaction a on b.constituentID=a.constituentID
left join 
(select x.constituentID, x.minDebitdate,y.CampaignID,z.Programme from 
(select constituentID, min(debitdate) As minDebitdate from gpea_analytics.extract_opportunity where Success=1 group by constituentID) x
left join gpea_analytics.extract_opportunity y on x.constituentID=y.ConstituentID and x.minDebitdate=y.DebitDate
inner join gpea_analytics.extract_campaign z on z.CampaignId=y.CampaignID) c on b.ConstituentID=c.ConstituentID);


---unionall_HKTW:
create temp table supporter_count_unionHKTW as select * from (
----NewDonor
select 'Actual' as Comparison, Region, debityear as Year, debitmonth as Month, to_date(debitdate,'YYYY-MM-DD') as Date,
constituentID, campaignid, Name, Source,'' as Resource,
Team,Type, count as NewDonor_Actual, amount as NewDonorAmt_Actual, 0 as CurrentDonor_Actual,0 as LapseDonor_Actual, 0 as CanceledDonor,0 as DonatedDonor from supporter_count_newdonor
----CurrentDonor 
union all
select  'Actual' as Comparison, Region,currentyear as Year,currentmonth as Month,to_date(min(currentdate),'YYYY-MM-DD') as Date,
'' as constituentID,'' as comapignid, '' as Name, Source,'' as Resource,
'' as Team, type,0 as NewDonor_Actual, 0 as NewDonorAmt_Actual,count(distinct constituentID) as CurrentDonor_Actual, 0 as LapseDonor_Actual,0 as CanceledDonor,0 as DonatedDonor from supporter_count_currentdonor
Group by Region, CurrentYear, CurrentMonth, Type, Source
----LapsedDonor
union all
select 'Actual' as Comparison,Region,Debityear as Year, debitmonth as Month, to_date(Date,'YYYY-MM-DD'),
constituentID, '' as campaignID, '' as name, Source,'' as Resource,
'' as team,type, 0 as NewDonor_Actual, 0 as NewDonorAmt_Actual, 0 as CurrentDonor_Actual, Count as LapseDonor_Actual, 0 as CanceledDonor,0 as DonatedDonor from supporter_count_lapseddonor
----CanceledDonor
union all
select 'Actual' as Comparison,Region,Year, CurrentMonth as Month, to_date(Date,'YYYY-MM-DD'),
'' as constituentID, '' as campaignID, '' as name, Source,'' as Resource,
'' as team,type, 0 as NewDonor_Actual, 0 as NewDonorAmt_Actual, 0 as CurrentDonor_Actual, 0 as LapseDonor_Actual, count(distinct constituentID) as CanceledDonor,0 as DonatedDonor  from supporter_count_canceleddonor
Group by Region, Year, CurrentMonth, date,Type, Source
----DonatedDonor
union all
select 'Actual' as Comparison,Region,Year, Month as Month, to_date(Date,'YYYY-MM-DD'),
'' as constituentID, '' as campaignid, '' as name, Source, '' as Resource,
'' as team,type, 0 as NewDonor_Actual, 0 as NewDonorAmt_Actual, 0 as CurrentDonor_Actual, 0 as LapseDonor_Actual, 0 as CanceledDonor,count(distinct constituentID) as DonatedDonor  from supporter_count_donateddonor
Group by Region, Year, Month, date,Type, Source
---UnionBudgetData
union all
select comparison as Comparison,region as Region,year as Year, month as Month, to_date(date,'YYYY-MM-DD') as Date,
constituentid as constituentID, campaignid as campaignid,name,source as Source,resource as Resource,
team, type, case when source in ('DDC','DRTC','Reactivation','Telephone','Web') then activedonor_actual else 0 end as NewDonor_Actual,
0 as NewDonorAmt_Actual, case when source in ('Current') then activedonor_actual else 0 end as CurrentDonor_Actual, 
case when source in ('Lapsed') then -activedonor_actual else 0 end as LapseDonor_Actual, 0 as CanceledDonor,0 as DonatedDonor  from gpea_analytics.extract_2019budget_supporter
union all
select comparison as Comparison,region as Region,year as Year, month as Month, to_date(date,'YYYY-MM-DD') as Date,
constituentid as constituentID, campaignid as campaignid,name,source as Source,resource as Resource,
team, type, case when source in ('DDC','DRTC','Reactivation','Telephone','Web') then activedonor_actual else 0 end as NewDonor_Actual,
0 as NewDonorAmt_Actual, case when source in ('Current') then activedonor_actual else 0 end as CurrentDonor_Actual, 
case when source in ('Lapsed') then -activedonor_actual else 0 end as LapseDonor_Actual, 0 as CanceledDonor,0 as DonatedDonor  from gpea_analytics.extract_2018budget_supporter);

---createtable
DROP TABLE if exists gpea_reporting.table_report_supporter_count; 
CREATE TABLE gpea_reporting.table_report_supporter_count AS (
select * from supporter_count_unionHKTW);

-- GRANT Statements for GPEA Group
GRANT ALL ON SCHEMA gpea_analytics TO GROUP gpea;
GRANT ALL ON SCHEMA gpea_staging TO GROUP gpea;
GRANT ALL ON SCHEMA gpea_reporting TO GROUP gpea;
GRANT ALL ON SCHEMA public TO GROUP gpea;
GRANT ALL ON gpea_reporting.table_report_supporter_count TO GROUP gpea;
GRANT ALL ON gpea_staging.monthcount TO GROUP gpea;

GRANT ALL ON gpea_analytics.extract_activity_tfr TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_automatedtransaction TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_campaign TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_campaignmember TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_case TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_contact TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_creditcard TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_lead TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_opportunity TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_regulargiving TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_rgli TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_schedule TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_staging TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_tfrcall TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_transaction TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_transactionitem TO GROUP gpea;
GRANT ALL ON gpea_reporting.table_report_income TO GROUP gpea;

GRANT ALL ON gpea_staging.kr_supporter_alc TO GROUP gpea;
GRANT ALL ON gpea_staging.kr_refund TO GROUP gpea;
GRANT ALL ON gpea_staging.kr_income_account TO GROUP gpea;
GRANT ALL ON gpea_staging.kr_upgrade_monthly TO GROUP gpea;
GRANT ALL ON gpea_staging.kr_downgrade_monthly TO GROUP gpea;
GRANT ALL ON gpea_staging.kr_vw_mrm_history TO GROUP gpea;
GRANT ALL ON gpea_staging.kr_vw_mrm_payment_result TO GROUP gpea;
GRANT ALL ON gpea_staging.currency_conversion TO GROUP gpea;

GRANT ALL ON gpea_staging.kr_vw_mrm_groups TO GROUP gpea;

GRANT ALL ON gpea_analytics.extract_2017budget_income TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_2018budget_income TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_2018budget_supporter TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_2019budget_income TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_2019budget_supporter TO GROUP gpea;
GRANT ALL ON gpea_analytics.extract_budget_recode_group TO GROUP gpea;


-- GRANT Statements for GPEA Robot User
GRANT ALL ON SCHEMA gpea_analytics TO greenpeaceearobot;
GRANT ALL ON SCHEMA gpea_staging TO greenpeaceearobot;
GRANT ALL ON SCHEMA gpea_reporting TO greenpeaceearobot;
GRANT ALL ON SCHEMA public TO greenpeaceearobot;
GRANT ALL ON gpea_reporting.table_report_supporter_count TO greenpeaceearobot;
GRANT ALL ON gpea_staging.monthcount TO greenpeaceearobot;

GRANT ALL ON gpea_analytics.extract_activity_tfr TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_automatedtransaction TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_campaign TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_campaignmember TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_case TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_contact TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_creditcard TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_lead TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_opportunity TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_regulargiving TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_rgli TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_schedule TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_staging TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_tfrcall TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_transaction TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_transactionitem TO greenpeaceearobot;
GRANT ALL ON gpea_reporting.table_report_income TO greenpeaceearobot;

GRANT ALL ON gpea_staging.kr_supporter_alc TO greenpeaceearobot;
GRANT ALL ON gpea_staging.kr_refund TO greenpeaceearobot;
GRANT ALL ON gpea_staging.kr_income_account TO greenpeaceearobot;
GRANT ALL ON gpea_staging.kr_upgrade_monthly TO greenpeaceearobot;
GRANT ALL ON gpea_staging.kr_downgrade_monthly TO greenpeaceearobot;
GRANT ALL ON gpea_staging.kr_vw_mrm_history TO greenpeaceearobot;
GRANT ALL ON gpea_staging.kr_vw_mrm_payment_result TO greenpeaceearobot;
GRANT ALL ON gpea_staging.currency_conversion TO greenpeaceearobot;

GRANT ALL ON gpea_staging.kr_vw_mrm_groups TO greenpeaceearobot;

GRANT ALL ON gpea_analytics.extract_2017budget_income TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_2018budget_income TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_2018budget_supporter TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_2019budget_income TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_2019budget_supporter TO greenpeaceearobot;
GRANT ALL ON gpea_analytics.extract_budget_recode_group TO greenpeaceearobot;

-- GRANT Statements for Civis Group
GRANT ALL ON SCHEMA gpea_analytics TO GROUP civis;
GRANT ALL ON SCHEMA gpea_staging TO GROUP civis;
GRANT ALL ON SCHEMA gpea_reporting TO GROUP civis;
GRANT ALL ON SCHEMA public TO GROUP civis;
GRANT ALL ON gpea_reporting.table_report_supporter_count TO GROUP civis;
GRANT ALL ON gpea_staging.monthcount TO GROUP civis;

GRANT ALL ON gpea_analytics.extract_activity_tfr TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_automatedtransaction TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_campaign TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_campaignmember TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_case TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_contact TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_creditcard TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_lead TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_opportunity TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_regulargiving TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_rgli TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_schedule TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_staging TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_tfrcall TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_transaction TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_transactionitem TO GROUP civis;
GRANT ALL ON gpea_reporting.table_report_income TO GROUP civis;

GRANT ALL ON gpea_staging.kr_supporter_alc TO GROUP civis;
GRANT ALL ON gpea_staging.kr_refund TO GROUP civis;
GRANT ALL ON gpea_staging.kr_income_account TO GROUP civis;
GRANT ALL ON gpea_staging.kr_upgrade_monthly TO GROUP civis;
GRANT ALL ON gpea_staging.kr_downgrade_monthly TO GROUP civis;
GRANT ALL ON gpea_staging.kr_vw_mrm_history TO GROUP civis;
GRANT ALL ON gpea_staging.kr_vw_mrm_payment_result TO GROUP civis;
GRANT ALL ON gpea_staging.currency_conversion TO GROUP civis;

GRANT ALL ON gpea_staging.kr_vw_mrm_groups TO GROUP civis;

GRANT ALL ON gpea_analytics.extract_2017budget_income TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_2018budget_income TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_2018budget_supporter TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_2019budget_income TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_2019budget_supporter TO GROUP civis;
GRANT ALL ON gpea_analytics.extract_budget_recode_group TO GROUP civis;
