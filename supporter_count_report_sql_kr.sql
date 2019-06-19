DROP TABLE if exists gpea_analytics.support_count_KR;

SELECT 
comparison, 
Region, 
Year,  
case 
when Month = 1 THEN 'Jan'
when Month = 2 THEN 'Feb'
when Month = 3 THEN 'Mar'
when Month = 4 THEN 'Apr'
when Month = 5 THEN 'May'
when Month = 6 THEN 'Jun'
when Month = 7 THEN 'Jul'
when Month = 8 THEN 'Aug'
when Month = 9 THEN 'Sep'
when Month = 10 THEN 'Oct'
when Month = 11 THEN 'Nov'
when Month = 12 THEN 'Dec'
END AS Month,
Date::date,
constituentID::varchar(5204), 
'' AS campaignid, 
'' AS Name, 
Source, 
Resource,
'' AS Team, 
Type, 
NewDonor_Actual, 
NewDonorAmt_Actual::numeric(18,2), 
CurrentDonor_Actual,
LapseDonor_Actual, 
CanceledDonor, 
DonatedDonor
INTO gpea_analytics.support_count_KR
FROM gpea_staging.kr_supporter_count;