----New Donor
CREATE TEMP TABLE supporter_count_newdonor AS 
select * from
(SELECT        
a.Region, a.DebitYear, a.DebitMonth,a.DebitDate, a.ConstituentID,a.Type,c.Programme as Source, c.Resource, c.Team, a.CampaignId, c.Name, a.Amount, a.RGJoinDate, 
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
OR (z.Expr1 IS NULL)
