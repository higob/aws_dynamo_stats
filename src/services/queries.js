const total_interactions_query = `
SELECT guid, sum(total) as total_interactions FROM
(select c.guid, count(*) total from chiropractors c
inner join leads l on c.office_phone=l.office_phone
inner join messages m on m.lead_phone=l.phone
GROUP BY c.guid
UNION 
select c.guid, count(*) total from chiropractors c
inner join leads l on c.office_phone=l.office_phone
inner join calls s on s.lead_phone=l.phone
GROUP BY c.guid
) A
GROUP BY guid;
`;

const total_conversions_query = `
select c.guid, count(*) total_conversions from leads l
inner join chiropractors c ON l.office_phone=c.office_phone
where l.status='patient'
group by c.guid;
`;

const total_leads_query = `
SELECT c.guid, count(*) total_leads FROM leads l
INNER JOIN chiropractors c ON l.office_phone=c.office_phone
GROUP BY c.guid
`;

const total_spend_query = `
select p.guid, sum(i.spend) total_spend from ads_insights i
inner join ads a on i.ad_id = a.ad_id
inner join adsets s on a.adset_id=s.adset_id
inner join campaigns c on c.campaign_id=s.campaign_id
inner join ad_accounts t on t.ad_account_id=c.ad_account_id
inner join chiropractors p on p.office_phone=t.office_phone
group by p.guid;
`;

const roi_monthly_query = `
select guid, mon, sum((client_ltv * conversions / 100)) as 'return'
from
(select p.guid, coalesce(c.client_ltv, p.client_ltv) client_ltv,
count(*) as conversions,
	DATE_FORMAT(created, '%Y-%m-01') as mon
from leads l
inner join ads a on l.source_id = a.ad_id
inner join adsets s on a.adset_id=s.adset_id
inner join campaigns c on c.campaign_id=s.campaign_id
inner join ad_accounts t on t.ad_account_id=c.ad_account_id
inner join chiropractors p on p.office_phone=t.office_phone
where l.status='patient'
group by p.guid, c.campaign_id, DATE_FORMAT(created, '%Y-%m-01')
UNION ALL
select c.guid, c.client_ltv client_ltv,
count(*) as conversions, DATE_FORMAT(created, '%Y-%m-01') as mon
from leads l
inner join chiropractors c ON l.office_phone=c.office_phone
where l.status='patient' and l.source_id is null
group by c.guid, mon
) s1
group by guid, mon;
`;

const total_return_query = `
select p.guid, l.client_ltv as lead_client_ltv, 
SUM(coalesce(NULLIF(l.client_ltv, 0), c.client_ltv, p.client_ltv)) as total_return
from leads l
left join ads a on l.source_id = a.ad_id
left join adsets s on a.adset_id=s.adset_id
left join campaigns c on c.campaign_id=s.campaign_id
left join ad_accounts t on t.ad_account_id=c.ad_account_id
left join chiropractors p on p.office_phone=l.office_phone
where l.status='patient'
GROUP BY p.guid;
`;

const total_spend_query_grouped_by_asset_id = `
    select ss.id, p.guid, ss.file_url, sum(i.spend) total_spend from ads_insights i
    inner join ads a on a.ad_id=i.ad_id
    inner join assets_ad_creatives ac on ac.ad_creative_id=a.creative_id
    inner join assets ss on ss.id=ac.asset_id
    inner join platform_assets pa on pa.asset_id=ss.id
    inner join ad_accounts t on t.ad_account_id=pa.ad_account_id
    inner join chiropractors p on p.office_phone=t.office_phone
    group by ss.id;
`;

const total_impressions_query_grouped_by_asset_id = `
    select ss.id, p.guid, ss.file_url, sum(i.impressions) total_impressions from ads_insights i
    inner join ads a on a.ad_id=i.ad_id
    inner join assets_ad_creatives ac on ac.ad_creative_id=a.creative_id
    inner join assets ss on ss.id=ac.asset_id
    inner join platform_assets pa on pa.asset_id=ss.id
    inner join ad_accounts t on t.ad_account_id=pa.ad_account_id
    inner join chiropractors p on p.office_phone=t.office_phone
    group by ss.id;
`;

const total_reachs_query_grouped_by_asset_id = `
    select ss.id, p.guid, ss.file_url, sum(i.reach) total_reachs from ads_insights i
    inner join ads a on a.ad_id=i.ad_id
    inner join assets_ad_creatives ac on ac.ad_creative_id=a.creative_id
    inner join assets ss on ss.id=ac.asset_id
    inner join platform_assets pa on pa.asset_id=ss.id
    inner join ad_accounts t on t.ad_account_id=pa.ad_account_id
    inner join chiropractors p on p.office_phone=t.office_phone
    group by ss.id;
`;

const total_clicks_query_grouped_by_asset_id = `
    select ss.id, p.guid, ss.file_url, sum(i.clicks) total_clicks from ads_insights i
    inner join ads a on a.ad_id=i.ad_id
    inner join assets_ad_creatives ac on ac.ad_creative_id=a.creative_id
    inner join assets ss on ss.id=ac.asset_id
    inner join platform_assets pa on pa.asset_id=ss.id
    inner join ad_accounts t on t.ad_account_id=pa.ad_account_id
    inner join chiropractors p on p.office_phone=t.office_phone
    group by ss.id;
`;

const total_inline_link_clicks_query_grouped_by_asset_id = `
    select ss.id, p.guid, ss.file_url, sum(i.inline_link_clicks) total_inline_link_clicks from ads_insights i
    inner join ads a on a.ad_id=i.ad_id
    inner join assets_ad_creatives ac on ac.ad_creative_id=a.creative_id
    inner join assets ss on ss.id=ac.asset_id
    inner join platform_assets pa on pa.asset_id=ss.id
    inner join ad_accounts t on t.ad_account_id=pa.ad_account_id
    inner join chiropractors p on p.office_phone=t.office_phone
    group by ss.id;
`;

const calculate_all_non_generic_assets_statistics = `
SELECT c.office,c.owner,a.id,a.name,a.asset_type type, e.dashboard_uuid,a.created_at,c.guid, 
SUBSTR(replace(a.file_url, SUBSTRING_INDEX(a.file_url, '/', 3), ''),2) AS key_url,
SUM( IF( ap.name = 'facebook', ai.impressions, 0 ) ) AS views_facebook,  
SUM( IF( ap.name = 'instagram', ai.impressions, 0 ) ) AS views_instagram,
SUM( IF( ap.name = 'messenger', ai.impressions, 0 ) ) AS views_messenger,
SUM( IF( ap.name = 'audience_network', ai.impressions, 0 ) ) AS views_audience_network,
MAX(DATE(ai.date)) as last_used,
count(distinct ac.creative_id) as num_creatives,
count(distinct ad.ad_id) as num_ads,
sum(ai.spend) as ad_spend,
sum(ai.impressions) as views, ROUND(100*(sum(ai.inline_link_clicks)/sum(ai.impressions)), 3) as lctr,
round(sum(ai.spend)/round(sum(ai.impressions)/1000, 2), 2) as cpm,e.full_name,
sum(l.num_leads) as leads, ROUND(sum(ai.spend)/sum(l.num_leads), 2) as cost_per_lead,
sum(l.num_conversions) as shows, ROUND(sum(ai.spend)/sum(l.num_conversions), 2) as cost_per_show,
pa.disabled FROM assets a
LEFT JOIN employees e on e.dashboard_uuid = a.dashboard_uuid
LEFT JOIN chiropractors c ON e.guid = c.guid
LEFT JOIN assets_ad_creatives aac ON aac.asset_id = a.id
LEFT JOIN ad_creatives ac ON ac.creative_id = aac.ad_creative_id
LEFT JOIN ads ad ON ad.creative_id = ac.creative_id
LEFT JOIN ads_insights ai ON ai.ad_id = ad.ad_id
LEFT JOIN ad_platforms ap ON ap.id = ai.platform
LEFT JOIN platform_assets pa ON pa.asset_id = a.id
LEFT JOIN (
SELECT source_id, local_time, count(phone) as num_leads, sum(case when status = 'patient' then 1 else 0 end) as num_conversions, platform_id
FROM leads
GROUP BY source_id, platform_id
) l ON l.source_id = ai.ad_id and date(ai.date) = date(l.local_time) and l.platform_id = ai.platform
WHERE a.file_url not like '%generic%' AND e.full_name IS NOT NULL AND c.owner IS NOT NULL 
GROUP BY a.id
ORDER BY spend DESC, a.created_at DESC;
`;

const calculate_all_non_generic_assets_statistics_by_chiropractor_id = `
SELECT c.office,c.owner,a.id,a.name,a.asset_type type, e.dashboard_uuid,a.created_at,c.guid, 
SUBSTR(replace(a.file_url, SUBSTRING_INDEX(a.file_url, '/', 3), ''),2) AS key_url,
SUM( IF( ap.name = 'facebook', ai.impressions, 0 ) ) AS views_facebook,  
SUM( IF( ap.name = 'instagram', ai.impressions, 0 ) ) AS views_instagram,
SUM( IF( ap.name = 'messenger', ai.impressions, 0 ) ) AS views_messenger,
SUM( IF( ap.name = 'audience_network', ai.impressions, 0 ) ) AS views_audience_network,
MAX(DATE(ai.date)) as last_used,
count(distinct ac.creative_id) as num_creatives,
count(distinct ad.ad_id) as num_ads,
sum(ai.spend) as ad_spend,
sum(ai.impressions) as views, ROUND(100*(sum(ai.inline_link_clicks)/sum(ai.impressions)), 3) as lctr,
round(sum(ai.spend)/round(sum(ai.impressions)/1000, 2), 2) as cpm,e.full_name,
sum(l.num_leads) as leads, ROUND(sum(ai.spend)/sum(l.num_leads), 2) as cost_per_lead,
sum(l.num_conversions) as shows, ROUND(sum(ai.spend)/sum(l.num_conversions), 2) as cost_per_show,
pa.disabled FROM assets a
LEFT JOIN employees e on e.dashboard_uuid = a.dashboard_uuid
LEFT JOIN chiropractors c ON e.guid = c.guid
LEFT JOIN assets_ad_creatives aac ON aac.asset_id = a.id
LEFT JOIN ad_creatives ac ON ac.creative_id = aac.ad_creative_id
LEFT JOIN ads ad ON ad.creative_id = ac.creative_id
LEFT JOIN ads_insights ai ON ai.ad_id = ad.ad_id
LEFT JOIN ad_platforms ap ON ap.id = ai.platform
LEFT JOIN platform_assets pa ON pa.asset_id = a.id
LEFT JOIN (
SELECT source_id, local_time, count(phone) as num_leads, sum(case when status = 'patient' then 1 else 0 end) as num_conversions, platform_id
FROM leads
GROUP BY source_id, platform_id
) l ON l.source_id = ai.ad_id and date(ai.date) = date(l.local_time) and l.platform_id = ai.platform
WHERE a.file_url not like '%generic%' AND e.full_name IS NOT NULL AND c.owner IS NOT NULL and pa.chiropractor_id=?
GROUP BY a.id
ORDER BY spend DESC, a.created_at DESC;
`;

const calculate_all_generic_assets_statistics = `
select pa.asset_id,pa.chiropractor_id,a.name,a.asset_type type, a.created_at, pa.disabled,
SUBSTR(replace(a.file_url, SUBSTRING_INDEX(a.file_url, '/', 3), ''),2) AS key_url,
SUM( IF( ap.name = 'facebook', ai.impressions, 0 ) ) AS views_facebook,  
SUM( IF( ap.name = 'instagram', ai.impressions, 0 ) ) AS views_instagram,
SUM( IF( ap.name = 'messenger', ai.impressions, 0 ) ) AS views_messenger,
SUM( IF( ap.name = 'audience_network', ai.impressions, 0 ) ) AS views_audience_network,
MAX(DATE(ai.date)) as last_used,
count(distinct ac.creative_id) as num_creatives,
count(distinct ad.ad_id) as num_ads,
sum(ai.spend) as ad_spend,
sum(ai.impressions) as views, ROUND(100*(sum(ai.inline_link_clicks)/sum(ai.impressions)), 3) as lctr,
round(sum(ai.spend)/round(sum(ai.impressions)/1000, 2), 2) as cpm,
sum(l.num_leads) as leads, ROUND(sum(ai.spend)/sum(l.num_leads), 2) as cost_per_lead,
sum(l.num_conversions) as shows, ROUND(sum(ai.spend)/sum(l.num_conversions), 2) as cost_per_show
from ads ad
inner join ad_creatives ac on ac.creative_id=ad.creative_id
inner join assets_ad_creatives aac on aac.ad_creative_id = ad.creative_id
inner JOIN ad_platforms ap ON ap.id = aac.platform_id
inner join platform_assets pa on pa.ad_account_id=ac.ad_account_id and pa.asset_id=aac.asset_id and pa.platform_id=aac.platform_id
inner join assets a on a.id=pa.asset_id
left join ads_insights ai on ai.ad_id = ad.ad_id
left JOIN (
SELECT source_id, local_time, count(phone) as num_leads, sum(case when status = 'patient' then 1 else 0 end) as num_conversions, platform_id
FROM leads
GROUP BY source_id, platform_id
) l ON l.source_id = ai.ad_id and date(ai.date) = date(l.local_time) and l.platform_id = ai.platform
WHERE a.file_url like '%generic%' 
group by pa.asset_id,pa.chiropractor_id;
`;

const calculate_all_generic_assets_statistics_by_chiropractor_id = `
select pa.asset_id,pa.chiropractor_id,a.name,a.asset_type type, a.created_at, pa.disabled,
SUBSTR(replace(a.file_url, SUBSTRING_INDEX(a.file_url, '/', 3), ''),2) AS key_url,
SUM( IF( ap.name = 'facebook', ai.impressions, 0 ) ) AS views_facebook,  
SUM( IF( ap.name = 'instagram', ai.impressions, 0 ) ) AS views_instagram,
SUM( IF( ap.name = 'messenger', ai.impressions, 0 ) ) AS views_messenger,
SUM( IF( ap.name = 'audience_network', ai.impressions, 0 ) ) AS views_audience_network,
MAX(DATE(ai.date)) as last_used,
count(distinct ac.creative_id) as num_creatives,
count(distinct ad.ad_id) as num_ads,
sum(ai.spend) as ad_spend,
sum(ai.impressions) as views, ROUND(100*(sum(ai.inline_link_clicks)/sum(ai.impressions)), 3) as lctr,
round(sum(ai.spend)/round(sum(ai.impressions)/1000, 2), 2) as cpm,
sum(l.num_leads) as leads, ROUND(sum(ai.spend)/sum(l.num_leads), 2) as cost_per_lead,
sum(l.num_conversions) as shows, ROUND(sum(ai.spend)/sum(l.num_conversions), 2) as cost_per_show
from ads ad
inner join ad_creatives ac on ac.creative_id=ad.creative_id
inner join assets_ad_creatives aac on aac.ad_creative_id = ad.creative_id
inner JOIN ad_platforms ap ON ap.id = aac.platform_id
inner join platform_assets pa on pa.ad_account_id=ac.ad_account_id and pa.asset_id=aac.asset_id and pa.platform_id=aac.platform_id
inner join assets a on a.id=pa.asset_id
left join ads_insights ai on ai.ad_id = ad.ad_id
left JOIN (
SELECT source_id, local_time, count(phone) as num_leads, sum(case when status = 'patient' then 1 else 0 end) as num_conversions, platform_id
FROM leads
GROUP BY source_id, platform_id
) l ON l.source_id = ai.ad_id and date(ai.date) = date(l.local_time) and l.platform_id = ai.platform
WHERE a.file_url like '%generic%' and pa.chiropractor_id=?
group by pa.asset_id,pa.chiropractor_id;
`;

const all_ad_accounts = `select ad_account_id from ad_accounts`;

const all_ad_platform = `select id, name from ad_platforms`;

module.exports = {
  total_conversions_query,
  total_interactions_query,
  total_leads_query,
  total_spend_query,
  roi_monthly_query,
  total_return_query,
  total_spend_query_grouped_by_asset_id,
  total_impressions_query_grouped_by_asset_id,
  total_reachs_query_grouped_by_asset_id,
  total_clicks_query_grouped_by_asset_id,
  total_inline_link_clicks_query_grouped_by_asset_id,
  calculate_all_non_generic_assets_statistics,
  calculate_all_non_generic_assets_statistics_by_chiropractor_id,
  calculate_all_generic_assets_statistics,
  calculate_all_generic_assets_statistics_by_chiropractor_id,
  all_ad_accounts,
  all_ad_platform,
};
