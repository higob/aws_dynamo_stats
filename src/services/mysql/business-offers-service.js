const DatabaseService = require('./database-service');

class BusinessOffersService extends DatabaseService {
  constructor({ log, pool } = {}) {
    super({
      log,
      pool,
    });
  }

  async getAllStats() {
    const query = `select
        c.guid as chiropractor_id, bo.offer_id, bo.disabled, o.name, sub.*
      from business_offers bo
      inner join offers o on o.offer_id=bo.offer_id
      inner join chiropractors c on c.office_phone=bo.office_phone
      left join (
        select
        pa.chiropractor_id as cguid, bo.offer_id as oid,
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
        inner join adsets s on s.adset_id=ad.adset_id
        inner join platform_assets pa on pa.ad_account_id=ac.ad_account_id and pa.asset_id=aac.asset_id and pa.platform_id=aac.platform_id
        LEFT JOIN ad_platforms ap ON ap.id = pa.platform_id
        inner join chiropractors c on c.guid=pa.chiropractor_id
        inner join offers o on o.offer_id=s.offer_id
        inner join business_offers bo on bo.offer_id=o.offer_id and bo.office_phone=c.office_phone
        left join ads_insights ai on ai.ad_id = ad.ad_id
        left JOIN (
          SELECT source_id, local_time, count(phone) as num_leads, sum(case when status = 'patient' then 1 else 0 end) as num_conversions, platform_id
          FROM leads
          GROUP BY source_id, platform_id
        ) l ON l.source_id = ai.ad_id and date(ai.date) = date(l.local_time) and l.platform_id = ai.platform
        group by pa.chiropractor_id, bo.offer_id
      ) sub on sub.cguid=c.guid and sub.oid=bo.offer_id`;

    return this.execute(query);
  }
}

module.exports = BusinessOffersService;
