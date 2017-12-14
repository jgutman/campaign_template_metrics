WITH campaign_lists AS (
  SELECT a.*,
    md.internal_user_id IS NOT NULL AS responder,
    pp.promo_period
  FROM analytics.dest_dinner_lapsed a
  LEFT JOIN dw.marketing_offers md
    ON md.internal_user_id = a.user_id
    AND ((md.offer_campaign_name = 'Sept Dest Dinner $25 off your next box'
         AND md.discount_name = '$25 off next box - expires 30 days from issue'
         AND a.test_group = 'test'
         AND a.offer = '25offer')
       OR (md.offer_campaign_name = 'Sept Dest Dinner $100 off your next 10 boxes'
            AND md.discount_name = '$10 off next 10 boxes'
            AND a.test_group = 'test'
            AND a.offer = '100offer'))
  CROSS JOIN (
    SELECT 'promo_period' AS promo_period
    UNION ALL
    SELECT 'post_promo'
  ) pp
),
last_subscription_change AS (
  SELECT a.user_id,
    CASE WHEN DATE(convert_timezone('America/New_York',
      subscription_changed_at)) <= '2017-10-18'
      THEN 'promo_period'
      WHEN DATE(convert_timezone('America/New_York',
        subscription_changed_at)) <= '2017-12-08'
        THEN 'post_promo'
      END AS promo_period,
  max(subscription_changed_at) AS last_change
  FROM campaign_lists a
  INNER JOIN dw.user_subscription_events use
  ON a.user_id = use.internal_user_id
  AND DATE(convert_timezone('America/New_York',
    subscription_changed_at)) >= '2017-09-18'
  GROUP BY 1,2
),
subscription_changes AS (
  SELECT use.internal_user_id,
    lsc.promo_period,
    use.subscription_status_change_event
  FROM last_subscription_change lsc
  INNER JOIN dw.user_subscription_events use
    ON lsc.user_id = use.internal_user_id
    AND lsc.last_change = use.subscription_changed_at
),
  boxes_ordered AS (
  SELECT bo.internal_user_id,
    CASE WHEN ship_date <= '2017-10-18'
      THEN 'promo_period'
      WHEN ship_date <= '2017-12-08'
      THEN 'post_promo'
    END AS promo_period,
    COUNT(*) AS total_boxes_ordered,
    SUM(gov) AS gov
  FROM campaign_lists a
  INNER JOIN dw.menu_order_boxes bo
  ON bo.internal_user_id = a.user_id
    AND ship_date >= '2017-09-18'
    AND status <> 'canceled'
  GROUP BY 1,2
)
  SELECT a.*,
    COALESCE(sc.subscription_status_change_event = 'cancelation',
    false) AS unsubscribed,
    COALESCE(sc.subscription_status_change_event = 'activation',
      false) AS activated,
    COALESCE(sc.subscription_status_change_event = 'reactivation',
    false) AS reactivated,
    COALESCE(bo.total_boxes_ordered, 0) AS total_boxes_ordered,
    COALESCE(bo.gov, 0) AS gov
  FROM campaign_lists a
  LEFT JOIN subscription_changes sc
  ON sc.internal_user_id = a.user_id
    AND sc.promo_period = a.promo_period
  LEFT JOIN boxes_ordered bo
  ON bo.internal_user_id = a.user_id
    AND bo.promo_period = a.promo_period
