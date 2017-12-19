WITH campaign_lists AS (
  SELECT a.*,
    md.internal_user_id IS NOT NULL AS offer_redeemed,
    pp.promo_period,
    pp.end_date
  FROM analytics.new_customer_phased_offer a
  LEFT JOIN dw.marketing_discounts md
  ON md.internal_user_id = a.user_id
  AND ((md.discount_name = '$15 off 4th and 6th box'
    AND a.target_name = 'First-box customers (2-servings)')
    OR (md.discount_name = '$20 off 4th and 6th box'
    AND a.target_name = 'First-box customers (3- and 4- servings)')
    OR (md.discount_name = '$15 off 4th and 6th box'
    AND a.target_name = 'Second-box customers (2-servings)')
    OR (md.discount_name = '$20 off 4th and 6th box'
    AND a.target_name = 'Second-box customers (3- and 4-servings)')
    OR (md.discount_name = '$15 off 4th and 6th box'
    AND a.target_name = 'Third-box customers (2-servings)')
    OR (md.discount_name = '$20 off 4th and 6th box'
    AND a.target_name = 'Third-box customers (3- and 4-servings)'))
  CROSS JOIN (
  SELECT 'promo_period' AS promo_period,
    '2017-12-06' AS end_date
	UNION ALL
	SELECT 'post_promo_period' AS promo_period,
    '2017-12-18' AS end_date
  ) pp
),
  subscription_changes AS (
    SELECT a.user_id,
      a.promo_period,
      bool_or( subscription_status_change_event = 'activation')
        AS activated,
      bool_or( subscription_status_change_event = 'reactivation')
        AS reactivated,
      bool_or( subscription_status_change_event = 'cancelation')
        AS canceled
    FROM campaign_lists a
    INNER JOIN dw.user_subscription_events use
      ON a.user_id = use.internal_user_id
      AND DATE(convert_timezone('America/New_York',
             subscription_changed_at)) >= '2017-09-07'
      AND DATE(convert_timezone('America/New_York',
             subscription_changed_at)) <= a.end_date
  GROUP BY 1,2
),
  last_change AS (
    SELECT a.user_id,
      a.promo_period,
      max(subscription_changed_at) last_change
    FROM campaign_lists a
    INNER JOIN dw.user_subscription_events use
      ON a.user_id = use.internal_user_id
      AND DATE(convert_timezone('America/New_York',
             subscription_changed_at)) <= a.end_date
    GROUP BY 1,2
),
  active_at_end AS (
    SELECT a.user_id,
      a.promo_period,
      subscription_status_change_event LIKE '%activation' AS active_at_end
    FROM last_change a
    INNER JOIN dw.user_subscription_events use
      ON a.user_id = use.internal_user_id
      AND last_change = subscription_changed_at
),
  boxes_ordered AS (
    SELECT a.user_id,
      a.promo_period,
      COUNT( DISTINCT delivery_schedule_name ) AS total_boxes_ordered,
      bool_or( nth_delivery = 4 ) AS ordered_4th_box,
      bool_or( nth_delivery = 6 ) AS ordered_6th_box,
      SUM(gov) AS gov,
      SUM( CASE WHEN dessert_plates > 0 THEN 1 END )
        AS desserts_ordered
    FROM campaign_lists a
    INNER JOIN dw.menu_order_boxes bo
      ON bo.internal_user_id = a.user_id
      AND ship_date >= '2017-09-07'
      AND ship_date <= a.end_date
      AND status <> 'canceled'
    GROUP BY 1,2
),
  individual_metrics AS (
     SELECT a.*,
       COALESCE(active_at_end, false) AS active_at_end,
       COALESCE(canceled, false) AS canceled,
       COALESCE(activated, false) AS activated,
       COALESCE(reactivated, false) AS reactivated,
       COALESCE(ordered_4th_box, false) AS ordered_4th_box,
       COALESCE(ordered_6th_box, false) AS ordered_6th_box,
       COALESCE(total_boxes_ordered, 0) AS total_boxes_ordered,
       COALESCE(gov, 0) AS gov,
       COALESCE(desserts_ordered, 0) AS desserts_ordered
    FROM campaign_lists a
    LEFT JOIN subscription_changes sc
      ON sc.user_id = a.user_id
      AND sc.promo_period = a.promo_period
    LEFT JOIN active_at_end ae
      ON ae.user_id = a.user_id
      AND ae.promo_period = a.promo_period
    LEFT JOIN boxes_ordered bo
      ON bo.user_id = a.user_id
      AND bo.promo_period = a.promo_period
),
  responder_metric AS (
      SELECT user_id,
      ordered_4th_box AS responder
      FROM individual_metrics a
      WHERE promo_period = 'promo_period'
)
  SELECT a.promo_period,
    '2017-09-07' AS start_date
    a.end_date,
    r.responder,
    a.target_name,
    a.test_group,
    a.segment_group,
    a.offer_group,
    a.creative_template_name,
    a.population_name,
    a.offer_campaign_name,
    a.discount_name,
    a.message_offer,
    COUNT(*) AS total_segment_size,
    SUM(CASE WHEN offer_redeemed THEN 1 END) AS offer_redeemed,
    SUM(CASE WHEN canceled THEN 1 END) AS canceled,
    SUM(CASE WHEN activated THEN 1 END) AS activated,
    SUM(CASE WHEN reactivated THEN 1 END) AS reactivated,
    SUM(total_boxes_ordered) AS total_boxes_ordered,
    SUM(gov) AS gov,
    SUM(CASE WHEN ordered_4th_box THEN 1 END) AS ordered_4th_box,
    SUM(CASE WHEN ordered_6th_box THEN 1 END) AS ordered_6th_box,
    SUM(desserts_ordered) AS desserts_ordered,
    SUM(CASE WHEN active_at_end THEN 1 END) AS active_at_end
    FROM individual_metrics a
    LEFT JOIN responder_metric r
      ON a.user_id = r.user_id
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
