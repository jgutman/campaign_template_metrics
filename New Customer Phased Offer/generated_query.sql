
    WITH campaign_lists AS (
        SELECT a.*,
          md.internal_user_id IS NOT NULL AS responder,
          pp.promo_period
        FROM analytics.new_customer_phased_offer a
        LEFT JOIN dw.marketing_discounts md
          ON md.internal_user_id = a.user_id
          AND ((md.discount_name = '$15 off 4th and 6th box'
        AND a.test_group IN ('test')
	AND a.subscription_servings IN ('2servings'))
        OR (md.discount_name = '$20 off 4th and 6th box'
        AND a.test_group IN ('test')
	AND a.subscription_servings IN ('3serving', '4serving'))
        )
        CROSS JOIN (
        SELECT 'promo_period' AS promo_period
	UNION ALL
	SELECT 'post_promo' AS promo_period
        ) pp
    ),
      last_subscription_change AS (
        SELECT a.user_id,
          CASE WHEN DATE(convert_timezone('America/New_York',
            subscription_changed_at)) <= '2017-12-06'
    THEN 'promo_period'
    WHEN DATE(convert_timezone('America/New_York',
            subscription_changed_at)) <= '2017-12-12'
    THEN 'post_promo'
     END AS promo_period,
          max(subscription_changed_at) AS last_change
        FROM campaign_lists a
        INNER JOIN dw.user_subscription_events use
          ON a.user_id = use.internal_user_id
          AND DATE(convert_timezone('America/New_York',
            subscription_changed_at)) >= '2017-09-07'
        GROUP BY 1,2
    ),
      cum_subscription_changes AS (
      		SELECT lsc.user_id,
        text('promo_period') AS promo_period,
        max(last_change) AS last_change
        FROM last_subscription_change lsc
        WHERE promo_period IN ('promo_period')
        GROUP BY 1
    	UNION ALL
		SELECT lsc.user_id,
        text('post_promo') AS promo_period,
        max(last_change) AS last_change
        FROM last_subscription_change lsc
        WHERE promo_period IN ('promo_period', 'post_promo')
        GROUP BY 1
    
    ),
      subscription_changes AS (
        SELECT use.internal_user_id,
          csc.promo_period,
          use.subscription_status_change_event
        FROM cum_subscription_changes csc
        INNER JOIN dw.user_subscription_events use
          ON csc.user_id = use.internal_user_id
          AND csc.last_change = use.subscription_changed_at
    ),
      boxes_ordered AS (
        SELECT bo.internal_user_id,
          CASE WHEN ship_date <= '2017-12-06'
    THEN 'promo_period'
    WHEN ship_date <= '2017-12-12'
    THEN 'post_promo'
     END AS promo_period,
          COUNT(*) AS total_boxes_ordered,
          SUM(CASE WHEN nth_delivery = 1 THEN 1 END) AS _1st_boxes_ordered,
          SUM(CASE WHEN nth_delivery = 4 THEN 1 END) AS _4th_boxes_ordered,
          SUM(CASE WHEN nth_delivery = 6 THEN 1 END) AS _6th_boxes_ordered,
          SUM(gov) AS gov,
          SUM(CASE WHEN dessert_plates > 0 THEN 1 END) AS desserts_ordered
      FROM campaign_lists a
      INNER JOIN dw.menu_order_boxes bo
      ON bo.internal_user_id = a.user_id
        AND ship_date >= '2017-09-07'
        AND status <> 'canceled'
      GROUP BY 1,2
    ),
      individual_metrics AS (
        SELECT a.*,
          COALESCE(sc.subscription_status_change_event = 'cancelation',
            false) AS canceled,
          COALESCE(sc.subscription_status_change_event = 'activation',
            false) AS activated,
          COALESCE(sc.subscription_status_change_event = 'reactivation',
            false) AS reactivated,
          COALESCE(bo.total_boxes_ordered, 0) AS total_boxes_ordered,
	COALESCE(bo.gov, 0) AS gov,
	COALESCE(bo._1st_boxes_ordered, 0) AS _1st_boxes_ordered,
	COALESCE(bo._4th_boxes_ordered, 0) AS _4th_boxes_ordered,
	COALESCE(bo._6th_boxes_ordered, 0) AS _6th_boxes_ordered,
	COALESCE(bo.desserts_ordered, 0) AS desserts_ordered
      FROM campaign_lists a
      LEFT JOIN subscription_changes sc
      ON sc.internal_user_id = a.user_id
        AND sc.promo_period = a.promo_period
      LEFT JOIN boxes_ordered bo
      ON bo.internal_user_id = a.user_id
        AND bo.promo_period = a.promo_period
    )
      SELECT a.test_group,
		a.subscription_servings,
		a.boxes_received,
		a.responder,
        promo_period,
        COUNT(*) AS total,
        SUM(CASE WHEN canceled THEN 1 END) AS cancelations,
        SUM(CASE WHEN activated THEN 1 END) AS new_activations,
        SUM(CASE WHEN reactivated THEN 1 END) AS reactivations,
        SUM(total_boxes_ordered) AS total_boxes_ordered,
	SUM(gov) AS gov,
	SUM(_1st_boxes_ordered) AS _1st_boxes_ordered,
	SUM(_4th_boxes_ordered) AS _4th_boxes_ordered,
	SUM(_6th_boxes_ordered) AS _6th_boxes_ordered,
	SUM(desserts_ordered) AS desserts_ordered
      FROM individual_metrics a
      GROUP BY 1, 2, 3, 4, 5
    