WITH
DateSelector AS (
    SELECT *
    FROM
        ( VALUES (
             DATE (:start_date), DATE (:end_date),
                 :start_date, :end_date
                 ))
        AS t ("StartDate","EndDate","StartDateStr","EndDateStr")
),

-- WITH
-- DateSelector As (
--     SELECT *
--     FROM
--         ( VALUES (
--             DATE('2025-09-16'), CURRENT_DATE,
--                  '2025-09-16'
--                  ))
--         as t ("StartDate","EndDate","StartDateStr")
-- ),

SmartOfferExWo AS(
    SELECT
        COALESCE(
            element_at(ex.edp_raw_data_map, 'Identities_ReservationSid'),
            element_at(ex.edp_raw_data_map, 'Identities_SessionId')
            ) as reservation_id,
        1 AS smartoffermodelmatch,
        MAX(CASE
            WHEN ELEMENT_AT(ex.edp_raw_data_map,
                            'ExtraData_behavior') = 'ASK_SALE'
            THEN 1 
            ELSE 0 END) "Ask_for_the_Sale_c",
        MAX(CASE
            WHEN ELEMENT_AT(ex.edp_raw_data_map,
                            'ExtraData_behavior') = 'NON_PARTNER_DEVICES'
            THEN 1 ELSE 0 END) "Non_Partner_Coverage_c",
        MAX(CASE
            WHEN ELEMENT_AT(ex.edp_raw_data_map,
                            'ExtraData_behavior') = 'ENGAGEMENT_QUESTION'
            THEN 1 ELSE 0 END) "Engagement_Questions_c",
        MAX(CASE
            WHEN ELEMENT_AT(ex.edp_raw_data_map,
                            'ExtraData_behavior') = 'FUTURE_DEVICES'
            THEN 1 ELSE 0 END) "Future_Purchase_Details_c",
        MAX(CASE
            WHEN ELEMENT_AT(ex.edp_raw_data_map,
                            'ExtraData_behavior') = 'ENROLLMENT_CELEBRATION'
            THEN 1 ELSE 0 END) "Celebrate_Enrollment_c",
        MAX(CASE
            WHEN ELEMENT_AT(ex.edp_raw_data_map,
                            'ExtraData_behavior') = 'ASK_SEND_LINK'
            THEN 1 ELSE 0 END) "Ask_to_Send_Enrollment_Link_c",
        MAX(CASE
            WHEN ELEMENT_AT(ex.edp_raw_data_map,
                            'ExtraData_behavior') = 'COVERAGE'
            THEN 1 ELSE 0 END) "Examples_of_Coverage_c",
        MAX(CASE
            WHEN ELEMENT_AT(ex.edp_raw_data_map,
                            'ExtraData_behavior') = 'xdfd'
            THEN 1 ELSE 0 END) "Resolve_the_Customers_Concern_c",
        MAX(CASE
            WHEN ELEMENT_AT(ex.edp_raw_data_map,
                            'ExtraData_behavior') = 'PROTECTION'
            THEN 1 ELSE 0 END) "RRR_c",
        MAX(CASE
            WHEN ELEMENT_AT(ex.edp_raw_data_map,
                            'ExtraData_behavior') = 'BILLING'
            THEN 1 ELSE 0 END) "Price_c",
        MAX(CASE
            WHEN ELEMENT_AT(ex.edp_raw_data_map,
                            'ExtraData_behavior') = 'DISCOVERY_QUESTION'
            THEN 1 ELSE 0 END) "Discovery_Questions_c",
        MAX(CASE
            WHEN ELEMENT_AT(ex.edp_raw_data_map,
                            'ExtraData_behavior') = 'TRANSITION'
            THEN 1 ELSE 0 END) "Transition_Statement_c"
    FROM
        hive.care.l1_asurion_home_events_analytics_exwo ex
    WHERE 1 = 1
        AND ex.edp_updated_date >= (SELECT StartDateStr FROM DateSelector)
        AND DATE(CAST( AT_TIMEZONE(
                from_iso8601_timestamp(element_at(ex.edp_raw_data_map,'Time')),
                'America/Chicago') AS TIMESTAMP)) 
            BETWEEN (SELECT StartDate FROM DateSelector) 
                AND (SELECT EndDate FROM DateSelector)
        AND  ELEMENT_AT(ex.edp_raw_data_map,
                        'Name') = 'SalesChecklistBehaviorMentioned'
    GROUP BY
        COALESCE(
            element_at(ex.edp_raw_data_map, 'Identities_ReservationSid'),
            element_at(ex.edp_raw_data_map, 'Identities_SessionId')
            )
),

SmartOfferAlpha AS (
    SELECT
        COALESCE(
            element_at(au.edp_raw_data_map, 'Identities_ReservationSid'),
            element_at(au.edp_raw_data_map, 'Identities_SessionId')
            ) as reservation_id,
        1 AS smartoffermodelmatch,
        MAX(CASE
            WHEN ELEMENT_AT(au.edp_raw_data_map,
                            'ExtraData_behavior') = 'ASK_SALE'
            THEN 1 
            ELSE 0 END) "Ask_for_the_Sale_c",
        MAX(CASE
            WHEN ELEMENT_AT(au.edp_raw_data_map,
                            'ExtraData_behavior') = 'NON_PARTNER_DEVICES'
            THEN 1 ELSE 0 END) "Non_Partner_Coverage_c",
        MAX(CASE
            WHEN ELEMENT_AT(au.edp_raw_data_map,
                            'ExtraData_behavior') = 'ENGAGEMENT_QUESTION'
            THEN 1 ELSE 0 END) "Engagement_Questions_c",
        MAX(CASE
            WHEN ELEMENT_AT(au.edp_raw_data_map,
                            'ExtraData_behavior') = 'FUTURE_DEVICES'
            THEN 1 ELSE 0 END) "Future_Purchase_Details_c",
        MAX(CASE
            WHEN ELEMENT_AT(au.edp_raw_data_map,
                            'ExtraData_behavior') = 'ENROLLMENT_CELEBRATION'
            THEN 1 ELSE 0 END) "Celebrate_Enrollment_c",
        MAX(CASE
            WHEN ELEMENT_AT(au.edp_raw_data_map,
                            'ExtraData_behavior') = 'ASK_SEND_LINK'
            THEN 1 ELSE 0 END) "Ask_to_Send_Enrollment_Link_c",
        MAX(CASE
            WHEN ELEMENT_AT(au.edp_raw_data_map,
                            'ExtraData_behavior') = 'COVERAGE'
            THEN 1 ELSE 0 END) "Examples_of_Coverage_c",
        MAX(CASE
            WHEN ELEMENT_AT(au.edp_raw_data_map,
                            'ExtraData_behavior') = 'xdfd'
            THEN 1 ELSE 0 END) "Resolve_the_Customers_Concern_c",
        MAX(CASE
            WHEN ELEMENT_AT(au.edp_raw_data_map,
                            'ExtraData_behavior') = 'PROTECTION'
            THEN 1 ELSE 0 END) "RRR_c",
        MAX(CASE
            WHEN ELEMENT_AT(au.edp_raw_data_map,
                            'ExtraData_behavior') = 'BILLING'
            THEN 1 ELSE 0 END) "Price_c",
        MAX(CASE
            WHEN ELEMENT_AT(au.edp_raw_data_map,
                            'ExtraData_behavior') = 'DISCOVERY_QUESTION'
            THEN 1 ELSE 0 END) "Discovery_Questions_c",
        MAX(CASE
            WHEN ELEMENT_AT(au.edp_raw_data_map,
                            'ExtraData_behavior') = 'TRANSITION'
            THEN 1 ELSE 0 END) "Transition_Statement_c"
    FROM
        (
                SELECT ahe.edp_raw_data_map
                FROM hive.care.l1_asurion_home_events ahe
                WHERE ahe.edp_updated_date >= (SELECT StartDateStr FROM DateSelector)
                    AND  ELEMENT_AT(ahe.edp_raw_data_map, 'Name') = 'SalesChecklistBehaviorMentioned'
                    AND from_iso8601_timestamp(element_at(ahe.edp_raw_data_map,'Time')) 
                        BETWEEN (SELECT StartDate FROM DateSelector) 
                                AND (SELECT DATE_ADD('day',1, EndDate) FROM DateSelector)
            UNION ALL
                SELECT the.edp_raw_data_map
                FROM hive.care.l1_att_home_events the
                WHERE the.edp_updated_date >= (SELECT StartDateStr FROM DateSelector)
                    AND  ELEMENT_AT(the.edp_raw_data_map, 'Name') = 'SalesChecklistBehaviorMentioned'
                    AND from_iso8601_timestamp(element_at(the.edp_raw_data_map,'Time')) 
                        BETWEEN (SELECT StartDate FROM DateSelector) 
                                AND (SELECT DATE_ADD('day',1, EndDate) FROM DateSelector)
            UNION ALL
                SELECT vhe.edp_raw_data_map
                FROM hive.care.l1_verizon_home_events vhe
                WHERE vhe.edp_updated_date >= (SELECT StartDateStr FROM DateSelector)
                    AND  ELEMENT_AT(vhe.edp_raw_data_map, 'Name') = 'SalesChecklistBehaviorMentioned'
                    AND from_iso8601_timestamp(element_at(vhe.edp_raw_data_map,'Time')) 
                        BETWEEN (SELECT StartDate FROM DateSelector) 
                                AND (SELECT DATE_ADD('day',1, EndDate) FROM DateSelector)

        ) au
    WHERE 1 = 1
    GROUP BY
        COALESCE(
            element_at(au.edp_raw_data_map, 'Identities_ReservationSid'),
            element_at(au.edp_raw_data_map, 'Identities_SessionId')
            )
),

SmartOffer AS (
    SELECT
        sou.reservation_Id,
        SIGN(SUM(sou.SmartOfferModelMatch)) AS SmartOfferModelMatch,
        SIGN(SUM(sou.Ask_for_the_Sale_c)) AS Ask_for_the_Sale_c,
        SIGN(SUM(sou.Non_Partner_Coverage_c)) AS Non_Partner_Coverage_c,
        SIGN(SUM(sou.Engagement_Questions_c)) AS Engagement_Questions_c,
        SIGN(SUM(sou.Future_Purchase_Details_c)) AS Future_Purchase_Details_c,
        SIGN(SUM(sou.Celebrate_Enrollment_c)) AS Celebrate_Enrollment_c,
        SIGN(SUM(sou.Ask_to_Send_Enrollment_Link_c)) AS Ask_to_Send_Enrollment_Link_c,
        SIGN(SUM(sou.Examples_of_Coverage_c)) AS Examples_of_Coverage_c,
        SIGN(SUM(sou.Resolve_the_Customers_Concern_c)) AS Resolve_the_Customers_Concern_c,
        SIGN(SUM(sou.RRR_c)) AS RRR_c,
        SIGN(SUM(sou.Price_c)) AS Price_c,
        SIGN(SUM(sou.Discovery_Questions_c)) AS Discovery_Questions_c,
        SIGN(SUM(sou.Transition_Statement_c)) AS Transition_Statement_c
    FROM
        (
            SELECT * FROM SmartOfferExWo
            UNION ALL
            SELECT * FROM SmartOfferAlpha
        ) sou
    GROUP BY
        sou.reservation_Id
),

Sales AS (
    SELECT
        s.reservation_id,
        1 as "SalesFunnellMatch",
        MAX(COALESCE(s.Opportunity_CH,0)) AS "Opportunity_CH",
        MAX(COALESCE(s.Ai_Offer_CH,0)) AS "Ai_Offer_CH",
        MAX(COALESCE(s.Sale_CH,0)) AS "Sale_CH"
    FROM(
        SELECT
            sf.reservation_id,
            MAX(COALESCE(sf.opportunity_flg,0)) "Opportunity_CH",
            MAX(COALESCE(sf.ai_offer_count,0)) AS "Ai_Offer_CH",
            MAX(COALESCE(sf.enroll_count,0)) AS "Sale_CH"
        FROM 
            hive.care.l3_att_sales_funnel sf
        WHERE 1 =1
            AND sf.interaction_channel = 'voice'
            AND sf.business_unit IN ('soluto','mobility')
            AND sf.product_group = 'Connected Home'
            AND DATE(sf.event_date_cst) BETWEEN (SELECT StartDate FROM DateSelector) 
                AND  (SELECT EndDate from DateSelector)
        GROUP BY
            sf.reservation_id
        
        UNION ALL
        
        SELECT
            sf.reservation_id,
            MAX(COALESCE(sf.opportunity_flg,0)) "Opportunity_CH",
            MAX(COALESCE(sf.ai_offer_count,0)) AS "Ai_Offer_CH",
            MAX(COALESCE(sf.enroll_count,0)) AS "Sale_CH"
        FROM 
            care.l3_verizon_sales_funnel sf
        WHERE 1 =1
            AND sf.interaction_channel = 'voice'
            AND sf.business_unit IN ('soluto','mobility')
            AND sf.product_group = 'Connected Home'
            AND DATE(sf.event_date_cst) BETWEEN (SELECT StartDate FROM DateSelector) 
                AND  (SELECT EndDate from DateSelector)
        GROUP BY
            sf.reservation_id
    ) s
    GROUP BY
        s.reservation_Id
),


Inbound_Call_Stats AS(
    SELECT
        tid.reservation_id AS "reservation_id",
        MIN_BY(tgs.recording_link,tid.segstart) AS "URL",
        MIN_BY(rk.Language, tid.segstart) AS "Language",
        MIN_BY(rk.Business_Unit, tid.segstart) AS "Business_Unit",
        MIN_BY(rk.client, tid.segstart) AS "Client",
        MIN_BY(rk.subclient, tid.segstart) AS "Subclient",
        MIN(tid.segstart) as segstart,
        MIN(tid.event_date_cst) AS "Date",
        MIN_BY(tid.answering_id,tid.segstart) AS "Employee",
        1 AS "InboundCalls",
        0 AS "OutboundCalls",
        MAX(CASE WHEN rk.sales_enabled = 'true' THEN 1 ELSE 0 END) "SalesEnabled",
        SUM(
            (COALESCE(tid.interaction_tm_sec, 0)
            +COALESCE(tid.wrap_tm_sec, 0))
            ) AS "ResolutionTime",
        SUM(s.SalesFunnellMatch) AS "SalesFunnellMatch",
        sum(s.Opportunity_CH) as "Opportunity_CH",
        SUM(s.Ai_Offer_CH) AS "Ai_Offer_CH",

        SIGN(SUM(so.SmartOfferModelMatch)) AS SmartOfferModelMatch,
        SIGN(SUM(so.Ask_for_the_Sale_c)) AS Ask_for_the_Sale_c,
        SIGN(SUM(so.Non_Partner_Coverage_c)) AS Non_Partner_Coverage_c,
        SIGN(SUM(so.Engagement_Questions_c)) AS Engagement_Questions_c,
        SIGN(SUM(so.Future_Purchase_Details_c)) AS Future_Purchase_Details_c,
        SIGN(SUM(so.Celebrate_Enrollment_c)) AS Celebrate_Enrollment_c,
        SIGN(SUM(so.Ask_to_Send_Enrollment_Link_c)) AS Ask_to_Send_Enrollment_Link_c,
        SIGN(SUM(so.Examples_of_Coverage_c)) AS Examples_of_Coverage_c,
        SIGN(SUM(so.Resolve_the_Customers_Concern_c)) AS Resolve_the_Customers_Concern_c,
        SIGN(SUM(so.RRR_c)) AS RRR_c,
        SIGN(SUM(so.Price_c)) AS Price_c,
        SIGN(SUM(so.Discovery_Questions_c)) AS Discovery_Questions_c,
        SIGN(SUM(so.Transition_Statement_c)) AS Transition_Statement_c,
        SUM(s.Sale_Ch) AS "Sale_Ch"
    FROM
        hive.care.l3_asurion_twilio_interaction_detail tid
        LEFT OUTER JOIN
        hive.care.l4_asurion_umt_routing_key_analytics_mapper rk
            ON lower(tid.interaction_routing_key) = lower(rk.routingrulekey)
            AND tid.event_date_cst between rk.startdate and rk.enddate
        LEFT OUTER JOIN
        Sales s
            ON tid.reservation_id = s.reservation_id
        LEFT OUTER JOIN
        SmartOffer so
            ON tid.reservation_id = so.reservation_id
        LEFT JOIN 
        hive.care.l3_asurion_interaction_detail_tags tgs
            ON tid.reservation_id = tgs.reservation_id
    WHERE 1=1
        AND tid.direction = 'inbound'
        AND tid.disposition_action in (
            'handled','consult','conference','transfer',
            'flex_int_transfer_WARM',
            'flex_int_transfer_COLD')
        AND (
            rk.business_unit = 'Soluto'
            OR (
                rk.business_unit = 'Mobility'
                AND lower(rk.type) LIKE '%fl%'
                AND lower(rk.subclient) NOT LIKE '%mex%'
                )
            )
        AND rk.client in ('Verizon','AT&T')
        AND tid.event_date_cst BETWEEN (SELECT StartDate FROM DateSelector) 
            AND  (SELECT EndDate from DateSelector)
    GROUP BY
        tid.reservation_id
),

Outbound_Call_Stats AS(
    SELECT
        ocl.inbound_reservation_id AS "reservation_id",
        MIN_BY(tgs.recording_link, ocl.outbound_segment_start) AS "URL",
        
        MIN_BY(rk.Language, ocl.outbound_segment_start) AS "Language",
        MIN_BY(rk.Business_Unit, ocl.outbound_segment_start) AS "Business_Unit",
        MIN_BY(rk.client, ocl.outbound_segment_start) AS "Client",
        MIN_BY(rk.subclient, ocl.outbound_segment_start) AS "Subclient",
        MIN(ocl.outbound_segment_start) as segstart,
        MIN(ocl.event_date_cst) AS "Date",
        MIN_BY(ocl.originating_id, 
                    ocl.outbound_segment_start) AS "Employee",
        -- 0 AS ExWo,
        0 AS "InboundCalls",
        COUNT(*) AS "OutboundCalls",
        MAX(CASE WHEN rk.sales_enabled = 'true' THEN 1 ELSE 0 END) "SalesEnabled",
        SUM(
            ( COALESCE(ocl.interaction_time_outbound, 0) 
            + COALESCE(ocl.wrap_time_outbound, 0))
            + CASE WHEN ocl.disposition_action IN ('abandoned', 'resv_canceled')
                   THEN ocl.abn_time_outbound ELSE 0 END
            ) AS "ResolutionTime",
        SUM(s.SalesFunnellMatch) AS "SalesFunnellMatch",
        sum(s.Opportunity_CH) as "Opportunity_CH",
        SUM(s.Ai_Offer_CH) AS "Ai_Offer_CH",
        SIGN(SUM(so.SmartOfferModelMatch)) AS SmartOfferModelMatch,
        SIGN(SUM(so.Ask_for_the_Sale_c)) AS Ask_for_the_Sale_c,
        SIGN(SUM(so.Non_Partner_Coverage_c)) AS Non_Partner_Coverage_c,
        SIGN(SUM(so.Engagement_Questions_c)) AS Engagement_Questions_c,
        SIGN(SUM(so.Future_Purchase_Details_c)) AS Future_Purchase_Details_c,
        SIGN(SUM(so.Celebrate_Enrollment_c)) AS Celebrate_Enrollment_c,
        SIGN(SUM(so.Ask_to_Send_Enrollment_Link_c)) AS Ask_to_Send_Enrollment_Link_c,
        SIGN(SUM(so.Examples_of_Coverage_c)) AS Examples_of_Coverage_c,
        SIGN(SUM(so.Resolve_the_Customers_Concern_c)) AS Resolve_the_Customers_Concern_c,
        SIGN(SUM(so.RRR_c)) AS RRR_c,
        SIGN(SUM(so.Price_c)) AS Price_c,
        SIGN(SUM(so.Discovery_Questions_c)) AS Discovery_Questions_c,
        SIGN(SUM(so.Transition_Statement_c)) AS Transition_Statement_c,
        SUM(s.Sale_Ch) AS "Sale_Ch"
    FROM
        hive.care.l3_asurion_inbound_outbound_call_link_tbl ocl
        LEFT JOIN
        hive.care.l4_asurion_umt_routing_key_analytics_mapper rk
            ON ocl.inbound_interaction_routing_key = rk.routingrulekey
            AND ocl.event_date_cst between rk.startdate and rk.enddate
        LEFT OUTER JOIN
        Sales s
            ON ocl.outbound_reservation_id = s.reservation_id
        LEFT OUTER JOIN
        SmartOffer so 
            ON ocl.outbound_reservation_id = so.reservation_id
        LEFT JOIN 
        hive.care.l3_asurion_interaction_detail_tags tgs
            ON ocl.outbound_reservation_id = tgs.reservation_id
    WHERE
        1 = 1
        AND (
            rk.business_unit = 'Soluto'
            OR (
                rk.business_unit = 'Mobility'
                AND lower(rk.type) LIKE '%fl%'
                AND lower(rk.subclient) NOT LIKE '%mex%'
                )
            )
        AND rk.client in ('Verizon','AT&T')
        AND ocl.disposition_action NOT IN ('resv_time_out','resv_rejected')
        AND ocl.event_date_cst BETWEEN (SELECT StartDate FROM DateSelector) 
            AND  (SELECT EndDate from DateSelector)
    GROUP BY
        ocl.inbound_reservation_id
),

Call_Stats AS (
SELECT
    c.reservation_id,
    MIN_BY(c.URL, c.segstart) "URL",
    CAST(SUBSTRING(MIN_BY(c.Language, c.segstart),1,3) AS Char(3)) "Language_",
    CAST(SUBSTRING(MIN_BY(c.business_unit, c.segstart),1,1) AS Char(1)) "BusinessUnit_",
    CAST(SUBSTRING(MIN_BY(c.Client, c.segstart),1,1) AS Char(1)) "Client_",
    MIN_BY(c.SubClient, c.segstart) "Subclient",
    CONCAT(
        MIN_BY(c.Employee, c.segstart),
        '-',
        date_format(MIN(DATE(c.Date)),'%Y-%m-%d')
    ) "EmployeeKey",
    MIN(DATE(c.Date)) "Date",
    CAST(MIN_BY(c.Employee, c.segstart) AS INT) Employee,

    COALESCE(SUM(c.InboundCalls),0) "Inbound_Calls",
    COALESCE(SUM(c.OutboundCalls),0) "Outbound_Calls",

    SUM(COALESCE(c.SalesFunnellMatch, 0)) "Sales_Funnel_Matches",
    MAX(COALESCE(c.SalesEnabled, 0)) "SalesEnabled",
    SIGN(COALESCE(SUM(c.Opportunity_CH),0)) "Sales_Opportunity_Connected_Home",
    -- SIGN(COALESCE(SUM(c.Ai_Offer_CH), 0)) "Ai Sales Offer Connected Home",
    
    SIGN(COALESCE(SUM(c.SmartOfferModelMatch),0)) "Smart Offer Model Results Available",
    SIGN(COALESCE(SUM(c.Ask_for_the_Sale_c),0)) "Ask_for_the_Sale_c",
    SIGN(COALESCE(SUM(c.Non_Partner_Coverage_c),0)) "Non_Partner_Coverage_c",
    SIGN(COALESCE(SUM(c.Engagement_Questions_c),0)) "Engagement_Questions_c",
    SIGN(COALESCE(SUM(c.Future_Purchase_Details_c),0)) "Future_Purchase_Details_c",
    SIGN(COALESCE(SUM(c.Celebrate_Enrollment_c),0)) "Celebrate_Enrollment_c",
    SIGN(COALESCE(SUM(c.Ask_to_Send_Enrollment_Link_c),0)) "Ask_to_Send_Enrollment_Link_c",
    SIGN(COALESCE(SUM(c.Examples_of_Coverage_c),0)) "Examples_of_Coverage_c",
    SIGN(COALESCE(SUM(c.Resolve_the_Customers_Concern_c),0)) "Resolve_the_Customers_Concern_c",
    SIGN(COALESCE(SUM(c.RRR_c),0)) "RRR_c",
    SIGN(COALESCE(SUM(c.Price_c),0)) "Price_c",
    SIGN(COALESCE(SUM(c.Discovery_Questions_c),0)) "Discovery_Questions_c",
    SIGN(COALESCE(SUM(c.Transition_Statement_c),0)) "Transition_Statement_c",

     SIGN(COALESCE(SUM(c.Ask_for_the_Sale_c),0))
    +SIGN(COALESCE(SUM(c.Non_Partner_Coverage_c),0))
    +SIGN(COALESCE(SUM(c.Engagement_Questions_c),0))
    +SIGN(COALESCE(SUM(c.Future_Purchase_Details_c),0))
    +SIGN(COALESCE(SUM(c.Ask_to_Send_Enrollment_Link_c),0))
    +SIGN(COALESCE(SUM(c.Examples_of_Coverage_c),0))
    +SIGN(COALESCE(SUM(c.RRR_c),0))
    +SIGN(COALESCE(SUM(c.Price_c),0))
    +SIGN(COALESCE(SUM(c.Discovery_Questions_c),0))
    +SIGN(COALESCE(SUM(c.Transition_Statement_c),0)) "Total_Smart_Offer_Behaviors",

    SIGN(COALESCE(SUM(c.Sale_Ch),0)) AS "Sale_Connected_Home"
 
    -- SUM(CASE WHEN c.OutboundCalls = 0  
    --          THEN c.ResolutionTime ELSE 0 END) AS "Handle Time",
    -- SUM(c.ResolutionTime) AS "Resolution Time"
FROM
    (
        SELECT * FROM Inbound_Call_Stats i
        UNION ALL
        SELECT * FROM Outbound_Call_Stats o
    ) c
WHERE 1=1
GROUP BY
    c.reservation_id
HAVING
    SUM(c.ResolutionTime) <= 8 * 60 * 60
    AND SUM(c.InboundCalls) > 0 
)

SELECT cs.BusinessUnit_, cs.Client_, CAST(cs.Employee as VARCHAR) as expert_id, 
	sum(Total_Smart_Offer_Behaviors) as num, 
	sum(cs.Sales_Opportunity_Connected_Home)*10 as den, 
    ROUND(
    COALESCE(
    CAST(SUM(Total_Smart_Offer_Behaviors) AS DOUBLE) /
    NULLIF(CAST(SUM(cs.Sales_Opportunity_Connected_Home)*10 AS DOUBLE), 0.0),
    0.000
    ),
    3
    ) AS calc
FROM 
    Call_Stats cs
WHERE 
	cs."Client_" = :client
	AND
	cs."BusinessUnit_" = :business_unit
    AND
    CAST(cs.Employee as VARCHAR) IN :expert_ids
	    
    
group BY BusinessUnit_, Client_, Employee
    