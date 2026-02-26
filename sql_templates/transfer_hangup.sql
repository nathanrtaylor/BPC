with base as (
select
	a.event_date_cst,
	b.expert_id,
	b.client,
	b.business_unit,
	a.interaction_id,
	a.outdial_segstart,
	b.segstop,
	date_diff(
		'second',
		a.outdial_segstart,
		b.segstop
	) as outdial_duration_seconds,
	case
		when a.dialed_number in ('8662717730') then 1
		else 0
	end as transfer_hotline,
	a.dialed_number
from hive.care.l3_asurion_twilio_interaction_transfers a
inner join hive.care.l3_asurion_interaction_detail_tags b
	on a.reservation_id = b.reservation_id
where a.outdial_type in ('client')
	and b.client = :client
	and b.business_unit = :business_unit
	-- and b.subclient = 'Home Tech'
	and a.disposition in ('transfer')
	and a.event_date_cst between DATE :start_date and DATE :end_date 
)

select expert_id, client, business_unit, NULL as site, sum(outdial_duration_seconds) as num, 
count(dialed_number) as den,
ROUND(
    COALESCE(
    CAST(SUM(outdial_duration_seconds) AS DOUBLE) /
    NULLIF(CAST(count(dialed_number) AS DOUBLE), 0.0),
    0.000
    ),
    0
    ) AS calc
from base
group by expert_id, client, business_unit