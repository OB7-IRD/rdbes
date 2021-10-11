with nb_trip_by_harbour as (
		select h2.topiaid as harbour_id,h2.locode, count(distinct t.topiaid)::float as nb 
		from trip t 
		join harbour h2 on t.landingharbour = h2.topiaid 
		where EXTRACT(YEAR FROM t.landingdate) = 2019
		group by harbour_id,h2.locode
),
	nb_trip_sampled_by_harbour as (
		select h2.topiaid as harbour_id, h2.locode, count(distinct t2.topiaid)::float as nb
		from trip t2 
		join harbour h2 on t2.landingharbour = h2.topiaid 
		join sample s on s.trip = t2.topiaid 
		where EXTRACT(YEAR FROM t2.landingdate) = 2019
		group by harbour_id, h2.locode
)
SELECT
	'OS'::text as "OSrecType"
 	,row_number() OVER (ORDER BY t.landingdate)::integer as "OSseqNum"
	,c2.label1::text as "OSnatName"
	,'Y'::text as "OSstratification"
	,h2.locode::text as "OSlocode"
	,t.landingdate::date as "OSsamDate"
	,''::text as "OSsamTime"
	,concat(h2.locode, '-PORT')::text as "OSstratumName"
	,'N'::text as "OSclustering"
	,'U'::text as "OSclusterName"
	,'Observer'::text as "OSsampler"
 	,''::text as "OStimeUnit"
	,''::text as "OStimeValue"
	,ntbh.nb::integer as "OSnumTotal"
	,coalesce(ntsbh.nb::integer, 0) as "OSnumSamp"
	,coalesce(round((ntsbh.nb / ntbh.nb)::numeric, 5), 0)::numeric as "OSselProb"
	,''::text AS "OSincProb"
	,'CENSUS'::text as "OSselectMeth"
	,concat('2019-PORT-', h2.locode )::text::text as "OSunitName"
	,'Port'::text as "OSlocType" 
	,''::text as "OSselectMethCluster"
	,''::text as "OSnumTotalClusters"
	,''::text as "OSnumSampClusters"
	,''::text as "OSselProbCluster"
	,''::text as "OSincProbCluster"
	,CASE 
		when h2.topiaid in (select harbour_id from nb_trip_sampled_by_harbour) then 'Y'
		else 'N'
	END::text as "OSsamp"
	,''::text as "OSnoSampReason"
FROM trip t
	join harbour h2 on t.landingharbour = h2.topiaid 
	join country c2 on h2.country =c2.topiaid 
	join nb_trip_by_harbour ntbh on ntbh.harbour_id = h2.topiaid
	left join nb_trip_sampled_by_harbour ntsbh on ntsbh.harbour_id = h2.topiaid
WHERE EXTRACT(YEAR FROM t.landingdate)=2019
group by c2.label1, h2.locode, t.landingdate, ntbh.nb, ntsbh.nb, h2.topiaid 
;
