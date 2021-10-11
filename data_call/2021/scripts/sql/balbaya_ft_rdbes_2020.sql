SELECT
	'FT'::text AS "FTrecType"
	, b.c_bat as "internal_vessel_id"
	, 0::integer as "FTseqNum"
	, 'Y/N'::text as "FTstratification"
	, ''::text as "FTstratumName"
	, ''::text as "FTclustering"
	, ''::text as "FTclusterName"
	, 'Observer'::text as "FTsampler"
	, 'On-Shore'::text as "FTsampType"
	, count(act.n_act) as "FTfoNum"
	, p_dbq.c_locode::text as "FTdepLoc"
	, m2.d_depart::date as "FTdepDate"
	--, ''::text as "FTdepTime"
	, p_dep.c_locode::text as "FTarvLoc"
	, m2.d_dbq::date as "FTarvDate"
	-- , ''::text as "FTarvTime"
	, ''::text as "FTnumTotal"
	, ''::text as "FTnumSamp"
--	, ''::text as "FTselProb"
--	, ''::text as "FTincProb"
	, 'CENSUS'::text as "FTselectMeth"
	, concat('FRA', LPAD(m2.c_bat::text, 4, '0')::text , extract(year from m2.d_dbq)::text,  LPAD(extract(month from m2.d_dbq)::text, 2, '0'), LPAD(extract(day from m2.d_dbq)::text, 2, '0'))::text as "FTunitName"
	, ''::text as "FTselectMethCluster"
	, ''::text as "FTnumTotalClusters"
	, ''::text as "FTnumSampClusters"
	, ''::text as "FTselProbCluster"
	, ''::text as "FTincProbCluster"
	, CASE
		WHEN sum(COALESCE(e2.n_ech, 0) ) <= 0 THEN 'N'
		WHEN sum(COALESCE(e2.n_ech, 0) ) > 0 THEN 'Y'
	END::text AS "FTsamp"
--	, ''::text as "FTnoSampReason"	
FROM 
	maree m2
	JOIN bateau b ON (m2.c_bat = b.c_bat)
	join port p_dbq on m2.c_port_dbq = p_dbq.c_port 
	join port p_dep on m2.c_port_dep = p_dep.c_port 
	JOIN public.activite act ON (act.c_bat = m2.c_bat AND act.d_dbq = m2.d_dbq  and act.c_opera = 1)
	JOIN public.a_pays_d p ON (act.c_pays_d = p.c_pays_d) 
	JOIN public.engin en ON (act.c_engin = en.c_engin)
	left outer join public.echant e2 on (e2.c_bat = m2.c_bat and e2.d_dbq = m2.d_dbq) 
--	JOIN public.ech_esp ee ON (e2.c_bat = ee.c_bat AND e2.d_dbq = ee.d_dbq AND ee.n_ech = e2.n_ech AND e2.id_cal = ee.id_cal)
--	JOIN espece s ON (s.c_esp = ee.c_esp) 
WHERE
	EXTRACT(YEAR FROM act.d_act) IN (2019)
	-- for the French fleet, 1 = France & 41 = Mayotte
	-- AND b.c_pav_b IN (1, 41)
	AND b.c_typ_b IN (1,2,4,5,6,7)
--	and ee.c_esp = 1
group by b.c_bat, p_dbq.c_locode, m2.d_depart, p_dep.c_locode, m2.d_dbq, m2.c_bat 
;