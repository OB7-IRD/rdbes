SELECT
	'LE'::text AS "LErecType"
	, m2.c_bat as "internal_vessel_id"
	, 'Y/N'::text as "LEstratification"
	, 0::integer as "LEseqNum"
--	, concat(act.c_bat, act.d_dbq, act.d_act, act.n_act)::text as "act_id_concat"
	, count(distinct concat(act.c_bat, act.d_dbq, act.d_act, act.n_act))::integer as "LEhaulNum"
-- , as "LEstratumName"
-- , as "LEclustering"
-- , as "LEclusterName"
 	, 'Observer'::text as "LEsampler"
	, CASE
		WHEN m2.f_cal_vid = 0 THEN 'Y'
		when m2.f_cal_vid = 1  THEN 'N'
	END::text AS "LEmixedTrip"
 	, 'Lan' as "LEcatReg"
	, p_dbq.c_locode as "LEloc"
	, 'Port' as "LElocType"
 	, p_dbq.c_pays_fao as "LEctry"
	, m2.d_dbq::date as "LEdate"
-- , as "LEtime"
    , 'RFMO' as "LEeconZoneIndi"
--    ,act.v_la_act::numeric AS latitude_dec
--	,act.v_lo_act::numeric AS longitude_dec
-- , as "LEarea"
-- , as "LEstatRect"
-- , as "LEgsaSubarea"
-- , as "LEjurisdArea"
 , 'NotApplicable' as "LEnatFishAct"
	,CASE
		WHEN en.l_engin = 'Canneur' THEN 'LHP_LPF'
		WHEN en.l_engin = 'Senneur' THEN 'PS_LPF'
		ELSE 'to_define'
	END::text AS "LEmetier5"
	,CASE
		WHEN en.l_engin = 'Canneur' THEN 'LHP_LPF_0_0_0'
		WHEN en.l_engin = 'Senneur' THEN 'PS_LPF_0_0_0'
		ELSE 'to_define'
	END::text AS "LEmetier6"
	,CASE
		WHEN en.l_engin = 'Canneur' THEN 'LHP'
		WHEN en.l_engin = 'Senneur' THEN 'PS'
		ELSE 'MIS'
	END::text AS "LEgear"
-- , as "LEmeshSize"
-- , as "LEselDev"
-- , as "LEselDevMeshSize"
	, 'LPF' as "LEtarget"
	, 'None' as "LEmitiDev"
-- , as "LEgearDim"
-- , as "LEnumTotal"
-- , as "LEnumSamp"
--    , count(distinct concat(e2.c_bat, e2.d_dbq, e2.n_ech, e2.id_cal))::integer as "LEnumSamp"
-- , as "LEselProb"
-- , as "LEincProb"
    , 'CENSUS' as "LEselectMeth"
    , concat('FRA', LPAD(m2.c_bat::text, 4, '0')::text , extract(year from m2.d_dbq)::text,  LPAD(extract(month from m2.d_dbq)::text, 2, '0'), LPAD(extract(day from m2.d_dbq)::text, 2, '0'))::text as "LEunitName"
-- , as "LEselectMethCluster"
-- , as "LEnumTotalClusters"
-- , as "LEnumSampClusters"
-- , as "LEselProbCluster"
-- , as "LEincProbCluster"
	, CASE
		WHEN sum(COALESCE(e2.n_ech, 0) ) <= 0 THEN 'N'
		WHEN sum(COALESCE(e2.n_ech, 0) ) > 0 THEN 'Y'
	END::text AS "LEsamp"
-- , as "LEnoSampReason"
-- , as "LEfullTripAva"
--	, ''::text as "FTstratumName"
--	, ''::text as "FTclustering"
--	, ''::text as "FTclusterName"
--	, 'Observer'::text as "FTsampler"
--	, 'On-Shore'::text as "FTsampType"
--	, 0::integer as "FTfoNum"
--	, p_dbq.c_locode::text as "FTdepLoc"
--	, m2.d_depart::date as "FTdepDate"
--	--, ''::text as "FTdepTime"
--	, p_dep.c_locode::text as "FTarvLoc"
--	, m2.d_dbq::date as "FTarvDate"
--	-- , ''::text as "FTarvTime"
--	, ''::text as "FTnumTotal"
--	, ''::text as "FTnumSamp"
----	, ''::text as "FTselProb"
----	, ''::text as "FTincProb"
--	, 'CENSUS'::text as "FTselectMeth"
--	, concat('FRA', LPAD(m2.c_bat::text, 4, '0')::text , extract(year from m2.d_dbq)::text,  LPAD(extract(month from m2.d_dbq)::text, 2, '0'), LPAD(extract(day from m2.d_dbq)::text, 2, '0'))::text as "FTunitName"
--	, ''::text as "FTselectMethCluster"
--	, ''::text as "FTnumTotalClusters"
--	, ''::text as "FTnumSampClusters"
--	, ''::text as "FTselProbCluster"
--	, ''::text as "FTincProbCluster"
--	, CASE
--		WHEN sum(COALESCE(e2.n_ech, 0) ) <= 0 THEN 'N'
--		WHEN sum(COALESCE(e2.n_ech, 0) ) > 0 THEN 'Y'
--	END::text AS "FTsamp"
--	, ''::text as "FTnoSampReason"	
FROM 
	maree m2
	JOIN bateau b ON (m2.c_bat = b.c_bat)
	join port p_dbq on m2.c_port_dbq = p_dbq.c_port 
--	join port p_dep on m2.c_port_dep = p_dep.c_port 
	JOIN public.activite act ON (act.c_bat = m2.c_bat AND act.d_dbq = m2.d_dbq and act.c_opera = 1)
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
group by m2.c_bat
--	,act.c_bat, act.d_dbq, act.d_act, act.n_act
	, m2.f_cal_vid, p_dbq.c_locode, p_dbq.c_pays_fao, m2.d_dbq, en.l_engin
--	, act.v_la_act, act.v_lo_act 
;