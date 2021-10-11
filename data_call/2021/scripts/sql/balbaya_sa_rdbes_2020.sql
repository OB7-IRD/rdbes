SELECT
	'SA'::text AS "SArecType"
	, e2.d_dbq::date
	, e2.n_ech::numeric
	, e2.id_cal::numeric
	,act.v_la_act::numeric AS latitude_dec
	,act.v_lo_act::numeric AS longitude_dec
	,s.c_esp::integer AS specie_code
	,s.l_esp_s::text AS specie_scientific_name
	,s.c_esp_3l::text AS "SAspeCodeFAO"
	,'Salted '::text as "SApres"
    ,'Dead'::text as "SAspecState"
	,'Lan'::text as "SAcatchCat"
	,'HuC'::TEXT AS "SAlandCat"
	,''::text as "SAcommCatScl"
	,''::text as "SAcommCat"
	,'U'::text as "SAsex"
	,'RFMO'::text as "SAeconZoneIndi"
	,CASE
		WHEN en.l_engin = 'Canneur' THEN 'LHP_LPF'
		WHEN en.l_engin = 'Senneur' THEN 'PS_LPF'
		ELSE 'to_define'
	END::text AS "SAmetier5"
	,CASE
		WHEN en.l_engin = 'Canneur' THEN 'LHP_LPF_0_0_0'
		WHEN en.l_engin = 'Senneur' THEN 'PS_LPF_0_0_0'
		ELSE 'to_define'
	END::text AS "SAmetier6"
	,CASE
		WHEN en.l_engin = 'Canneur' THEN 'LHP'
		WHEN en.l_engin = 'Senneur' THEN 'PS'
		ELSE 'to_define'
	END::text AS "SAgear"
	,0::integer AS "SAselDev"
	,'Haul'::text as "SAunitType"
-- , "SAtotalWtLive"
-- , "SAsampWtLive"
-- , "SAnumTotal"
-- , "SAnumSamp"
-- , "SAselProb"
-- , "SAincProb"
	,'SRSWOR'::text as "SAselectMeth"
--	 , concat('FRA', 
--	LPAD(e2.c_bat::text, 4, '0')::text , 
-- 	extract(year from e2.d_dbq)::text,  
-- 	LPAD(extract(month from e2.d_dbq)::text, 2, '0'), 
-- 	LPAD(extract(day from e2.d_dbq)::text, 2, '0'),
--  	extract(year from e2.d_act)::text,  
-- 	LPAD(extract(month from e2.d_act)::text, 2, '0'), 
-- 	LPAD(extract(day from e2.d_act)::text, 2, '0'),
-- 	LPAD(e2.n_ech::text, 3, '0')::text,
-- 	LPAD(e2.id_cal::text, 3, '0')::text
-- 	)
-- ::text as "SAunitName"
-- , "SAunitName" (Stratum code DCF OA1/OI1)
	,'B'::text as "SAlowHierarchy"
 , 'Observer'::text as "SAsampler"
	, 'Y'::text as "SAsamp"
-- , "SAnoSampReasonFM"
-- , "SAnoSampReasonBV"
   , ee.v_p_esp_ech
   , ee.v_p_esp_str 
   , e2.v_la_act 
   , e2.v_lo_act 
-- , "SAtotalWtMes"
-- , "SAsampWtMes"
-- , "SAconFacMesLive"
FROM 
	echant e2 
	JOIN bateau b ON (e2.c_bat = b.c_bat)
	JOIN public.activite act ON (act.c_bat = e2.c_bat AND act.d_dbq = e2.d_dbq)
	JOIN public.a_pays_d p ON (act.c_pays_d = p.c_pays_d) 
	JOIN public.engin en ON (act.c_engin = en.c_engin)
	JOIN public.ech_esp ee ON (e2.c_bat = ee.c_bat AND e2.d_dbq = ee.d_dbq AND ee.n_ech = e2.n_ech AND e2.id_cal = ee.id_cal)
	JOIN espece s ON (s.c_esp = ee.c_esp) 
WHERE
	EXTRACT(YEAR FROM act.d_act) IN (2019)
	-- for the French fleet, 1 = France & 41 = Mayotte
	AND b.c_pav_b IN (1, 41)
	AND b.c_typ_b IN (1,2,4,5,6,7)
	and ee.c_esp = 1
;