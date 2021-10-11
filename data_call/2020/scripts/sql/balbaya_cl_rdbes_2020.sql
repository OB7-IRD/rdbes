SELECT
	'CL'::text AS "CLrecType"
	,'Estimate'::text AS "CLdTypSciWeig"
	,'Logb'::text AS "CLdSouSciWeig"
	,''::text AS "CLsampScheme"
	,'Other'::text AS "CLdSouLanVal"
	,p.c_pays_fao::text AS landing_fao_country
	,pa.c_pays_fao::text AS vessel_flag_country_fao
	,EXTRACT(YEAR FROM c.d_act)::integer AS "CLyear"
	,EXTRACT(QUARTER FROM c.d_act)::integer AS "CLquar"
	,EXTRACT(MONTH FROM c.d_act)::integer AS "CLmonth"
	,a.v_la_act::numeric AS latitude_dec
	,a.v_lo_act::numeric AS longitude_dec
	,''::text AS "CLjurisdArea"
	,c.c_esp::integer AS specie_code
	,e.l_esp_s ::text AS specie_scientific_name
	,e.c_esp_fao::text AS "CLspecFAO"
	,'HuC'::text AS "CLlandCat"
	,'Lan'::text AS "CLcatchCat"
	,''::text AS "CLsizeCatScale"
	,''::text AS "CLsizeCat"
	,''::text AS "CLnatFishAct"
	,CASE
		WHEN tb.c_engin = 1 THEN 'PS_LPF_0_0_0'
		WHEN tb.c_engin = 2 THEN 'LHP_LPF_0_0_0'
		WHEN tb.c_engin = 3 THEN 'LLD_LPF_0_0_0'
		ELSE 'to_define'
	END::text AS "CLmetier6"
	,'None'::text AS "CLIBmitiDev"
	,p.c_locode::text AS "CLloc"
	,CASE
		WHEN b.v_l_ht < 8 THEN '<8'
		WHEN b.v_l_ht >= 8 AND b.v_l_ht < 10 THEN '8-<10'
		WHEN b.v_l_ht >= 10 AND b.v_l_ht < 12 THEN '10-<12'
		WHEN b.v_l_ht >= 12 AND b.v_l_ht < 15 THEN '12-<15'
		WHEN b.v_l_ht >= 15 AND b.v_l_ht < 18 THEN '15-<18'
		WHEN b.v_l_ht >= 18 AND b.v_l_ht < 24 THEN '18-<24'
		WHEN b.v_l_ht >= 24 AND b.v_l_ht < 40 THEN '24-<40'
		WHEN b.v_l_ht >= 40 THEN '40<'
	END::text AS "CLvesLenCat"
	,CASE
		WHEN tb.c_engin = 1 THEN 'PS'
		WHEN tb.c_engin IN (2, 3) THEN 'HOK'
	END::text AS "CLfishTech"
	,'N'::text AS "CLdeepSeaReg"
	,round(c.v_poids_capt::numeric, 0) AS "CLsciWeight"
FROM
	public.capture c
	JOIN public.activite a ON (c.c_bat = a.c_bat AND c.d_act = a.d_act AND c.n_act = a.n_act)
	JOIN public.bateau b ON (a.c_bat = b.c_bat)
	JOIN public.pavillon pa ON (b.c_flotte = pa.c_pav_b)
	JOIN public.type_bateau tb ON (b.c_typ_b = tb.c_typ_b)
	JOIN public.espece e ON (c.c_esp = e.c_esp)
	JOIN public.port p ON (a.c_port = p.c_port)
WHERE
	EXTRACT(YEAR FROM c.d_act) = 2019
	-- for the French fleet, 1 = France & 41 = Mayotte
	AND b.c_flotte IN (1, 41)
	-- 1 = YFT
	AND c.c_esp = 1
;
