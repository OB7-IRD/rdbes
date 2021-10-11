SELECT
	'CE'::text AS "CErecType"
	,'Estimate'::text AS "CEdTypSciEff"
	,'Logb'::text AS "CEdSouSciEff"
	,''::text AS "CEnatProgSciEff"
	,'FR'::text AS "CEvesFlagCou"
	,EXTRACT(YEAR FROM act.d_act)::integer AS "CEyear"
	,EXTRACT(QUARTER FROM act.d_act)::integer AS "CEquar"
	,EXTRACT(MONTH FROM act.d_act)::integer AS "CEMonth"
	,act.v_la_act::numeric AS latitude_dec
	,act.v_lo_act::numeric AS longitude_dec
	,''::text AS "CEjurisdArea"
	,''::text AS "CEnatFishAct"
	,CASE
		WHEN en.l_engin = 'Canneur' THEN 'LHP_LPF_0_0_0'
		WHEN en.l_engin = 'Senneur' THEN 'PS_LPF_0_0_0'
		ELSE 'to_define'
	END::text AS "CEmetier6"
	,'None'::text AS "CEIBmitiDev"
	,port.c_locode AS "CEloc"
	,CASE
		WHEN b.v_l_ht < 8 THEN '<8'
		WHEN b.v_l_ht >= 8 AND b.v_l_ht < 10 THEN '8-<10'
		WHEN b.v_l_ht >= 10 AND b.v_l_ht < 12 THEN '10-<12'
		WHEN b.v_l_ht >= 12 AND b.v_l_ht < 15 THEN '12-<15'
		WHEN b.v_l_ht >= 15 AND b.v_l_ht < 18 THEN '15-<18'
		WHEN b.v_l_ht >= 18 AND b.v_l_ht < 24 THEN '18-<24'
		WHEN b.v_l_ht >= 24 AND b.v_l_ht < 40 THEN '24-<40'
		WHEN b.v_l_ht >= 40 THEN '40<'
	END::text AS "CEvesLenCat"
	,CASE
		WHEN b.c_typ_b IN (4, 5, 6) THEN 'PS'
		WHEN b.c_typ_b IN (1, 2) THEN 'HOK'
	END::text AS "CEfishTech"
	,'N'::text AS "CEdeepSeaReg"
	-- days at sea
	,(act.v_tmer / 24)::numeric AS "CEoffDaySea"
	,(act.v_tmer / 24)::numeric as "CESciDaySea"
	-- fishing days
	,CASE
		WHEN act.c_ocea = 1 THEN (act.v_tpec / 12)
		WHEN act.c_ocea = 2 THEN (act.v_tpec / 13)
	END::numeric AS "CEoffFishDay"
	,CASE
		WHEN act.c_ocea = 1 THEN (act.v_tpec / 12)
		WHEN act.c_ocea = 2 THEN (act.v_tpec / 13)
	END::numeric AS "CEsciFishDay"
	-- fishing effort in kW-days = days at sea * engine power * 0.735499
	-- 1 ch = 0.73539875 kW
	,(act.v_tmer / 24) * b.v_p_cv * 0.735499::numeric AS "CEoffkWDaySea"
	,(act.v_tmer / 24) * b.v_p_cv * 0.735499::numeric AS "CEscikWDaySea"
	-- fishing effort in kW-days =  fishing days * engine power * 0.735499
	,CASE
		WHEN act.c_ocea = 1 THEN ((act.v_tpec / 12) * b.v_p_cv * 0.735499)
		WHEN act.c_ocea = 2 THEN ((act.v_tpec / 13) * b.v_p_cv * 0.735499)
	END::numeric AS "CEoffkWFishDay"
	,CASE
		WHEN act.c_ocea = 1 THEN (act.v_tpec / 12) * b.v_p_cv * 0.735499
		WHEN act.c_ocea = 2 THEN (act.v_tpec / 13) * b.v_p_cv * 0.735499
	END::numeric AS "CEscikWFishDay"
	-- fishing effort in kW by fishing hours
	,CASE
		WHEN act.c_ocea = 1 THEN ((act.v_tpec / 12) / 24 )* b.v_p_cv * 0.735499
		WHEN act.c_ocea = 2 THEN ((act.v_tpec / 13) / 24 )* b.v_p_cv * 0.735499
	END::numeric AS "CEoffkWFishHour"
	,CASE
		WHEN act.c_ocea = 1 THEN (((act.v_tpec / 12) / 24) * b.v_p_cv * 0.735499)
		WHEN act.c_ocea = 2 THEN (((act.v_tpec / 13) / 24) * b.v_p_cv * 0.735499)
	END::numeric AS "CEscikWFishHour"
	-- fishing effort in GT * days at sea
	,(act.v_tmer / 24) * ((0.2 + 0.02 * log(b.v_ct_m3)) * b.v_ct_m3)::numeric AS "CEgTDaySea"
	-- fishing effort in GT * fishing days
	,CASE
		WHEN act.c_ocea = 1 THEN ((act.v_tpec / 12) * ((0.2 + 0.02 * log(b.v_ct_m3)) * b.v_ct_m3))
		WHEN act.c_ocea = 2 THEN ((act.v_tpec / 13) * ((0.2 + 0.02 * log(b.v_ct_m3)) * b.v_ct_m3))
	END::numeric AS "CEgTFishDay"
	-- fishing effort in GT * fishing hours
	,CASE
		WHEN act.c_ocea = 1 THEN (((act.v_tpec / 12) / 24) * ((0.2 + 0.02 * log(b.v_ct_m3)) * b.v_ct_m3))
		WHEN act.c_ocea = 2 THEN (((act.v_tpec / 13) / 24) * ((0.2 + 0.02 * log(b.v_ct_m3)) * b.v_ct_m3))
	END::numeric AS "CEgTFishHour"
	,m.c_bat::integer AS vessel_id
FROM 
	maree m
	JOIN bateau b ON (m.c_bat=b.c_bat)
	JOIN public.activite act ON (act.c_bat=m.c_bat AND act.d_dbq=m.d_dbq)
	JOIN public.zfao zf USING (id_zfao)
	JOIN public.a_pays_d p ON (act.c_pays_d=p.c_pays_d) 
	JOIN public.engin en ON (act.c_engin=en.c_engin)
	JOIN public.port port ON (port.c_port=m.c_port_dbq)
WHERE
	EXTRACT(YEAR FROM act.d_act) IN (2019)
	-- for the French fleet, 1 = France & 41 = Mayotte
	AND b.c_pav_b IN (1, 41)
	-- 1 = PS, 2 = BB and 3 = LL
	AND en.c_engin IN (1, 2, 3)
;
