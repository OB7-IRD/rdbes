SELECT
	'FM'::text AS "FMrecType"
	, ef.v_long as "FMclass"
	, ef.v_eff as "FMnumAtUnit"
	, 'Fork Length'::text as "FMtype"
, 'Measured by hand with caliper'::text as "FMmeasEquip"
	--, ""::text as "FMaccuracy"
, 'Observer'::text as "FMsampler"
	--, ""::text as "FMaddGrpMeas"
	--, ""::text as "FMaddGrpMeasType"
FROM 
	echant e2
	JOIN bateau b ON (e2.c_bat = b.c_bat)
--	join port p_dbq on m2.c_port_dbq = p_dbq.c_port 
--	join port p_dep on m2.c_port_dep = p_dep.c_port 
	JOIN public.activite act ON (act.c_bat = e2.c_bat AND act.d_dbq = e2.d_dbq)
--	JOIN public.a_pays_d p ON (act.c_pays_d = p.c_pays_d) 
--	JOIN public.engin en ON (act.c_engin = en.c_engin)
--	JOIN public.ech_esp ee ON (e2.c_bat = ee.c_bat AND e2.d_dbq = ee.d_dbq AND ee.n_ech = e2.n_ech AND e2.id_cal = ee.id_cal)
	JOIN public.ech_freqt ef ON (e2.c_bat = ef.c_bat AND e2.d_dbq = ef.d_dbq AND ef.n_ech = e2.n_ech AND e2.id_cal = ef.id_cal)
	JOIN espece s ON (s.c_esp = ef.c_esp) 
WHERE
	EXTRACT(YEAR FROM act.d_act) IN (2019)
	-- for the French fleet, 1 = France & 41 = Mayotte
	-- AND b.c_pav_b IN (1, 41)
	AND b.c_typ_b IN (1,2,4,5,6,7)
	and ef.c_esp = 1
--group by m2.c_bat
--	,act.c_bat, act.d_dbq, act.d_act, act.n_act
--	, m2.f_cal_vid, p_dbq.c_locode, p_dbq.c_pays_fao, m2.d_dbq, en.l_engin, act.v_la_act, act.v_lo_act 
;