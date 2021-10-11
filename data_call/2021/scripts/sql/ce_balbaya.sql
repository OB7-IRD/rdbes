SELECT
	EXTRACT(YEAR FROM act.d_act)::integer AS "CEyear"
	,EXTRACT(QUARTER FROM act.d_act)::integer AS "CEquar"
	,EXTRACT(MONTH FROM act.d_act)::integer AS "CEMonth"
	,act.v_la_act::numeric AS latitude_dec
	,act.v_lo_act::numeric AS longitude_dec
	,z.c_dfao::TEXT AS db_division_fao
	,m.c_bat::integer AS vessel_id
	,b.v_l_ht::NUMERIC AS vessel_length
	,b.c_typ_b::integer AS vessel_type
	,b.v_p_cv::NUMERIC AS vessel_engine_power
	,b.v_ct_m3::NUMERIC AS vessel_volume
	,port.c_locode AS "CEloc"
	,act.c_ocea::integer AS ocean
	,act.v_tmer::NUMERIC AS hours_at_sea
	,act.v_tpec::NUMERIC AS fishing_time
FROM 
	maree m
	JOIN bateau b ON (m.c_bat=b.c_bat)
	JOIN public.activite act ON (act.c_bat=m.c_bat AND act.d_dbq=m.d_dbq)
	JOIN public.zfao zf USING (id_zfao)
	JOIN public.a_pays_d p ON (act.c_pays_d=p.c_pays_d) 
	JOIN public.engin en ON (act.c_engin=en.c_engin)
	JOIN public.port port ON (port.c_port=m.c_port_dbq)
	JOIN public.zfao z ON (z.id_zfao = act.id_zfao)
WHERE
	EXTRACT(YEAR FROM act.d_act) IN (?time_period)
	AND b.c_pav_b IN (?countries)
	AND en.c_engin IN (?gears)
;
