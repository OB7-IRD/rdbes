SELECT
	pa.c_pays_fao::text AS vessel_flag_country_fao
	,EXTRACT(YEAR FROM c.d_act)::integer AS "CLyear"
	,EXTRACT(QUARTER FROM c.d_act)::integer AS "CLquar"
	,EXTRACT(MONTH FROM c.d_act)::integer AS "CLmonth"
	,a.v_la_act::numeric AS latitude_dec
	,a.v_lo_act::numeric AS longitude_dec
	,c.c_esp::integer AS specie_code
	,e.l_esp_s ::text AS specie_scientific_name
	,e.c_esp_fao::text AS "CLspecFAO"
	,tb.c_engin::integer AS vessel_type
	,p.c_locode::text AS "CLloc"
	,b.v_l_ht::numeric AS vessel_length
	,round(sum(c.v_poids_capt)::numeric, 0) AS "CLsciWeight"
FROM
	public.capture c
	JOIN public.activite a ON (c.c_bat = a.c_bat AND c.d_act = a.d_act AND c.n_act = a.n_act)
	JOIN public.bateau b ON (a.c_bat = b.c_bat)
	JOIN public.pavillon pa ON (b.c_flotte = pa.c_pav_b)
	JOIN public.type_bateau tb ON (b.c_typ_b = tb.c_typ_b)
	JOIN public.espece e ON (c.c_esp = e.c_esp)
	JOIN public.port p ON (a.c_port = p.c_port)
WHERE
	EXTRACT(YEAR FROM c.d_act) IN (?time_period)
	AND b.c_flotte IN (?countries)
	AND c.c_esp IN (?species)
GROUP BY
	vessel_flag_country_fao
	,"CLyear"
	,"CLquar"
	,"CLmonth"
	,latitude_dec
	,longitude_dec
	,specie_code
	,specie_scientific_name
	,"CLspecFAO"
	,vessel_type
	,"CLloc"
	,vessel_length
;
