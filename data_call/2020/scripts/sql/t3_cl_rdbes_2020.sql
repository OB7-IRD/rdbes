WITH landing_harbour_country AS (
	SELECT DISTINCT
		t.topiaid::text AS trip_id
		,h.locode::text AS harbour_locode
		,c.codeiso2::text AS landing_country
		,c.codeiso3::text AS landing_country_fao
	FROM 
		public.trip t
		JOIN public.route r ON (t.topiaid =r.trip)
		JOIN public.harbour h ON (t.landingharbour = h.topiaid)
		JOIN public.country c ON (h.country = c.topiaid)
	WHERE
		EXTRACT(YEAR FROM r.date) = 2019),
	vessel_flag_country AS (
	SELECT DISTINCT
		t.topiaid::text AS trip_id
		,c.codeiso2::text AS vessel_flag_country
		,c.codeiso3::text AS vessel_flag_country_fao
	FROM 
		public.trip t
		JOIN public.route r ON (t.topiaid =r.trip)
		JOIN public.vessel v ON (t.vessel = v.topiaid)
		JOIN public.country c ON (v.fleetcountry = c.topiaid)
	WHERE
		EXTRACT(YEAR FROM r.date) = 2019)
SELECT
	'CL'::text AS "CLrecType"
	,'Estimate'::text AS "CLdTypSciWeig"
	,'Logb'::text AS "CLdSouSciWeig"
	,''::text AS "CLsampScheme"
	,'Other'::text AS "CLdSouLanVal"
	,lhc.landing_country::text AS "CLlanCou"
	,lhc.landing_country_fao::text AS landing_fao_country
	,vfc.vessel_flag_country::text AS "CLvesFlagCou"
	,vfc.vessel_flag_country_fao::text AS vessel_flag_country_fao
	,EXTRACT(YEAR FROM r.date)::integer AS "CLyear"
	,EXTRACT(QUARTER FROM r.date)::integer AS "CLquar"
	,EXTRACT(MONTH FROM r.date)::integer AS "CLmonth"
	,a.latitude::numeric AS latitude_dec
	,a.longitude::numeric AS longitude_dec
	,''::text AS "CLjurisdArea"
	,s.code::integer AS specie_code
	,s.scientificlabel::text AS specie_scientific_name
	,s.code3l::text AS "CLspecFAO"
	,'HuC'::text AS "CLlandCat"
	,'Lan'::text AS "CLcatchCat"
	,''::text AS "CLsizeCatScale"
	,''::text AS "CLsizeCat"
	,''::text AS "CLnatFishAct"
	,CASE
		WHEN vst.code = 1 THEN 'PS_LPF_0_0_0'
		WHEN vst.code = 2 THEN 'LHP_LPF_0_0_0'
		WHEN vst.code = 3 THEN 'LLD_LPF_0_0_0'
		ELSE 'to_define'
	END::text AS "CLmetier6"
	,'None'::text AS "CLIBmitiDev"
	,lhc.harbour_locode::text AS "CLloc"
	,CASE
		WHEN v.length < 8 THEN '<8'
		WHEN v.length >= 8 AND v.length < 10 THEN '8-<10'
		WHEN v.length >= 10 AND v.length < 12 THEN '10-<12'
		WHEN v.length >= 12 AND v.length < 15 THEN '12-<15'
		WHEN v.length >= 15 AND v.length < 18 THEN '15-<18'
		WHEN v.length >= 18 AND v.length < 24 THEN '18-<24'
		WHEN v.length >= 24 AND v.length < 40 THEN '24-<40'
		WHEN v.length >= 40 THEN '40<'
	END::text AS "CLvesLenCat"
	,CASE
		WHEN vst.code = 1 THEN 'PS'
		WHEN vst.code IN (2, 3) THEN 'HOK'
	END::text AS "CLfishTech"
	,'N'::text AS "CLdeepSeaReg"
	,ec.catchweight::numeric AS "CLoffWeight"
	,'NA'::text AS "CLtotOffLanVal"
	,''::text AS "CLsciLanRSE"
	,''::text AS "CLvalRSE"
	,''::text AS "CLsciLanQualBias"
	,v.code::integer AS vessel_id
FROM 
	public.elementarycatch ec
	JOIN public.activity a ON (ec.activity = a.topiaid)
	JOIN public.route r ON (a.route = r.topiaid)
	JOIN public.trip t ON (r.trip = t.topiaid)
	JOIN landing_harbour_country lhc ON (lhc.trip_id = t.topiaid)
	JOIN vessel_flag_country vfc ON (vfc.trip_id = t.topiaid)
	JOIN public.vessel v ON (v.topiaid = t.vessel)
	JOIN public.vesseltype vt ON (v.vesseltype = vt.topiaid)
	JOIN public.vesselsimpletype vst ON (vt.vesselsimpletype = vst.topiaid)
	JOIN public.country c ON (v.fleetcountry = c.topiaid)
	JOIN public.weightcategorylogbook wcl ON (wcl.topiaid = ec.weightcategorylogbook)
	JOIN public.species s ON (s.topiaid = wcl.species)
WHERE
	EXTRACT(YEAR FROM r.date) = 2019
	-- for the French fleet, 1 = France & 41 = Mayotte
	AND c.code IN (1, 41)
	-- 1 = YFT
	AND s.code = 1
;

