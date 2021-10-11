WITH vessel_flag_country AS (
	SELECT DISTINCT
		t.topiaid::text AS trip_id
		,c.codeiso2::text AS vessel_flag_country
	FROM 
		public.trip t
		JOIN public.route r ON (t.topiaid =r.trip)
		JOIN public.vessel v ON (t.vessel = v.topiaid)
		JOIN public.country c ON (v.fleetcountry = c.topiaid)
	WHERE
		EXTRACT(YEAR FROM r.date) = 2019)
SELECT DISTINCT
	'VD'::TEXT AS "VDrecType"
	,v.topiaid::text AS "VDencrVessCode"
	,EXTRACT(YEAR FROM r.date)::integer AS "VDyear"
	,'FR' as "VDctry"
	,''::text AS "VDhomePort"
  	,vfc.vessel_flag_country as "VDflgCtry"
	,v.length::integer as "VDlen"
	,CASE
		WHEN v.length < 8 THEN '<8'
		WHEN v.length >= 8 AND v.length < 10 THEN '8-<10'
		WHEN v.length >= 10 AND v.length < 12 THEN '10-<12'
		WHEN v.length >= 12 AND v.length < 15 THEN '12-<15'
		WHEN v.length >= 15 AND v.length < 18 THEN '15-<18'
		WHEN v.length >= 18 AND v.length < 24 THEN '18-<24'
		WHEN v.length >= 24 AND v.length < 40 THEN '24-<40'
		WHEN v.length >= 40 THEN '40<'
	END::text AS "VDlenCat"
 	,v.power::integer as "VDpwr"
 	,v.capacity::integer as "VDton"
 	,'GT'::text as "VDtonUnit"
FROM elementarycatch ec 
	JOIN public.activity a ON (ec.activity = a.topiaid)
	JOIN public.route r ON (a.route = r.topiaid)
	JOIN public.trip t ON (r.trip = t.topiaid)
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
	AND v.power IS NOT NULL
;
