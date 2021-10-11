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
		EXTRACT(YEAR FROM r.date) IN (?time_period))
SELECT DISTINCT
	v.topiaid::text AS "VDencrVessCode"
	,EXTRACT(YEAR FROM r.date)::integer AS "VDyear"
  	,vfc.vessel_flag_country as "VDflgCtry"
	,v.length::integer as "VDlen"
 	,v.power::integer as "VDpwr"
 	,v.capacity::integer as "VDton"
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
	EXTRACT(YEAR FROM r.date) IN (?time_period)
	AND c.code IN (?countries)
	AND s.code IN (?species)
	AND v.power IS NOT NULL
;
