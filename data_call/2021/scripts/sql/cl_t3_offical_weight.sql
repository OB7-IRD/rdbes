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
		EXTRACT(YEAR FROM r.date) IN (?time_period)),
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
		EXTRACT(YEAR FROM r.date) IN (?time_period))
SELECT
	lhc.landing_country::text AS "CLlanCou"
	,vfc.vessel_flag_country::text AS "CLvesFlagCou"
	,EXTRACT(YEAR FROM r.date)::integer AS "CLyear"
	,EXTRACT(QUARTER FROM r.date)::integer AS "CLquar"
	,EXTRACT(MONTH FROM r.date)::integer AS "CLmonth"
	,a.latitude::numeric AS latitude_dec
	,a.longitude::numeric AS longitude_dec
	,s.code::integer AS specie_code
	,s.scientificlabel::text AS specie_scientific_name
	,s.code3l::text AS "CLspecFAO"
	,lhc.harbour_locode::text AS "CLloc"
	,v.length::numeric AS vessel_length
	,vst.code::integer AS vessel_type
	,v.code::integer AS vessel_id
	,sum(ec.catchweight)::numeric AS "CLoffWeight"
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
	EXTRACT(YEAR FROM r.date) IN (?time_period)
	AND c.code IN (?countries)
	AND s.code IN (?species)
GROUP BY
	"CLlanCou"
	,"CLvesFlagCou"
	,"CLyear"
	,"CLquar"
	,"CLmonth"
	,latitude_dec
	,longitude_dec
	,specie_code
	,specie_scientific_name
	,"CLspecFAO"
	,"CLloc"
	,vessel_length
	,vessel_type
	,vessel_id
;
