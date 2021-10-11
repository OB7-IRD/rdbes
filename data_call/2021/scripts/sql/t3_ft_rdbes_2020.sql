WITH number_of_set AS (
	SELECT 
		t.topiaid::text AS trip_id
		,count(*)::integer AS number_of_set
	FROM
		public.trip t
		JOIN public.route r ON (t.topiaid = r.trip)
		JOIN public.vessel v ON (t.vessel = v.topiaid)
		JOIN public.country c ON (v.fleetcountry = c.topiaid)
		JOIN public.activity a ON (r.topiaid = a.route)
		JOIN public.vesselactivity v2 ON (a.vesselactivity = v2.topiaid)
	WHERE
		EXTRACT(YEAR FROM t.landingdate) = 2019
		AND c.code IN (1, 41)
		AND v2.code IN (0, 1, 2, 14, 100)
	GROUP BY
		t.topiaid),
landing_harbour AS (
	SELECT DISTINCT
		t.topiaid::text AS trip_id
		,h.locode::text AS landing_harbour
		,c.codeiso2::text AS landing_country
	FROM 
		public.trip t
		JOIN public.route r ON (t.topiaid = r.trip)
		JOIN public.vessel v ON (t.vessel = v.topiaid)
		JOIN public.country c ON (v.fleetcountry = c.topiaid)
		JOIN public.harbour h ON (t.landingharbour = h.topiaid)
	WHERE
		EXTRACT(YEAR FROM t.landingdate) = 2019
		AND c.code IN (1, 41)),
trips_sampled AS (
	SELECT DISTINCT
		t.topiaid::text AS trip_id
		,'Y'::text AS trip_sampled
	FROM
		public.sample s
		JOIN public.well w ON (s.well = w.topiaid)
		JOIN public.trip t ON (w.trip = t.topiaid)
		JOIN public.route r ON (t.topiaid = r.trip)
		JOIN public.vessel v ON (t.vessel = v.topiaid)
		JOIN public.country c ON (v.fleetcountry = c.topiaid)
	WHERE
		EXTRACT(YEAR FROM t.landingdate) = 2019
		AND c.code IN (1, 41))
SELECT
	'FT'::text AS "FTrecType"
	,v.topiaid::text AS "FTencrVessCode"
	,row_number() OVER (ORDER BY t.topiaid)::integer as "FTsequenceNumber"
	,'N'::text AS "FTstratification"
	,'U'::text AS "FTstratumName"
	,'N'::text AS "FTclustering"
	,'U'::text AS "FTclusterName"
	,'Observer'::text AS "FTsampler"
	,'On-Shore'::text AS "FTsampType"
	,COALESCE(nos.number_of_set, 0)::integer AS "FTfoNum"
	,h2.locode::text AS "FTdepLoc"
	,t.departuredate::date AS "FTdepDate"
	,''::text AS "FTdepTime"
	,lh.landing_harbour::text AS "FTarvLoc"
	,t.landingdate::date AS "FTarvDate"
	,''::text AS "FTarvTime"
	,''::text AS "FTnumTotal"
	,''::text AS "FTnumSamp"
	,''::text AS "FTselProb"
	,''::text AS "FTincProb"
	,'CENSUS'::text as "FTselectMeth"
	,concat(
		'FRA',
		LPAD(v.code::text, 4, '0'),
		extract(year from t.landingdate),
		LPAD(extract(month from t.landingdate)::text, 2, '0'),
		LPAD(extract(day from t.landingdate)::text, 2, '0'))::text as "FTunitName"
	,''::text AS "FTselectMethCluster"
	,''::text AS "FTnumTotalClusters"
	,''::text AS "FTnumSampClusters"
	,''::text AS "FTselProbCluster"
	,''::text AS "FTincProbCluster"
	,CASE
		WHEN ts.trip_sampled = 'Y' THEN 'Y'
		ELSE 'N'
	END::text AS "FTsamp"
	,CASE
		WHEN ts.trip_sampled = 'Y' THEN ''
		ELSE 'Other'
	END::text AS "FTnoSampReason"
FROM
	public.trip t
	JOIN public.vessel v ON (t.vessel = v.topiaid)
	JOIN public.vesseltype v3 ON (v.vesseltype = v3.topiaid)
	JOIN public.vesselsimpletype v4 ON (v3.vesselsimpletype = v4.topiaid)
	JOIN public.country c ON (v.fleetcountry = c.topiaid)
	LEFT JOIN number_of_set nos ON (t.topiaid = nos.trip_id)
	JOIN harbour h2 ON (t.departureharbour = h2.topiaid)
	JOIN landing_harbour lh ON (t.topiaid = lh.trip_id)
	LEFT JOIN trips_sampled ts ON (t.topiaid = ts.trip_id)
WHERE
	EXTRACT(YEAR FROM t.landingdate) = 2019
	-- for the French fleet, 1 = France & 41 = Mayotte
	AND c.code IN (1, 41)
	-- add canneur after correction in Kw in RDBES vessel ref
	AND v4.code IN (1)
	-- remove these rules after modalities addition in the RDBES database
	AND lh.landing_harbour NOT IN ('ESZJF', 'ESRBI')
	AND h2.locode NOT IN ('ESZJF', 'ESRBI')
;
