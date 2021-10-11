SELECT
	part1.*
	,part2.number_of_well::integer AS "LEnumTotal"
	,COALESCE(part3.number_of_well_sampled, 0)::integer AS "LEnumSamp"
	,CASE
		WHEN part3.number_of_well_sampled >= 1 THEN 'Y'
		ELSE 'N'
	END::text AS "LEsamp" 
FROM (
WITH nb_activity_by_well AS (
	SELECT
		w.topiaid::text AS well_id
		,count(DISTINCT w2.activity)::integer AS nb_activity_by_well
	FROM
		public.well w 
		JOIN public.trip t ON (w.trip = t.topiaid)
		JOIN public.route r ON (t.topiaid = r.trip)
		JOIN public.vessel v ON (t.vessel = v.topiaid)
		JOIN public.country c ON (v.fleetcountry = c.topiaid)
		JOIN public.wellplan w2 ON (w.topiaid = w2.well)
	WHERE
		EXTRACT(YEAR FROM t.landingdate) = 2019
		AND c.code IN (1, 41)
	GROUP BY
		w.topiaid),
landing_harbour AS (
	SELECT DISTINCT
		t.topiaid::text AS trip_id
		,h.locode::text AS landing_harbour
	FROM 
		public.trip t
		JOIN public.route r ON (t.topiaid = r.trip)
		JOIN public.vessel v ON (t.vessel = v.topiaid)
		JOIN public.country c ON (v.fleetcountry = c.topiaid)
		JOIN public.harbour h ON (t.landingharbour = h.topiaid)
	WHERE
		EXTRACT(YEAR FROM t.landingdate) = 2019
		AND c.code IN (1, 41)),
landing_country AS (
	SELECT
		h.locode::text AS locode_harbour
		,c.codeiso2::text AS landing_country
	FROM
		public.harbour h
		JOIN public.country c ON (h.country = c.topiaid)),
dominante_catches_area AS (
	SELECT 
		*
	FROM (
		SELECT
			w.topiaid::text AS well_id
			,wp.activity::text AS activity_id
			,a.latitude::numeric AS latitude
			,a.longitude::numeric AS longitude
			,sum(wp.weight)::numeric AS weight
			,ROW_NUMBER () OVER (PARTITION BY w.topiaid ORDER BY sum(wp.weight) DESC)::integer AS rank_number
		FROM
			public.wellplan wp
			JOIN public.well w ON (wp.well = w.topiaid)
			JOIN public.trip t ON (t.topiaid = w.trip)
			JOIN public.route r ON (t.topiaid = r.trip)
			JOIN public.vessel v ON (t.vessel = v.topiaid)
			JOIN public.country c ON (v.fleetcountry = c.topiaid)
			JOIN public.activity a ON (wp.activity = a.topiaid)
		WHERE
			EXTRACT(YEAR FROM t.landingdate) = 2019
			AND c.code IN (1, 41)
		GROUP BY
			w.topiaid
			,wp.activity
			,a.latitude
			,a.longitude
		ORDER BY
			w.topiaid
			,sum(wp.weight) DESC) AS sub_query
	WHERE
		sub_query.rank_number = 1)
SELECT DISTINCT
	w.trip::TEXT AS trip_id
	,'LE'::text AS "LErecType"
	,v.topiaid::text AS "LEencrVessCode"
	,'N'::text AS "LEstratification"
	,1::integer AS "LEseqNum"
	,naw.nb_activity_by_well::integer AS "LEhaulNum"
	,'U'::text AS "LEstratumName"
	,'N'::text AS "LEclustering"
	,'U'::text AS "LEclusterName"
	,'Observer'::text AS "LEsampler"
	,'N'::text AS "LEmixedTrip"
	,'Lan'::text AS "LEcatReg"
	,lh.landing_harbour::text AS "LEloc"
	,'Port'::text AS "LElocType"
	,lc.landing_country::text AS "LEctry"
	,''::text AS "LEdate"
	,''::text AS "LEtime"
	,''::text AS "LEeconZoneIndi"
	,dca.latitude::numeric AS latitude
	,dca.longitude::numeric AS longitude
	,''::text AS "LEjurisdArea"
	,'NotApplicable'::text as "LEnatFishAct"
	,CASE
		WHEN v3.code = 2 THEN 'LHP_LPF'
		WHEN v3.code = 1 THEN 'PS_LPF'
		ELSE 'to_define'
	END::text AS "LEmetier5"
	,CASE
		WHEN v3.code = 2 THEN 'LHP_LPF_0_0_0'
		WHEN v3.code = 1 THEN 'PS_LPF_0_0_0'
		ELSE 'to_define'
	END::text AS "LEmetier6"
	,CASE
		WHEN v3.code = 2 THEN 'LHP'
		WHEN v3.code = 1 THEN 'PS'
		ELSE 'MIS'
	END::text AS "LEgear"
	,''::text AS "LEmeshSize"
	,0::integer AS "LEselDev"
	,''::text AS "LEselDevMeshSize"
	,'LPF'::text AS "LEtarget"
	,'None'::text AS "LEmitiDev"
	,''::text AS "LEgearDim"
	,''::text AS "LEselProb"
	,''::text AS "LEincProb"
	,'UPSWOR'::text AS "LEselectMeth"
	,concat('well_number_', w.wellnumber, '_well_position_', w.wellposition)::text AS "LEunitName"
	,''::text AS "LEselectMethCluster"
	,''::text AS "LEnumTotalClusters"
	,''::text AS "LEnumSampClusters"
	,''::text AS "LEselProbCluster"
	,''::text AS "LEincProbCluster"
	,''::text AS "LEnoSampReason"
	,''::text AS "LEfullTripAva"
FROM
	public.well w 
	JOIN public.trip t ON (w.trip = t.topiaid)
	JOIN public.route r ON (t.topiaid = r.trip)
	JOIN public.vessel v ON (t.vessel = v.topiaid)
	JOIN public.country c ON (v.fleetcountry = c.topiaid)
	JOIN nb_activity_by_well naw ON (w.topiaid = naw.well_id)
	JOIN landing_harbour lh ON (t.topiaid = lh.trip_id)
	JOIN landing_country lc ON (lc.locode_harbour = lh.landing_harbour)
	JOIN dominante_catches_area dca ON (w.topiaid = dca.well_id)
	JOIN public.vesseltype v2 ON (v.vesseltype = v2.topiaid)
	JOIN public.vesselsimpletype v3 ON (v2.vesselsimpletype = v3.topiaid)
WHERE
	EXTRACT(YEAR FROM t.landingdate) = 2019
	AND c.code IN (1, 41)) AS part1
JOIN (
	SELECT
		w.trip::TEXT AS trip_id	
		,count(w.topiaid)::integer AS number_of_well
	FROM
		public.well w
		JOIN public.trip t ON (t.topiaid = w.trip)
		JOIN public.vessel v ON (t.vessel = v.topiaid)
		JOIN public.country c ON (v.fleetcountry = c.topiaid)
	WHERE
		EXTRACT(YEAR FROM t.landingdate) = 2019
		AND c.code IN (1, 41)
	GROUP BY
		w.trip) part2 ON (part1.trip_id = part2.trip_id)
LEFT JOIN (
	SELECT
		s.trip::text AS trip_id
		,count(DISTINCT s.well)::integer AS number_of_well_sampled
	FROM
		public.sample s
		JOIN public.trip t ON (t.topiaid = s.trip)
		JOIN public.route r ON (t.topiaid = r.trip)
		JOIN public.vessel v ON (t.vessel = v.topiaid)
		JOIN public.country c ON (v.fleetcountry = c.topiaid)
	WHERE
		EXTRACT(YEAR FROM t.landingdate) = 2019
		AND c.code IN (1, 41)
	GROUP BY
		s.trip) part3 ON (part1.trip_id = part3.trip_id)
;
