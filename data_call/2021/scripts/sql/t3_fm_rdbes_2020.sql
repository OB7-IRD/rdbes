SELECT
	'FM'::text AS "FMrecType"
	,ssf.lengthclass as "FMclass"
	,ssf."number" as "FMnumAtUnit"
	,'Fork Length'::text as "FMtype"
	,'Measured by hand with caliper'::text as "FMmeasEquip"
	,'cm'::text as "FMaccuracy"
	,'Observer'::text as "FMsampler" --
	,''::text as "FMaddGrpMeas"
	,''::text as "FMaddGrpMeasType"
FROM 
	sample s
	join trip t on (t.topiaid = s.trip)
	JOIN vessel v ON (t.vessel = v.topiaid)
	JOIN public.vesseltype vt ON (v.vesseltype = vt.topiaid)
	JOIN public.vesselsimpletype vst ON (vt.vesselsimpletype = vst.topiaid)
	JOIN public.country c ON (v.fleetcountry = c.topiaid)
	join samplespecies ss on ss.sample = s.topiaid 
	join species spec on spec.topiaid = ss.species 
	join samplespeciesfrequency ssf on ssf.samplespecies = ss.topiaid 
WHERE
	EXTRACT(YEAR FROM t.landingdate) IN (2019)
		-- for the French fleet, 1 = France & 41 = Mayotte
		AND c.code IN (1, 41)
		-- 1 = YFT
--		AND spec.code = 1
		and ss.ldlfflag =2
;
