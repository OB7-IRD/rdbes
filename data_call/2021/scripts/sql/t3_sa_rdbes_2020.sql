SELECT
	'SA'::text AS "SArecType"
	,s.topiaid::text as "SAseqNum"
	,''::text as  "SAparSequNum"
	,'N'::text as "SAstratification"
	,'U'::text as "SAstratumName"
	,a2.latitude::numeric AS latitude_dec
	,a2.longitude::numeric AS longitude_dec
 	,spec.code::integer AS specie_code
 	,spec.scientificlabel::text AS specie_scientific_name
 	,spec.code3l ::text AS code_fao
 	,spec.faoid as "SAspeCodeFAO"
 	,'Salted '::text as "SApres"
    ,'Dead'::text as "SAspecState"
	,'Lan'::text as "SAcatchCat"
 	,'HuC'::TEXT AS "SAlandCat"
 	,''::text as "SAcommCatScl"
 	,''::text as "SAcommCat"
 	,'U'::text as "SAsex"
 	,'RFMO'::text as "SAeconZoneIndi"
	,''::text as "SAjurisdArea"
	,''::text as "SAnatFishAct"	 
	,CASE
		WHEN vst.code = 1 THEN 'PS_LPF'
		WHEN vst.code = 2 THEN 'LHP_LPF'
		WHEN vst.code = 3 THEN 'LLD_LPF'
		ELSE 'to_define'
	END::text AS "SAmetier5"
	,CASE
		WHEN vst.code = 1 THEN 'PS_LPF_0_0_0'
		WHEN vst.code = 2 THEN 'LHP_LPF_0_0_0'
		WHEN vst.code = 3 THEN 'LLD_LPF_0_0_0'
		ELSE 'to_define'
	END::text AS "SAmetier6"
	,CASE
		WHEN vst.code = 1 THEN 'PS'
		WHEN vst.code = 2 THEN 'LHP'
		WHEN vst.code = 3 THEN 'LLD'
		ELSE 'to_define'
	END::text AS "SAgear"
	,''::text as "SAmeshSize"	
 	,0::integer AS "SAselDev"
	,''::text as "SAselDevMeshSize"	 
 	,'Haul'::text as "SAunitType"
	,sum(wp.weight) as "SAtotalWtLive"
	,sum(s.globalweight) as "SAsampWtLive"
 	,''::text as "SAnumTotal"
 	,''::text as "SAnumSamp"
	,''::text as "SAselProb"
	,''::text as "SAincProb"
	,'SRSWOR'::text as "SAselectMeth"
	,s.topiaid::text as "SAunitName" -- , "SAunitName" (Stratum code DCF OA1/OI1)
	,'B'::text as "SAlowHierarchy"
	,'Observer'::text as "SAsampler"
	,'Y'::text as "SAsamp"
	,''::text as "SAnoSampReasonFM"
	,''::text as "SAnoSampReasonBV"
	,''::text as "SAtotalWtMes"
	,''::text as "SAsampWtMes"
	,''::text as "SAconFacMesLive"	
FROM 
	sample s
    join trip t on (t.topiaid = s.trip)
    JOIN vessel v ON (t.vessel = v.topiaid)
    JOIN public.vesseltype vt ON (v.vesseltype = vt.topiaid)
	JOIN public.vesselsimpletype vst ON (vt.vesselsimpletype = vst.topiaid)
	JOIN public.country c ON (v.fleetcountry = c.topiaid)
	join samplespecies ss on ss.sample = s.topiaid 
	join species spec on spec.topiaid = ss.species 
	join sampleset sset on sset.sample = s.topiaid 
	join activity a2 on sset.activity = a2.topiaid 
	join well w on s.well = w.topiaid 
	join wellplan wp on wp.well = w.topiaid 
WHERE
	EXTRACT(YEAR FROM t.landingdate) IN (2019)
	-- for the French fleet, 1 = France & 41 = Mayotte
	AND c.code IN (1, 41)
	AND vt.code in  (1,2,4,5,6,7)
	-- 1 = YFT
	-- AND spec.code = 1
	AND ss.ldlfflag =2
GROUP BY 
	s.topiaid
	,t.landingdate
	,a2.latitude
	,a2.longitude
	,spec.code
	,spec.scientificlabel
	,spec.code3l
	,spec.faoid
	,vst.code
;
