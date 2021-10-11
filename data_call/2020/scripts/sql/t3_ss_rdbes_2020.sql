select 
	 'SS'::text AS "SSrecType"
	, row_number() OVER (ORDER BY w.topiaid) as "SSseqNum"
	, 'N'::text as "SSstratification"
	, 'SORT'::text as "SSobsActTyp"
 	, 'Lan'::text as "SScatchFra"
 	, 'vol'::text as "SSobsTyp"
 	, 'U'::text as "SSstratumName"
 	, 'N'::text as "SSclustering"
 	, 'U'::text as "SSclusterName"
 	, 'Observer'::text as "SSsampler"
 	, 'IRD_specie_list'::text as "SSspecListName"
 	, 'N'::text as "SSuseCalcZero"
 	, 1 as "SSnumTotal"
 	, count(distinct ss.species) as "SSnumSamp"
	, ''::text as "SSselProb"
	, ''::text as "SSincProb"
 	, 'CENSUS'::text as "SSselectMeth"
 	, w.topiaid as "SSunitName"
	, ''::text as "SSselectMethCluster"
	, ''::text as "SSnumTotalClusters"
	, ''::text as "SSnumSampClusters"
	, ''::text as "SSselProbCluster"
	, ''::text as "SSincProbCluster"
	, 'Y'::text as "SSsamp"
	, ''::text as "SSnoSampReason"
FROM 
	sample s
    join trip t on (t.topiaid = s.trip)
    JOIN vessel v ON (t.vessel = v.topiaid)
    JOIN public.vesseltype vt ON (v.vesseltype = vt.topiaid)
	JOIN public.vesselsimpletype vst ON (vt.vesselsimpletype = vst.topiaid)
	JOIN public.country c ON (v.fleetcountry = c.topiaid)
	join samplespecies ss on ss.sample = s.topiaid 
	join species spec on spec.topiaid = ss.species 
	--join activity a2 on sset.activity = a2.topiaid 
	join well w on s.well = w.topiaid 
	--join wellplan wp on wp.well = w.topiaid 
WHERE
	EXTRACT(YEAR FROM t.landingdate) IN (2019)
		-- for the French fleet, 1 = France & 41 = Mayotte
		AND c.code IN (1, 41)
		and vt.code in  (1,2,4,5,6,7)
	group by w.topiaid 
;
