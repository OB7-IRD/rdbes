# Setup ----
library(devtools)
#install_github("https://github.com/OB7-IRD/furdeb")
library(furdeb)
library(tidyverse)
library(readxl)
library(RPostgreSQL)
library(worrms)

# Load configuration information ----

config_file <- configuration_file(path_file = "D:\\projets_themes\\data_calls\\rdbes\\2020\\data\\configfile_rdbes2020.yml")

# File separator
fsep = "\\"

# Databases connections ----
t3_con <- postgresql_db_connection (db_user = config_file[["databases_configuration"]][["t3_prod_vmot7"]][["login"]],
                                    db_password = config_file[["databases_configuration"]][["t3_prod_vmot7"]][["password"]],
                                    db_dbname = config_file[["databases_configuration"]][["t3_prod_vmot7"]][["dbname"]],
                                    db_host = config_file[["databases_configuration"]][["t3_prod_vmot7"]][["host"]],
                                    db_port = config_file[["databases_configuration"]][["t3_prod_vmot7"]][["port"]])

balbaya_con <- postgresql_db_connection (db_user = config_file[["databases_configuration"]][["balbaya_vmot5"]][["login"]],
                                         db_password = config_file[["databases_configuration"]][["balbaya_vmot5"]][["password"]],
                                         db_dbname = config_file[["databases_configuration"]][["balbaya_vmot5"]][["dbname"]],
                                         db_host = config_file[["databases_configuration"]][["balbaya_vmot5"]][["host"]],
                                         db_port = config_file[["databases_configuration"]][["balbaya_vmot5"]][["port"]])

# Table CL, Commercial Landing ----
t3_cl_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                               "scripts",
                                               "sql",
                                               "t3_cl_rdbes_2020.sql",
                                               fsep = fsep)),
                     collapse = '\n')

t3_cl <- dbGetQuery(conn = t3_con,
                    statement = t3_cl_query)

balbaya_cl_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                                    "scripts",
                                                    "sql",
                                                    "balbaya_cl_rdbes_2020.sql",
                                                    fsep = )),
                          collapse = '\n')

balbaya_cl <- dbGetQuery(conn = balbaya_con,
                         statement = balbaya_cl_query)

# CLeconZoneIndi = case_when(f_subarea_mao == "34.1" ~ "COAST",
#                            f_subarea_mao %in% c("34.3", "34.4", "41.1", "47.1", "47.A", "51.3", "51.4", "51.5", "51.6", "51.7") ~ "RFMO",
#                            TRUE ~ "eezi_not_define_yet")

for (a in c("t3_cl", "balbaya_cl")) {
  cat("Work in progress for data", a, "\nPlease be patient\n")
  # Areas definition
  tmp_cl <- get(x = a)
  tmp_cl <- marine_area_overlay(data = tmp_cl,
                                overlay_level = "major",
                                longitude_name = "longitude_dec",
                                latitude_name = "latitude_dec",
                                tolerance = 0) %>%
    mutate(CLarea = f_area_mao,
           CLstatRect = ifelse(test = f_area_mao == "27",
                               yes = stop("Area 27 present in data, need developing a function for defining ICES statistical rectangle\n"),
                               no = "-9"),
           CLgsaSubarea = ifelse(test = f_area_mao == "37",
                                 yes = stop("Area 37 present in data, need developing a function for defining GSA subarea\n"),
                                 no = "NotApplicable"),
           CLeconZoneIndi = "")
  assign(x = a,
         value = tmp_cl)
  # WoRMS variable
  tmp_species <- unique(tmp_cl[, c("specie_code", "CLspecFAO", "specie_scientific_name")])
  for (b in seq_len(length.out = dim(tmp_species)[1])) {
    if (class(try(expr = wm_name2id(name = tmp_species[b, "specie_scientific_name"]),
                  silent = TRUE)) != "try-error") {
      tmp_species[b, "CLspecCode"] <-  wm_name2id(name = tmp_species[b, "specie_scientific_name"])
    } else {
      if (tmp_species[b, "specie_scientific_name"] == "Thunnus alalunga") {
        tmp_species[b, "CLspecCode"] <- 127026
      } else if (tmp_species[b, "CLspecFAO"] == "RAV") {
        tmp_species[b, "CLspecCode"] <- 127017
      } else if (tmp_species[b, "specie_scientific_name"] == "Sphyraena barracuda") {
        tmp_species[b, "CLspecCode"] <- 345843
      } else {
        tmp_species[b, "CLspecCode"] <- "not_match"
      }
    }
  }
  assign(x = paste0(a, "_species"),
         value = tmp_species)
  cat("Process succeeds for data", a, "\n")
}
rm(tmp_cl, tmp_species, a, b)

for (a in c("t3_cl", "balbaya_cl")) {
  tmp_cl <- get(x = a)
  tmp_species <- get(x = paste0(a, "_species"))
  if ("not_match" %in% unique(tmp_cl$CLspecCode)) {
    stop("Be careful! At least one FAO specie code not match with WoRMS specie ID\n")
  } else {
    tmp_cl <- inner_join(x = tmp_cl,
                         y = tmp_species,
                         by = c("specie_code", "CLspecFAO", "specie_scientific_name"))
  }
  assign(x = a,
         value = tmp_cl)
}
rm(tmp_cl, tmp_species, a)

t3_cl <- group_by(.data = t3_cl,
                  CLrecType,
                  CLdTypSciWeig,
                  CLdSouSciWeig,
                  CLsampScheme,
                  CLdSouLanVal,
                  CLlanCou,
                  landing_fao_country,
                  CLvesFlagCou,
                  vessel_flag_country_fao,
                  CLyear,
                  CLquar,
                  CLmonth,
                  CLarea,
                  CLstatRect,
                  CLgsaSubarea,
                  CLjurisdArea,
                  CLeconZoneIndi,
                  CLspecCode,
                  CLspecFAO,
                  CLlandCat,
                  CLcatchCat,
                  CLsizeCatScale,
                  CLsizeCat,
                  CLnatFishAct,
                  CLmetier6,
                  CLIBmitiDev,
                  CLloc,
                  CLvesLenCat,
                  CLfishTech,
                  CLdeepSeaReg,
                  CLtotOffLanVal,
                  CLsciLanRSE,
                  CLvalRSE,
                  CLsciLanQualBias) %>%
  summarise(CLoffWeight = round(sum(CLoffWeight), 0),
            CLnumUniqVes = n_distinct(vessel_id),
            .groups = "drop")

balbaya_cl <- group_by(.data = balbaya_cl,
                       CLrecType,
                       CLdTypSciWeig,
                       CLdSouSciWeig,
                       CLsampScheme,
                       landing_fao_country,
                       vessel_flag_country_fao,
                       CLyear,
                       CLquar,
                       CLmonth,
                       CLarea,
                       CLstatRect,
                       CLgsaSubarea,
                       CLeconZoneIndi,
                       CLspecCode,
                       CLspecFAO,
                       CLlandCat,
                       CLcatchCat,
                       CLsizeCatScale,
                       CLsizeCat,
                       CLnatFishAct,
                       CLmetier6,
                       CLIBmitiDev,
                       CLloc,
                       CLvesLenCat,
                       CLfishTech,
                       CLdeepSeaReg) %>%
  summarise(CLsciWeight = round(sum(CLsciWeight), 0),
            .groups = "drop")

cl_final <- t3_cl %>%
  left_join(balbaya_cl,
            by = c("CLrecType",
                   "CLdTypSciWeig",
                   "CLdSouSciWeig",
                   "CLsampScheme",
                   "landing_fao_country",
                   "vessel_flag_country_fao",
                   "CLyear",
                   "CLquar",
                   "CLmonth",
                   "CLarea",
                   "CLstatRect",
                   "CLgsaSubarea",
                   "CLeconZoneIndi",
                   "CLspecCode",
                   "CLspecFAO",
                   "CLlandCat",
                   "CLcatchCat",
                   "CLsizeCatScale",
                   "CLsizeCat",
                   "CLnatFishAct",
                   "CLmetier6",
                   "CLIBmitiDev",
                   "CLloc",
                   "CLvesLenCat",
                   "CLfishTech",
                   "CLdeepSeaReg")) %>%
  filter(CLnumUniqVes >= 3) %>%
  mutate(CLexpDiff = case_when(
           CLoffWeight != CLsciWeight ~ "Sampld",
           TRUE ~ "NoDiff"
         )) %>%
  select(-landing_fao_country, -vessel_flag_country_fao) %>%
  relocate(CLoffWeight, CLsciWeight, CLexpDiff, .after = CLdeepSeaReg) %>%
  relocate(CLnumUniqVes, .after = CLtotOffLanVal)

# Table CE, Commercial Effort ----
balbaya_ce_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                                    "scripts",
                                                    "sql",
                                                    "balbaya_ce_rdbes_2020.sql",
                                                    fsep = fsep)),
                          collapse = '\n')

balbaya_ce <- dbGetQuery(conn = balbaya_con,
                         statement = balbaya_ce_query)

balbaya_ce <- marine_area_overlay(data = balbaya_ce,
                                  overlay_level = "major",
                                  longitude_name = "longitude_dec",
                                  latitude_name = "latitude_dec",
                                  tolerance = 10) %>%
  mutate(CEArea = f_area_mao,
         CEstatRect = ifelse(test = f_area_mao == "27",
                             yes = stop("Area 27 present in data, need developing a function for defining ICES statistical rectangle\n"),
                             no = "-9"),
         CEgsaSubarea = ifelse(test = f_area_mao == "37",
                               yes = stop("Area 37 present in data, need developing a function for defining GSA subarea\n"),
                               no = "NotApplicable"),
         CEeconZoneIndi = "")

ce_final <- balbaya_ce %>%
  group_by(CErecType,
           CEdTypSciEff,
           CEdSouSciEff,
           CEnatProgSciEff,
           CEvesFlagCou,
           CEyear,
           CEquar,
           CEMonth,
           CEArea,
           CEstatRect,
           CEgsaSubarea,
           CEjurisdArea,
           CEeconZoneIndi,
           CEnatFishAct,
           CEmetier6,
           CEIBmitiDev,
           CEloc,
           CEvesLenCat,
           CEfishTech,
           CEdeepSeaReg) %>%
  summarise(CEoffDaySea = sum(CEoffDaySea),
            CESciDaySea = sum(CESciDaySea),
            CEoffFishDay = sum(CEoffFishDay),
            CEsciFishDay = sum(CEsciFishDay),
            CEoffkWDaySea = sum(CEoffkWDaySea),
            CEscikWDaySea = sum(CEscikWDaySea),
            CEoffkWFishDay = sum(CEoffkWFishDay),
            CEscikWFishDay = sum(CEscikWFishDay),
            CEoffkWFishHour = sum(CEoffkWFishHour),
            CEscikWFishHour = sum(CEscikWFishHour),
            CEgTDaySea = sum(CEgTDaySea),
            CEgTFishDay = sum(CEgTFishDay),
            CEgTFishHour = sum(CEgTFishHour),
            CEnumUniqVes = n_distinct(vessel_id),
            .groups = "drop") %>%
  group_by(CEArea,
           CEstatRect,
           CEmetier6,
           CEMonth) %>%
  mutate(CEnumFracTrips = CEoffDaySea * 1 / sum(CEoffDaySea),
         CEnumDomTrip = case_when(
           CEnumFracTrips == max(CEnumFracTrips) ~ 1,
           TRUE ~ 0
         )) %>%
  ungroup() %>%
  filter(CEnumUniqVes >= 3) %>%
  mutate(CEnumFracTrips = round(CEnumFracTrips, 2),
         CEoffDaySea = round(CEoffDaySea, 2),
         CESciDaySea = round(CESciDaySea, 2),
         CEoffFishDay = round(CEoffFishDay, 2),
         CEsciFishDay = round(CEsciFishDay, 2),
         CEoffkWDaySea = round(CEoffkWDaySea, 0),
         CEscikWDaySea = round(CEscikWDaySea, 0),
         CEoffkWFishDay = round(CEoffkWFishDay, 0),
         CEscikWFishDay = round(CEscikWFishDay, 0),
         CEoffkWFishHour = round(CEoffkWFishHour, 0),
         CEscikWFishHour = round(CEscikWFishHour, 0),
         CEgTDaySea = round(CEgTDaySea, 0),
         CEgTFishDay = round(CEgTFishDay, 0),
         CEgTFishHour = round(CEgTFishHour, 0),
         CEoffNumHaulSet = "",
         CEsciNumHaulSet = "",
         CEoffVesFishHour = "",
         CEsciVesFishHour = "",
         CEoffSoakMeterHour = "",
         CEsciSoakMeterHour = "",
         CEsciFishDayRSE = "",
         CEscientificFishingDaysQualBias = "") %>%
  relocate(CEnumFracTrips, CEnumDomTrip, .after = CEdeepSeaReg) %>%
  relocate(CEoffNumHaulSet, CEsciNumHaulSet, CEoffVesFishHour, CEsciVesFishHour, CEoffSoakMeterHour, CEsciSoakMeterHour, .after = CEsciFishDay)

# Table SL, Species List ----
sl_final <- data.frame(SLrecType = "SL",
                       SLcou = "FR",
                       SLinst = as.integer(1836),
                       SLspeclistName = "IRD_specie_list",
                       SLyear = 2020,
                       SLcatchFrac = "Catch",
                       SLcommTaxon = wm_name2id(name = "Thunnus albacares"),
                       SLsppCode = wm_name2id(name = "Thunnus albacares"))

# Table VD, Vessel Detail ----
t3_vd_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                               "scripts",
                                               "sql",
                                               "t3_vd_rdbes_2020.sql",
                                               fsep = fsep)),
                     collapse = '\n')

t3_vd <- dbGetQuery(conn = t3_con,
                    statement = t3_vd_query)

vessel_code <- read.csv2(file = file.path(config_file[["wd_path"]],
                                          "data",
                                          "vessels_identification.csv",
                                          fsep = fsep))
vd_final <- t3_vd %>%
  left_join(vessel_code,
            by = c("VDencrVessCode" = "vessel_id_database")) %>%
  mutate(VDencrVessCode = vessel_id) %>%
  select(-vessel_id)

# Hierarchy 14 generation ----
# DE -> SD -> FT -> LE -> SS -> SA -> FM

# DE, Design ----
de_final <- data.frame(DErecordType = "DE",
                       DEsamplingScheme = "National Routine",
                       DEsamplingSchemeType = "NatRouCF",
                       DEyear = 2019,
                       DEstratumName = "OI1",
                       DEhierarchyCor = "N",
                       DEhierarchy = 5)

# SD, Sampling Details ----
sd_final <- data.frame(SDrecType = "SD",
                       SDctry = "FR",
                       SDinst = as.integer(1836))

# OS, OnShore Events ----
t3_os_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                               "scripts",
                                               "sql",
                                               "t3_os_rdbes_2020.sql",
                                               fsep = fsep)),
                     collapse = '\n')

os_final <- dbGetQuery(conn = t3_con,
                       statement = t3_os_query)

Encoding(os_final$OSnatName) = "UTF-8"

# FT, Fishing Trip ----
t3_ft_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                               "scripts",
                                               "sql",
                                               "t3_ft_rdbes_2020.sql",
                                               fsep = fsep)),
                     collapse = '\n')
ft_final <- dbGetQuery(conn = t3_con,
                       statement = t3_ft_query)

ft_final <- ft_final %>%
  inner_join(vessel_code,
             by = c("FTencrVessCode" = "vessel_id_database")) %>%
  mutate(FTencrVessCode = vessel_id) %>%
  select(-vessel_id)

# LE, Langing Event ----
t3_le_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                               "scripts",
                                               "sql",
                                               "t3_le_rdbes_2020.sql",
                                               fsep = fsep)),
                     collapse = '\n')

LE <- dbGetQuery(conn = t3_con,
                 statement = t3_le_query)

# marine_area_overlay(data = LE,
#                     overlay_level = "subunit",
#                     longitude_name = "longitude",
#                     latitude_name = "latitude",
#                     tolerance = 0) %>%
#   mutate(LEarea = ifelse(test = f_subunit_mao != "far_away",
#                          yes = f_subunit_mao,
#                          no = ifelse(test = f_subdivis_mao != "far_away",
#                                      yes = f_subdivis_mao,
#                                      no = ifelse(test = f_division_mao != "far_away",
#                                                  yes = f_division_mao,
#                                                  no = ifelse(test = f_subarea_mao != "far_away",
#                                                              yes = f_subarea_mao,
#                                                              no = f_area_mao))))

le_final <- marine_area_overlay(data = LE,
                                overlay_level = "major",
                                longitude_name = "longitude",
                                latitude_name = "latitude",
                                tolerance = 0) %>%
  mutate(LEarea = f_area_mao,
         LEstatRect = ifelse(test = f_area_mao == "27",
                             yes = stop("Area 27 present in data, need developing a function for defining ICES statistical rectangle\n"),
                             no = "-9"),
         LEgsaSubarea = ifelse(test = f_area_mao == "37",
                               yes = stop("Area 37 present in data, need developing a function for defining GSA subarea\n"),
                               no = "NotApplicable")) %>%
  left_join(vessel_code,
            by = c("LEencrVessCode" = "vessel_id_database")) %>%
  mutate(LEencrVessCode = vessel_id) %>%
  select(-trip_id, -vessel_id, -f_area_mao, -latitude, -longitude) %>%
  relocate(LEarea, LEstatRect, LEgsaSubarea, .after = LEeconZoneIndi) %>%
  relocate(LEnumTotal, LEnumSamp, .after = LEgearDim) %>%
  relocate(LEsamp, .after = LEincProbCluster)

# SS, Species Selection ----
t3_ss_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                               "scripts",
                                               "sql",
                                               "t3_ss_rdbes_2020.sql",
                                               fsep = fsep)),
                     collapse = '\n')

ss_final <- dbGetQuery(conn = t3_con,
                       statement = t3_ss_query)

# SA, SAmple ----
t3_sa_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                               "scripts",
                                               "sql",
                                               "t3_sa_rdbes_2020.sql",
                                               fsep = fsep)),
                     collapse = '\n')
SA <- dbGetQuery(conn = t3_con,
                 statement = t3_sa_query)

tmp_species_sa <- unique(SA[, c("specie_code", "SAspeCodeFAO", "specie_scientific_name")])
for (b in seq_len(length.out = dim(tmp_species_sa)[1])) {
  if (class(try(expr = wm_name2id(name = tmp_species_sa[b, "specie_scientific_name"]),
                silent = TRUE)) != "try-error") {
    tmp_species_sa[b, "SAspeCode"] <-  wm_name2id(name = tmp_species_sa[b, "specie_scientific_name"])
  } else {
    if (tmp_species_sa[b, "specie_scientific_name"] == "Thunnus alalunga") {
      tmp_species_sa[b, "SAspeCode"] <- 127026
    } else if (tmp_species_sa[b, "CLspecFAO"] == "RAV") {
      tmp_species_sa[b, "SAspeCode"] <- 127017
    } else if (tmp_species_sa[b, "specie_scientific_name"] == "Sphyraena barracuda") {
      tmp_species_sa[b, "SAspeCode"] <- 345843
    } else {
      tmp_species_sa[b, "SAspeCode"] <- "not_match"
    }
  }
}

if ("not_match" %in% unique(tmp_species_sa$SAspeCode)) {
  stop("Be careful! At least one FAO specie code not match with WoRMS specie ID\n")
} else {
  SA <- inner_join(x = SA,
                   y = tmp_species_sa,
                   by = c("specie_code",
                          "specie_scientific_name",
                          "SAspeCodeFAO"))
}

sa_final <- marine_area_overlay(data = SA,
                                overlay_level = "major",
                                longitude_name = "longitude_dec",
                                latitude_name = "latitude_dec",
                                tolerance = 0) %>%
  mutate(SAarea = f_area_mao,
         SArectangle = ifelse(test = f_area_mao == "27",
                              yes = stop("Area 27 present in data, need developing a function for defining ICES statistical rectangle\n"),
                              no = "-9"),
         SAgsaSubarea = ifelse(test = f_area_mao == "37",
                               yes = stop("Area 37 present in data, need developing a function for defining GSA subarea\n"),
                               no = "NotApplicable")) %>%
  select(-latitude_dec, -longitude_dec, -specie_code, -specie_scientific_name, -code_fao, -f_area_mao) %>%
  relocate(SAspeCode, .after = SAstratumName) %>%
  relocate(SAarea, SArectangle, SAgsaSubarea, .after = SAeconZoneIndi)

# FM, Frequencies Measures ----
t3_fm_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                               "scripts",
                                               "sql",
                                               "t3_fm_rdbes_2020.sql",
                                               fsep = fsep)),
                     collapse = '\n')

fm_final <- dbGetQuery(conn = t3_con,
                       statement = t3_fm_query)

# Extraction ----
tables <- c("de", "sd", "os", "ft", "le", "ss", "sa", "fm", "cl", "ce", "sl", "vd")
tables_order <- sprintf("%02d", 1:length(tables))
for (table in seq_len(length.out = length(tables))) {
  if (! "try-error" %in% class(try(expr = get(x = paste0(tables[table], "_final")),
                                   silent = TRUE))) {
    if (tables[table] %in% c("cl", "ce", "sl", "vd")) {
      write.table(x = get(x = paste0(tables[table], "_final")),
                  file = file.path(config_file[["output_path"]],
                                   paste0("rdbes_datacall2020_fra_H",
                                          toupper(tables[table]),
                                          ".csv"),
                                   fsep = fsep),
                  row.names = FALSE,
                  col.names = FALSE,
                  sep = ";",
                  quote = FALSE,
                  dec = '.')
    } else {
      write.table(x = get(x = paste0(tables[table], "_final")),
                  file = file.path(config_file[["output_path"]],
                                   paste0(tables_order[table],
                                          "_rdbes_datacall2020_",
                                          tables[table],
                                          "_fra_H5.csv"),
                                   fsep = fsep),
                  row.names = FALSE,
                  col.names = FALSE,
                  sep = ";",
                  quote = FALSE,
                  dec = '.')
    }
  } else {
    stop("Missing table ", table, " in the environment\n")
  }
}

# cmd command for windows (run in the output directory): copy *.csv rdbes_datacall2020_fra_h5.csv
