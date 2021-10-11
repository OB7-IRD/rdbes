# setup ----
library(furdeb)
library(dplyr)
library(readxl)
library(RPostgreSQL)
library(worrms)

config_file <- configuration_file(path_file = "configuration_file_rdbes_2021.yml",
                                  silent = TRUE)
fsep <- "\\"
time_period <- c(2018, 2019, 2020)
countries <- c(1, 41)
species <- 1
gears <- c(1, 2, 3)

# databases connections ----
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

# table CL, Commercial Landing ----
referential_cl <- read_xlsx(path = file.path(config_file[["wd_path"]],
                                             "data",
                                             "RDBES Data Model CL CE_2021.xlsx",
                                             fsep = fsep),
                            sheet = "Commercial Landing CL",
                            range = cell_limits(ul = c(NA, NA),
                                                lr = c(39, 10)))

cl_t3_offical_weight_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                                              "scripts",
                                                              "sql",
                                                              "cl_t3_offical_weight.sql",
                                                              fsep = fsep)),
                                    collapse = '\n')

cl_t3_offical_weight_query <- DBI::sqlInterpolate(conn = t3_con,
                                                  sql = cl_t3_offical_weight_query,
                                                  time_period = DBI::SQL(paste0(time_period,
                                                                                collapse = ", ")),
                                                  countries = DBI::SQL(paste0(countries,
                                                                              collapse = ", ")),
                                                  species = DBI::SQL(paste0(species,
                                                                            collapse = ", ")))

cl_t3_offical_weight <- dbGetQuery(conn = t3_con,
                                   statement = cl_t3_offical_weight_query)

cl_balbaya_scientific_weight_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                                                      "scripts",
                                                                      "sql",
                                                                      "cl_balbaya_scientific_weight.sql",
                                                                      fsep = )),
                                            collapse = '\n')

cl_balbaya_scientific_weight_query <- DBI::sqlInterpolate(conn = balbaya_con,
                                                          sql = cl_balbaya_scientific_weight_query,
                                                          time_period = DBI::SQL(paste0(time_period,
                                                                                        collapse = ", ")),
                                                          countries = DBI::SQL(paste0(countries,
                                                                                      collapse = ", ")),
                                                          species = DBI::SQL(paste0(species,
                                                                                    collapse = ", ")))

cl_balbaya_scientific_weight <- dbGetQuery(conn = balbaya_con,
                                           statement = cl_balbaya_scientific_weight_query)

for (a in c("cl_t3_offical_weight",
            "cl_balbaya_scientific_weight")) {
  cat("Work in progress for data",
      a,
      "\nPlease be patient\n")
  # areas definition
  tmp_cl <- get(x = a)
  tmp_cl <- marine_area_overlay(data = tmp_cl,
                                overlay_expected = "all",
                                longitude_name = "longitude_dec",
                                latitude_name = "latitude_dec",
                                fao_area_file_path = config_file[["shapes_location"]]$fao_area,
                                fao_overlay_level = "division",
                                auto_selection_fao  = TRUE,
                                eez_area_file_path = config_file[["shapes_location"]]$eez_area,
                                for_fdi_use = TRUE,
                                ices_area_file_path = config_file[["shapes_location"]]$ices_area,
                                silent = TRUE) %>%
    rowwise() %>%
    mutate(CLarea = best_fao_area,
           CLstatRect = case_when(
             ! is.na(ices_area) ~ ices_area,
             TRUE ~ "-9"
           ),
           CLgsaSubarea = ifelse(test = unlist(strsplit(CLarea, '[.]'))[1] == "37",
                                 yes = stop("Area 37 present in data, need developing a function for defining GSA subarea\n"),
                                 no = "NotApplicable"),
           CLeconZoneIndi = eez_indicator)
  assign(x = a,
         value = tmp_cl)
  # WoRMS variable
  tmp_species <- unique(tmp_cl[, c("specie_code",
                                   "CLspecFAO",
                                   "specie_scientific_name")])
  for (b in seq_len(length.out = nrow(tmp_species))) {
    if (class(try(expr = wm_name2id(name = as.character(tmp_species[b, "specie_scientific_name"])),
                  silent = TRUE)) != "try-error") {
      tmp_species[b, "CLspecCode"] <-  wm_name2id(name = as.character(tmp_species[b, "specie_scientific_name"]))
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
rm(tmp_cl,
   tmp_species,
   a,
   b)

for (a in c("cl_t3_offical_weight",
            "cl_balbaya_scientific_weight")) {
  tmp_cl <- get(x = a)
  tmp_species <- get(x = paste0(a, "_species"))
  if ("not_match" %in% unique(tmp_cl$CLspecCode)) {
    stop("Be careful! At least one FAO specie code not match with WoRMS specie ID\n")
  } else {
    tmp_cl <- inner_join(x = tmp_cl,
                         y = tmp_species,
                         by = c("specie_code",
                                "CLspecFAO",
                                "specie_scientific_name"))
  }
  assign(x = a,
         value = tmp_cl)
}
rm(tmp_cl,
   tmp_species,
   a)

cl_t3_offical_weight <- cl_t3_offical_weight[! is.na(cl_t3_offical_weight$CLarea), ]
cl_balbaya_scientific_weight <- cl_balbaya_scientific_weight[! is.na(cl_balbaya_scientific_weight$CLarea), ]

cl_t3_offical_weight <- mutate(.data = cl_t3_offical_weight,
                               CLrecType = "CL",
                               CLsampScheme = "",
                               CLdSouLanVal = "Other",
                               CLjurisdArea = "",
                               CLlandCat = "HuC",
                               CLcatchCat = "Lan",
                               CLregDisCat = "NotApplicable",
                               CLsizeCatScale = "Unsorted",
                               CLsizeCat = "",
                               CLnatFishAct = "",
                               CLmetier6 = case_when(
                                 vessel_type == 1 ~ "PS_LPF_0_0_0",
                                 vessel_type == 2 ~ "LHP_LPF_0_0_0",
                                 vessel_type == 3 ~ "LLD_LPF_0_0_0",
                                 TRUE ~ "referential_error",
                               ),
                               CLIBmitiDev = "None",
                               CLvesLenCat = case_when(
                                 vessel_length < 8 ~ "<8",
                                 vessel_length >= 8 & vessel_length < 10 ~ '8-<10',
                                 vessel_length >= 10 & vessel_length < 12 ~ '10-<12',
                                 vessel_length >= 12 & vessel_length < 15 ~ '12-<15',
                                 vessel_length >= 15 & vessel_length < 18 ~ '15-<18',
                                 vessel_length >= 18 & vessel_length < 24 ~ '18-<24',
                                 vessel_length >= 24 & vessel_length < 40 ~ '24-<40',
                                 vessel_length >= 40 ~ '40<'
                               ),
                               CLfishTech = case_when(
                                 vessel_type == 1 ~ "PS",
                                 vessel_type %in% c(2, 3) ~ "HOK",
                                 TRUE ~ "referential_error"
                               ),
                               CLdeepSeaReg = "N",
                               CLtotOffLanVal = "NA",
                               CLsciWeightRSE = "",
                               CLvalRSE = "",
                               CLsciWeightQualBias = "NotApplicable") %>%
  group_by(CLrecType,
           CLsampScheme,
           CLdSouLanVal,
           CLlanCou,
           CLvesFlagCou,
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
           CLregDisCat,
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
           CLsciWeightRSE,
           CLvalRSE,
           CLsciWeightQualBias) %>%
  summarise(CLoffWeight = round(sum(CLoffWeight), 0),
            CLnumUniqVes = n_distinct(vessel_id),
            .groups = "drop") %>%
  relocate(CLoffWeight, .after = CLdeepSeaReg) %>%
  relocate(CLnumUniqVes, .after = CLtotOffLanVal)

cl_balbaya_scientific_weight <- mutate(.data = cl_balbaya_scientific_weight,
              CLdTypSciWeig = "Estimate",
              CLdSouSciWeig = "Logb",
              CLmetier6 = case_when(
                vessel_type == 1 ~ "PS_LPF_0_0_0",
                vessel_type == 2 ~ "LHP_LPF_0_0_0",
                vessel_type == 3 ~ "LLD_LPF_0_0_0",
                TRUE ~ "referential_error",
              ),
              CLvesLenCat = case_when(
                vessel_length < 8 ~ "<8",
                vessel_length >= 8 & vessel_length < 10 ~ '8-<10',
                vessel_length >= 10 & vessel_length < 12 ~ '10-<12',
                vessel_length >= 12 & vessel_length < 15 ~ '12-<15',
                vessel_length >= 15 & vessel_length < 18 ~ '15-<18',
                vessel_length >= 18 & vessel_length < 24 ~ '18-<24',
                vessel_length >= 24 & vessel_length < 40 ~ '24-<40',
                vessel_length >= 40 ~ '40<'
              ),
              CLfishTech = case_when(
                vessel_type == 1 ~ "PS",
                vessel_type %in% c(2, 3) ~ "HOK",
                TRUE ~ "referential_error"
              )) %>%
  group_by(CLdTypSciWeig,
           CLdSouSciWeig,
           CLyear,
           CLquar,
           CLmonth,
           CLarea,
           CLstatRect,
           CLgsaSubarea,
           CLeconZoneIndi,
           CLspecCode,
           CLspecFAO,
           CLmetier6,
           CLloc,
           CLvesLenCat,
           CLfishTech) %>%
  summarise(CLsciWeight = round(sum(CLsciWeight),
                                0),
            .groups = "drop")

cl_final <- cl_t3_offical_weight %>%
  left_join(cl_balbaya_scientific_weight,
            by = c("CLyear",
                   "CLquar",
                   "CLmonth",
                   "CLarea",
                   "CLstatRect",
                   "CLgsaSubarea",
                   "CLeconZoneIndi",
                   "CLspecCode",
                   "CLspecFAO",
                   "CLmetier6",
                   "CLloc",
                   "CLvesLenCat",
                   "CLfishTech")) %>%
  filter(CLnumUniqVes >= 3) %>%
  mutate(CLexpDiff = case_when(
    CLoffWeight != CLsciWeight ~ "Sampld",
    TRUE ~ "NoDiff"
  )) %>%
  relocate(CLdTypSciWeig, CLdSouSciWeig, .after = CLrecType) %>%
  relocate(CLsciWeight, CLexpDiff, .after = CLoffWeight)

# verification with the referential table
if (! identical(x = names(cl_final),
                y = filter(.data = referential_cl,
                           Order != "Not in format")$`R Name`)) {
  stop("Error in the CL columns names, check the CL referential table\n")
}
  
# table extraction
write.table(x = cl_final,
            file = file.path(config_file[["output_path"]],
                             paste0(format(Sys.time()
                                           ,"%Y%m%d_%H%M%S"),
                                    "_rdbes_cl.csv"),
                             fsep = fsep),
            row.names = FALSE,
            col.names = FALSE,
            sep = ",",
            quote = FALSE,
            dec = '.')

# table CE, Commercial Effort ----
referential_ce <- read_xlsx(path = file.path(config_file[["wd_path"]],
                                             "data",
                                             "RDBES Data Model CL CE_2021.xlsx",
                                             fsep = fsep),
                            sheet = "Commercial Effort CE",
                            range = cell_limits(ul = c(NA, NA),
                                                lr = c(46, 8)))

balbaya_ce_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                                    "scripts",
                                                    "sql",
                                                    "ce_balbaya.sql",
                                                    fsep = fsep)),
                          collapse = '\n')

balbaya_ce_query <- DBI::sqlInterpolate(conn = balbaya_con,
                                        sql = balbaya_ce_query,
                                        time_period = DBI::SQL(paste0(time_period,
                                                                      collapse = ", ")),
                                        countries = DBI::SQL(paste0(countries,
                                                                    collapse = ", ")),
                                        gears = DBI::SQL(paste0(gears,
                                                                collapse = ", ")))

ce <- dbGetQuery(conn = balbaya_con,
                 statement = balbaya_ce_query)

ce_final <- marine_area_overlay(data = ce,
                                overlay_expected = "all",
                                longitude_name = "longitude_dec",
                                latitude_name = "latitude_dec",
                                fao_area_file_path = config_file[["shapes_location"]]$fao_area,
                                fao_overlay_level = "division",
                                auto_selection_fao  = TRUE,
                                eez_area_file_path = config_file[["shapes_location"]]$eez_area,
                                for_fdi_use = TRUE,
                                ices_area_file_path = config_file[["shapes_location"]]$ices_area,
                                silent = TRUE) %>%
  mutate(CErecType = "CE",
         CEdTypSciEff = "Estimate",
         CEdSouSciEff = "Logb",
         CEsampScheme = "",
         CEvesFlagCou = "FR",
         CEjurisdArea = "",
         CEnatFishAct = "",
         CEmetier6 = case_when(
           vessel_type == 1 ~ "PS_LPF_0_0_0",
           vessel_type == 2 ~ "LHP_LPF_0_0_0",
           vessel_type == 3 ~ "LLD_LPF_0_0_0",
           TRUE ~ "referential_error",
         ),
         CEIBmitiDev = "None",
         CEvesLenCat = case_when(
           vessel_length < 8 ~ "<8",
           vessel_length >= 8 & vessel_length < 10 ~ '8-<10',
           vessel_length >= 10 & vessel_length < 12 ~ '10-<12',
           vessel_length >= 12 & vessel_length < 15 ~ '12-<15',
           vessel_length >= 15 & vessel_length < 18 ~ '15-<18',
           vessel_length >= 18 & vessel_length < 24 ~ '18-<24',
           vessel_length >= 24 & vessel_length < 40 ~ '24-<40',
           vessel_length >= 40 ~ '40<'
         ),
         CEfishTech = case_when(
           vessel_type == 1 ~ "PS",
           vessel_type %in% c(2, 3) ~ "HOK",
           TRUE ~ "referential_error"
         ),
         CEdeepSeaReg = "N",
         CEoffNumHaulSet = "",
         CEsciNumHaulSet = "",
         CEoffVesFishHour = "",
         CEsciVesFishHour = "",
         CEoffSoakMeterHour = "",
         CEsciSoakMeterHour = "",
         CEsciFishDayRSE = "",
         CEscientificFishingDaysQualBias = "",
         CEArea = case_when(
           is.na(best_fao_area) ~ db_division_fao,
           ! is.na(best_fao_area) ~ best_fao_area 
         ),
         CEstatRect = case_when(
           ! is.na(ices_area) ~ ices_area,
           TRUE ~ "-9"
         ),
         CEeconZoneIndi = eez_indicator,
         CEoffDaySea = hours_at_sea / 24,
         CESciDaySea = hours_at_sea / 24,
         CEoffFishDay = case_when(
           ocean == 1 ~ fishing_time / 12,
           ocean == 2 ~ fishing_time / 13
         ),
         CEsciFishDay = case_when(
           ocean == 1 ~ fishing_time / 12,
           ocean == 2 ~ fishing_time / 13
         ),
         CEoffkWDaySea = CEoffDaySea * vessel_engine_power * 0.73539875,
         CEscikWDaySea = CESciDaySea * vessel_engine_power * 0.73539875,
         CEoffkWFishDay = CEoffFishDay * vessel_engine_power * 0.73539875,
         CEscikWFishDay = CESciDaySea * vessel_engine_power * 0.73539875,
         CEoffkWFishHour = (CEoffFishDay / 24) * vessel_engine_power * 0.73539875,
         CEscikWFishHour = (CEsciFishDay / 24) * vessel_engine_power * 0.73539875,
         CEgTDaySea = CEoffDaySea * (0.2 + 0.02 * log10(vessel_volume)) * vessel_volume,
         CEgTFishDay = CEoffFishDay * (0.2 + 0.02 * log10(vessel_volume)) * vessel_volume,
         CEgTFishHour = (CEoffFishDay / 24) * (0.2 + 0.02 * log10(vessel_volume)) * vessel_volume) %>%
  rowwise() %>%
  mutate(CEgsaSubarea = ifelse(test = unlist(strsplit(CEArea, '[.]'))[1] == "37",
                               yes = stop("Area 37 present in data, need developing a function for defining GSA subarea\n"),
                               no = "NotApplicable")) %>%
  filter(CEArea != "99") %>%
  group_by(CErecType,
           CEdTypSciEff,
           CEdSouSciEff,
           CEsampScheme,
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
           CEdeepSeaReg,
           CEoffNumHaulSet,
           CEsciNumHaulSet,
           CEoffVesFishHour,
           CEsciVesFishHour,
           CEoffSoakMeterHour,
           CEsciSoakMeterHour,
           CEsciFishDayRSE,
           CEscientificFishingDaysQualBias) %>% 
  summarise(CEoffDaySea = round(sum(CEoffDaySea), 2),
            CESciDaySea = round(sum(CESciDaySea), 2),
            CEoffFishDay = round(sum(CEoffFishDay), 2),
            CEsciFishDay = round(sum(CEsciFishDay), 2),
            CEoffkWDaySea = round(sum(CEoffkWDaySea), 2),
            CEscikWDaySea = round(sum(CEscikWDaySea), 2),
            CEoffkWFishDay = round(sum(CEoffkWFishDay), 2),
            CEscikWFishDay = round(sum(CEscikWFishDay), 2),
            CEoffkWFishHour = round(sum(CEoffkWFishHour), 2),
            CEscikWFishHour = round(sum(CEscikWFishHour), 2),
            CEgTDaySea = round(sum(CEgTDaySea), 2),
            CEgTFishDay = round(sum(CEgTFishDay), 2),
            CEgTFishHour = round(sum(CEgTFishHour), 2),
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
  relocate(CEnumFracTrips,
           CEnumDomTrip,
           .after = CEdeepSeaReg) %>%
  relocate(CEoffDaySea,
           CESciDaySea,
           CEoffFishDay,
           CEsciFishDay,
           .after = CEnumDomTrip) %>%
  relocate(CEoffkWDaySea,
           CEscikWDaySea,
           CEoffkWFishDay,
           CEscikWFishDay,
           CEoffkWFishHour,
           CEscikWFishHour,
           CEgTDaySea,
           CEgTFishDay,
           CEgTFishHour,
           CEnumUniqVes,
           .after = CEsciSoakMeterHour)

# verification with the referential table
if (! identical(x = names(ce_final),
                y = filter(.data = referential_ce,
                           Order != "Not in format")$`R Name`)) {
  stop("Error in the CE columns names, check the CE referential table\n")
}

# table extraction
write.table(x = ce_final,
            file = file.path(config_file[["output_path"]],
                             paste0(format(Sys.time()
                                           ,"%Y%m%d_%H%M%S"),
                                    "_rdbes_ce.csv"),
                             fsep = fsep),
            row.names = FALSE,
            col.names = FALSE,
            sep = ",",
            quote = FALSE,
            dec = '.')

# table SL, Species List ----
referential_sl <- read_xlsx(path = file.path(config_file[["wd_path"]],
                                             "data",
                                             "RDBES Data Model VD SL 2021.xlsx",
                                             fsep = fsep),
                            sheet = "Species List Details",
                            range = cell_limits(ul = c(NA, NA),
                                                lr = c(10, 8)))

sl_final <- data.frame(SLrecType = rep(x = "SL", length(x = unique(cl_final$CLspecCode))),
                       SLcou = rep(x = "FR", length(x = unique(cl_final$CLspecCode))),
                       SLinst = rep(x = as.integer(1836), length(x = unique(cl_final$CLspecCode))),
                       SLspeclistName = rep(x = "IRD_specie_list", length(x = unique(cl_final$CLspecCode))),
                       SLyear = rep(x = 2021, length(x = unique(cl_final$CLspecCode))),
                       SLsppCode = unique(cl_final$CLspecCode)) %>%
  mutate(SLcatchFrac = case_when(
    SLsppCode == 127027 ~ "Catch",
    TRUE ~ "error"
  ),
  SLcommTaxon = case_when(
    SLsppCode == 127027 ~ 127027,
    TRUE ~ as.numeric(NA)
  )) %>%
  relocate(SLsppCode, .after = SLcommTaxon)

# verification with the referential table
if (! identical(x = names(sl_final),
                y = filter(.data = referential_sl,
                           Order != "Not in Format")$`R Name`)) {
  stop("Error in the SL columns names, check the SL referential table\n")
}

# table extraction
write.table(x = sl_final,
            file = file.path(config_file[["output_path"]],
                             paste0(format(Sys.time()
                                           ,"%Y%m%d_%H%M%S"),
                                    "_rdbes_sl.csv"),
                             fsep = fsep),
            row.names = FALSE,
            col.names = FALSE,
            sep = ",",
            quote = FALSE,
            dec = '.')

# table VD, Vessel Detail ----
referential_vd <- read_xlsx(path = file.path(config_file[["wd_path"]],
                                            "data",
                                             "RDBES Data Model VD SL 2021.xlsx",
                                             fsep = fsep),
                            sheet = "Vessel Details",
                            range = cell_limits(ul = c(NA, NA),
                                                lr = c(13, 11)))

t3_vd_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                               "scripts",
                                               "sql",
                                               "vd_t3.sql",
                                               fsep = fsep)),
                     collapse = '\n')

t3_vd_query <- DBI::sqlInterpolate(conn = t3_con,
                                   sql = t3_vd_query,
                                   time_period = DBI::SQL(paste0(time_period,
                                                                 collapse = ", ")),
                                   countries = DBI::SQL(paste0(countries,
                                                               collapse = ", ")),
                                   species = DBI::SQL(paste0(species,
                                                             collapse = ", ")))

vd <- dbGetQuery(conn = t3_con,
                 statement = t3_vd_query)

vessel_code <- read.csv2(file = file.path(config_file[["wd_path"]],
                                          "data",
                                          "vessels_identification.csv",
                                          fsep = fsep))

vd_final <- vd %>%
  mutate(VDrecType = "VD",
         VDctry = "FR",
         VDhomePort = "",
         VDlenCat = case_when(
           VDlen < 8 ~ "<8",
           VDlen >= 8 & VDlen < 10 ~ '8-<10',
           VDlen >= 10 & VDlen < 12 ~ '10-<12',
           VDlen >= 12 & VDlen < 15 ~ '12-<15',
           VDlen >= 15 & VDlen < 18 ~ '15-<18',
           VDlen >= 18 & VDlen < 24 ~ '18-<24',
           VDlen >= 24 & VDlen < 40 ~ '24-<40',
           VDlen >= 40 ~ '40<'
         ),
         VDtonUnit = "GT") %>%
  left_join(vessel_code,
            by = c("VDencrVessCode" = "vessel_id_database")) %>%
  mutate(VDencrVessCode = vessel_id) %>%
  select(-vessel_id) %>%
  relocate(VDrecType) %>%
  relocate(VDctry, VDhomePort, .before = VDflgCtry) %>%
  relocate(VDlenCat, .before = VDpwr)
  

# verification with the referential table
if (! identical(x = names(vd_final),
                y = filter(.data = referential_vd,
                           Order != "Not in Format")$`R Name`)) {
  stop("Error in the SL columns names, check the SL referential table\n")
}

# table extraction
write.table(x = vd_final,
            file = file.path(config_file[["output_path"]],
                             paste0(format(Sys.time()
                                           ,"%Y%m%d_%H%M%S"),
                                    "_rdbes_vd.csv"),
                             fsep = fsep),
            row.names = FALSE,
            col.names = FALSE,
            sep = ",",
            quote = FALSE,
            dec = '.')

# hierarchy n°5 ----
# referentials imports
referential_de <- read_xlsx(path = file.path(config_file[["wd_path"]],
                                             "data",
                                             "RDBES Data Model 2021.xlsx",
                                             fsep = fsep),
                            sheet = "Design",
                            range = cell_limits(ul = c(NA, NA),
                                                lr = c(11, 11)))
referential_sd <- read_xlsx(path = file.path(config_file[["wd_path"]],
                                             "data",
                                             "RDBES Data Model 2021.xlsx",
                                             fsep = fsep),
                            sheet = "Sampling Details",
                            range = cell_limits(ul = c(NA, NA),
                                                lr = c(61, 11)))

for (year in time_period) {
  # DE, Design ----
  de_final <- data.frame(DErecType = "DE",
                         DEsampScheme = "National Routine",
                         DEsampSchemeType = "NatRouCF",
                         DEyear = year,
                         DEstratumName = "FR_stratum1",
                         DEhierarchyCor = "N",
                         DEhierarchy = 5,
                         DEsamp = "Y",
                         DEnoSampReason = "")
  # verification with the referential table
  if (! identical(x = names(de_final),
                  y = filter(.data = referential_de,
                             Order != "Not in format")$`R Name`)) {
    stop("Error in the DE columns names, check the DE referential table\n")
  }
  # SD, Sampling Details ----
  sd_final <- data.frame(SDrecType = "SD",
                         SDctry = "FR",
                         SDinst = as.integer(1836))
  # verification with the referential table
  if (! identical(x = names(sd_final),
                  y = filter(.data = referential_sd,
                             Order != "Not in format")$`R Name`)) {
    stop("Error in the SD columns names, check the SD referential table\n")
  }
  # OS, Onshore Events ----
  t3_os_query <- paste(readLines(con = file.path(config_file[["wd_path"]],
                                                 "scripts",
                                                 "sql",
                                                 "t3_os_rdbes_2020.sql",
                                                 fsep = fsep)),
                       collapse = '\n')
  
  os_final <- dbGetQuery(conn = t3_con,
                         statement = t3_os_query)
  
  Encoding(os_final$OSnatName) = "UTF-8"
}




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
