# Setup ----
library(devtools)
#install_github("https://github.com/OB7-IRD/furdeb")
library(furdeb)
library(tidyverse)
library(readxl)
library(RPostgreSQL)
library(worrms)

config_file <- configuration_file(new_config = F,
                                  path_config = "configfile_rdbes2020.csv")

# Databases connections ----
t3_con <- db_connection(db_user = config_file[["t3plus_user"]],
                        db_password = config_file[["t3plus_password"]],
                        db_dbname = config_file[["t3plus_dbname"]],
                        db_host = config_file[["t3plus_host"]],
                        db_port = config_file[["t3plus_port"]])

# Table SA, Sample
t3_sa_query <- paste(readLines(con = file.path(config_file[["queries_loc"]],
                                               "t3_sa_rdbes_2020.sql",
                                               fsep = "/")),
                     collapse = '\n')

t3_sa <- dbGetQuery(conn = t3_con,
                    statement = t3_sa_query)

# dim(unique(t3_cl[,c("vessel_id", "date", "number")]))[1]
# dim(unique(balbaya_cl[, c("c_bat", "d_act", "n_act")]))[1]
# 
# tmp1 <- unique(t3_cl[,c("vessel_id", "date", "number")]) %>%
#   rename("c_bat" = "vessel_id",
#          "d_act" = "date",
#          "n_act" = "number")
# tmp2 <- unique(balbaya_cl[, c("c_bat", "d_act", "n_act")])
# setdiff(x = tmp1, y = tmp2)
# 
# tmp1[tmp1$c_bat==889 & tmp1$d_act=="2019-01-01",]
# tmp2[tmp2$c_bat==889 & tmp2$d_act=="2019-01-01",]

for (a in c("t3_cl", "balbaya_cl")) {
  cat("Work in progress for data", a, "\nPlease be patient\n")
  # Areas definition
  tmp_cl <- get(x = a)
  tmp_cl <- marine_area_overlay(data = tmp_cl,
                               overlay_level = "subunit",
                               longitude_name = "longitude_dec",
                               latitude_name = "latitude_dec",
                               tolerance = 0) %>%
    mutate(CLarea = ifelse(test = f_subunit_mao != "far_away",
                           yes = f_subunit_mao,
                           no = ifelse(test = f_subdivis_mao != "far_away",
                                       yes = f_subdivis_mao,
                                       no = ifelse(test = f_division_mao != "far_away",
                                                   yes = f_division_mao,
                                                   no = ifelse(test = f_subarea_mao != "far_away",
                                                               yes = f_subarea_mao,
                                                               no = f_area_mao)))),
           CLstatRect = ifelse(test = f_area_mao == "27",
                               yes = stop("Area 27 present in data, need developing a function for defining ICES statistical rectangle\n"),
                               no = "-9"),
           CLgsaSubarea = ifelse(test = f_area_mao == "37",
                                 yes = stop("Area 37 present in data, need developing a function for defining GSA subarea\n"),
                                 no = "NotApplicable"),
           CLeconZoneIndi = case_when(f_subarea_mao == "34.1" ~ "COAST",
                                      f_subarea_mao %in% c("34.3", "34.4", "41.1", "47.1", "47.A", "51.3", "51.4", "51.5", "51.6", "51.7") ~ "NA",
                                      TRUE ~ "eezi_not_define_yet"))
  assign(x = a,
         value = tmp_cl)
  # WoRMS variable
  tmp_species <- unique(tmp_cl[, c("specie_code", "SAspeCodeFAO", "specie_scientific_name")])
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

# Design

t3_cl <- group_by(.data = t3_cl,
                  CLrecType,
                  CLdTypSciWeig,
                  CLdSouSciWeig,
                  CLlanCou,
                  CLvesFlagCou,
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
                  CLIBmitiDev,
                  CLloc,
                  CLvesLenCat,
                  CLfishTech,
                  CLdeepSeaReg) %>% 
  mutate(CLnumUniqVes = n_distinct(vessel_id)) %>%
  ungroup() %>%
  select(CLrecType,
         CLdTypSciWeig,
         CLdSouSciWeig,
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
         CLeconZoneIndi,
         CLspecCode,
         CLspecFAO,
         CLmetier6,
         CLIBmitiDev,
         CLloc,
         CLvesLenCat,
         CLfishTech,
         CLdeepSeaReg,
         CLoffWeight,
         CLnumUniqVes)

t3_cl_final <- t3_cl[t3_cl$CLnumUniqVes >= 3, ]

balbaya_cl <- select(.data = balbaya_cl,
                     CLrecType,
                     CLdTypSciWeig,
                     CLdSouSciWeig,
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
                     CLmetier6,
                     CLIBmitiDev,
                     CLloc,
                     CLvesLenCat,
                     CLfishTech,
                     CLdeepSeaReg,
                     CLsciWeight)

tmp <- t3_cl_final %>%
  left_join(balbaya_cl)
















