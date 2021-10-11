# Setup ----
library(devtools)
#install_github("https://github.com/OB7-IRD/furdeb")
library(furdeb)
library(tidyverse)
library(readxl)
library(RPostgreSQL)

config_file <- configuration_file(new_config = F,
                                  path_config = "/home/jlebranc/IRD/Projets/SI/7 - AAD/data_call_rdbes/configfile_rdbes2020.csv")

balbaya_con <- db_connection(db_user = config_file[["balbaya_user"]],
                             db_password = config_file[["balbaya_password"]],
                             db_dbname = config_file[["balbaya_dbname"]],
                             db_host = config_file[["balbaya_host"]],
                             db_port = config_file[["balbaya_port"]])

# Table SA, Sample
sa_query <- paste(readLines(con = file.path(config_file[["queries_loc"]],
                                               "balbaya_sa_rdbes_2020.sql",
                                               fsep = "/")),
                     collapse = '\n')

sa <- dbGetQuery(conn = balbaya_con,
                    statement = sa_query)



