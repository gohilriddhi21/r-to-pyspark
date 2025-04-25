source("~/Desktop/Candidate Assessment/helpers.r")
source("~/Desktop/Candidate Assessment/convertMe.r")

install.packages("tidyverse")
install.packages("dplyr")
install.packages("readr")
install.packages("lubridate")

library("tidyverse")
library("dplyr")
library("readr")
library("lubridate")

dfCannibalizationFactors <- read.csv("~/Desktop/Candidate Assessment/data/dfCannibalizationFactors.csv")
dfMonthlySales <- read.csv("~/Desktop/Candidate Assessment/data/dfMonthlySales.csv")
dfReinvestmentFactors <- read.csv("~/Desktop/Candidate Assessment/data/dfReinvestmentFactors.csv")
dfReinvestmentProjects <- read.csv("~/Desktop/Candidate Assessment/data/dfReinvestmentProjects.csv")
dfSalesDaysFuture <- read.csv("~/Desktop/Candidate Assessment/data/dfSalesDaysFuture.csv")

missing_date_index <- dfSalesDaysFuture$close_date == 'null'
dfSalesDaysFuture$close_date[missing_date_index] <- '2200-01-01'

dfSalesDaysFuture <- dfSalesDaysFuture %>% mutate(
  loc_num = str_pad(loc_num, 5, pad = "0"),
  open_date = ymd(open_date),
  close_date = ymd(close_date), 
  months_predict = 120
)
dfCannibalizationFactors <- dfCannibalizationFactors %>% mutate(month = ymd(month))
dfMonthlySales <- dfMonthlySales %>% mutate(
  loc_num = str_pad(loc_num, 5, pad = "0"),
  month = ymd(month), 
  open_date = ymd(open_date)
)
dfReinvestmentFactors <- dfReinvestmentFactors %>% mutate(
  loc_num = str_pad(loc_num, 5, pad = "0"),
  month = ymd(month))
dfReinvestmentProjects <- dfReinvestmentProjects %>% mutate(
  loc_num = str_pad(loc_num, 5, pad = "0"),
  shutdown = ymd(shutdown), 
  reopen = ymd(reopen)
)

convertMe(dfSalesDaysFuture, 
          dfMonthlySales, 
          dfReinvestmentProjects, 
          dfCannibalizationFactors, 
          dfReinvestmentFactors)



