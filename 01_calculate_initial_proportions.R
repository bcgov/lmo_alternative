#libraries-----------------------
library(tidyverse)
library(here)
library(conflicted)
library(vroom)
library(janitor)
library(readxl)
library(fpp3)
conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::lag)
#constants---------------------
pval_power <- .02 #we raise p-values to this power to create weight on bc's growth rate.
base_years <- c(2022:2024) #need to change
#functions-----------------------

get_factor <- function(tbbl){
  tbbl <- tbbl|>
    mutate(series="base")
  max_year <- max(tbbl$year)
  bound <- bind_rows(tbbl, bc)#adds in top level employment to this series
  mod <- lm(log(employment+1)~year*series, data=bound)|>
    broom::tidy()
  this_growth <- mod|> #the estimated growth factor for this series
    filter(term=="year")|>
    pull(estimate)|>
    exp()
  pval <- mod|> #what is probability of observing a growth rate at least this different than BC's if null hypothesis is true?
    filter(term=="year:seriesLFS")|>
    pull(p.value)
  bc_weight <- pval^pval_power #the smaller the p.value, the smaller the weight on bc (more confident that growth factors differ)
  growth_factor <-(1-bc_weight)*this_growth+(bc_weight)*bc_growth_factor
  factor <- growth_factor^(2:12)#forecast starts 2 years later than middle of period used to calculate base shares
  year <- (max_year+1):(max_year+11)
  tibble(year=year, factor=factor)
}

agg_and_save <- function(tbbl, var1, var2=NULL){
  tbbl|>
    group_by(year, {{  var1  }}, {{  var2  }})|>
    summarize(employment=sum(employment))|>
    mutate(series="historic")|>
    write_rds(here("out",paste0("historic_",as.character(substitute(var1)),".rds")))
}
#read in the data----------------------
mapping <- read_excel(here("data", "mapping", "industry_mapping_2025_with_stokes_agg.xlsx"))|>
  select(naics_5, lmo_detailed_industry, lmo_ind_code)

lmo_nocs <-read_csv(here("data", "mapping", "noc21descriptions.csv"))|>
  select(lmo_noc)|>
  distinct()|>
  mutate(lmo_noc=as.character(lmo_noc),
         lmo_noc=str_pad(lmo_noc, "0", side = "left", width=5))

lmo_nocs <- bind_rows(lmo_nocs, tibble(lmo_noc=NA_character_)) #add in the NA so we keep the aggregate

lfs <- vroom(list.files(here("data","status"), pattern = "_stat", full.names = TRUE))|>
  clean_names()|>
  rename(year=syear,
         employment=count)|>
  mutate(employment=employment/12,
         noc_5=if_else(noc_5 %in% c("00011", "00012", "00013", "00014", "00015"), "00018", noc_5))|>
  filter(!is.na(year) & year<max(year, na.rm=TRUE))|>
  inner_join(mapping)|>
  group_by(year, bc_region, noc_5, lmo_ind_code, lmo_detailed_industry)|>
  summarize(employment=sum(employment))

#aggregate historic data by margin
no_aggregates <- lfs|>
  filter(!is.na(bc_region),
         !is.na(noc_5),
         lmo_detailed_industry!="Total, All Industries"
  )|>
  tsibble(index=year, key=c(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry))|>
  tsibble::fill_gaps(employment=0, .full=TRUE)|>
  tibble()

agg_and_save(no_aggregates, bc_region)
agg_and_save(no_aggregates, lmo_ind_code, lmo_detailed_industry)
agg_and_save(no_aggregates, noc_5)

#Historic data for all of BC
bc <- lfs|>
  filter(is.na(bc_region) & is.na(noc_5) & lmo_detailed_industry=="Total, All Industries")|>
  group_by(year)|>
  summarize(employment=sum(employment))|>
  mutate(series="LFS")

bc_with_cagr <- bc|>
  mutate(cagr=(employment[year==max(year)]/employment[year==(max(year)-10)])^.1-1)

bc_growth_factor <- lm(log(employment+1)~year, data=bc)|>
  broom::tidy()|> #the estimated growth factor for this series
  filter(term=="year")|>
  pull(estimate)|>
  exp()

 #CREATE BC FORECAST---------------------------------------
budget <- read_excel(here("data","constraint.xlsx"))|>
  mutate(series="budget forecast",
         cagr=(employment[year==max(year)]/employment[year==min(year)])^(1/(max(year)-min(year)))-1
  )

budget_growth <- (tail(budget$employment, n=1)/head(budget$employment, n=1))^.25

our_forecast <- tibble(year=(max(budget$year)+1):(max(budget$year)+6))|>
  mutate(employment=tail(budget$employment, n=1)*budget_growth^(year-max(budget$year)),
         series="our forecast",
         cagr=(employment[year==max(year)]/employment[year==min(year)])^(1/(max(year)-min(year)))-1
         )

bc_forecast <- our_forecast|>
  bind_rows(bc_with_cagr, budget)|>
  arrange(year)

write_rds(bc_forecast, here("out", "bc_forecast.rds"))
write_rds(bc_forecast, here("out","modified","bc_forecast.rds"))

#' BASELINE SHARES: based on recent data (2020=COVID)------------

base_share <- lfs|>
  filter(year %in% base_years, #' note that some NOCs have either missing or zero employment for the base years
         !is.na(bc_region),
         !is.na(noc_5),
         lmo_detailed_industry!="Total, All Industries"
         )|>
  group_by(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry)|>
  summarize(employment=sum(employment))|>
  ungroup()|>
  mutate(base_share=employment/sum(employment))|>
  filter(base_share>0)

#REGIONAL GROWTH FACTORS----------------
regional_factor <- lfs|>
  filter(!is.na(bc_region),
         is.na(noc_5),
         lmo_detailed_industry=="Total, All Industries"
         )|>
  group_by(year, bc_region)|>
  summarize(employment=sum(employment))|>
  group_by(bc_region)|>
  nest()|>
  mutate(factors=map(data, get_factor))|>
  unnest(factors)|>
  select(-data)|>
  rename(regional_factor=factor)

#INDUSTRY GROWTH FACTORS---------------
industry_factor <- lfs|>
  filter(is.na(bc_region),
         is.na(noc_5),
         lmo_detailed_industry!="Total, All Industries"
         )|>
  group_by(lmo_ind_code, lmo_detailed_industry)|>
  select(-bc_region, -noc_5)|>
  nest()|>
  mutate(factors=map(data, get_factor))|>
  unnest(factors)|>
  select(-data)|>
  rename(industry_factor=factor)

#OCCUPATION GROWTH FACTORS---------------------------------
occupation_factor <- lfs|>
  filter(is.na(bc_region),
         !is.na(noc_5),
         lmo_detailed_industry=="Total, All Industries"
         )|>
  group_by(year, noc_5)|>
  summarize(employment=sum(employment))|>
  tsibble(key = noc_5, index = year)|> #some implicit missing in the occupational data
  fill_gaps(employment=0, .full=TRUE)|> #replace implicit missing with 0
  tibble()|>
  group_by(noc_5)|>
  nest()|>
  mutate(factors=map(data, get_factor))|>
  unnest(factors)|>
  select(-data)|>
  rename(occupation_factor=factor)


# PRE MODIFICATION SHARES --------------------------
pre_mod_shares <- full_join(base_share, regional_factor)|>
  full_join(industry_factor)|>
  full_join(occupation_factor)|>
  mutate(adjusted_share=base_share*regional_factor*industry_factor*occupation_factor)|>
  group_by(year)|>
  mutate(pre_mod_share=adjusted_share/sum(adjusted_share, na.rm = TRUE))|> #make proportions sum to 1
  select(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry, year, pre_mod_share)|>
  filter(pre_mod_share>0)

write_rds(pre_mod_shares, here("out","modified","shares.rds"))
write_rds(pre_mod_shares, here("out","pre_mod_shares.rds"))

