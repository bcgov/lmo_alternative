#libraries-----------------------
library(tidyverse)
library(here)
library(conflicted)
library(vroom)
library(janitor)
library(fpp3)
conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::lag)

#constants---------------------
regional_weight <- .25
industry_weight <- .25
occupation_weight <- .1

#functions-----------------------
get_cagr <- function(tbbl, horizon){
  max_year <- max(tbbl$year)
  start <- tbbl$employment[tbbl$year==max_year-horizon]
  end <-  tbbl$employment[tbbl$year==max_year]
  if_else(start==0, 1, (end/start)^(1/horizon)) #if start=0 the data is probably shitty: assume zero growth
}

get_factor <- function(tbbl, horizon, cagr_weight){
  max_year <- max(tbbl$year)
  cagr <- get_cagr(tbbl, horizon)
  growth_factor <- cagr_weight*cagr+(1-cagr_weight)*bc_cagr
  factor <- growth_factor^(1:11)
  year <- (max_year+1):(max_year+11)
  tibble(year=year, factor=factor)
}
agg_and_save <- function(var1, var2=NULL){
  no_aggregates|>
    group_by(year, {{  var1  }}, {{  var2  }})|>
    summarize(employment=sum(employment))|>
    mutate(series="historic")|>
    write_rds(here("out",paste0("historic_",as.character(substitute(var1)),".rds")))
}
forecast_and_save <- function(var){
  pre_mod_shares|>
    group_by(year, {{  var  }})|>
    summarise(pre_mod_share=sum(pre_mod_share))|>
    full_join(bc_fcast)|>
    mutate(employment=employment*pre_mod_share)|>
    write_rds(here("out",paste0("base_fcast_",as.character(substitute(var)),".rds")))
}

#read in the data----------------------
mapping <- read_csv(here("data", "mapping", "tidy_2024_naics_to_lmo.csv"))|>
  mutate(naics=paste0("0",naics))|>
  select(-naics3,-naics2)

lfs <- vroom(list.files(here("data"), pattern = "RTRA", full.names = TRUE))|>
  clean_names()|>
  rename(year=syear,
         employment=count)|>
  mutate(employment=employment/12)|>
  filter(!is.na(year) & year<max(year, na.rm=TRUE))|>
  inner_join(mapping, by = c("naics_5"="naics"))|>
  group_by(year, bc_region, noc_5, lmo_ind_code, lmo_detailed_industry)|>
  summarize(employment=sum(employment))

#Historic data for all of BC
bc <- lfs|>
  filter(is.na(bc_region) & is.na(noc_5))|>
  group_by(year)|>
  summarize(employment=sum(employment))

#growth rate for all of bc: shrink towards for sparse data (e.g. by occupation)----------------
bc_cagr <- bc|>
  get_cagr(5)

#CREATE BC FORECAST---------------------------------------
budget <- read_csv(here("data","constraint.csv"))

bind_rows(bc, budget)|>
  tsibble(index = year)|>
  model(ETS(employment))|>
  forecast(h = 6)|>
  tibble()|>
  select(year, .mean)|>
  rename(employment=.mean)|>
  bind_rows(budget)|>
  arrange(year)|>
  write_rds(here("out", "bc_forecast.rds"))

# BASELINE SHARES: based on recent data (2020=COVID)------------
base_share <- lfs|>
  filter(year %in% c(2018, 2019, 2021:2023),
         !is.na(bc_region),
         !is.na(noc_5))|>
  group_by(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry)|>
  summarize(employment=sum(employment))|>
  ungroup()|>
  mutate(base_share=employment/sum(employment))

#REGIONAL GROWTH FACTORS----------------
regional_factor <- lfs|>
  filter(!is.na(bc_region),
         is.na(noc_5))|>
  group_by(year, bc_region)|>
  summarize(employment=sum(employment))|>
  group_by(bc_region)|>
  nest()|>
  mutate(factors=map(data, get_factor, 5, regional_weight))|>
  unnest(factors)|>
  select(-data)|>
  rename(regional_factor=factor)

#INDUSTRY GROWTH FACTORS---------------
industry_factor <- lfs|>
  filter(is.na(bc_region),
         is.na(noc_5))|>
  group_by(lmo_ind_code, lmo_detailed_industry)|>
  select(-bc_region, -noc_5)|>
  nest()|>
  mutate(factors=map(data, get_factor, 5, industry_weight))|>
  unnest(factors)|>
  select(-data)|>
  rename(industry_factor=factor)

#OCCUPATION GROWTH FACTORS---------------------------------
occupation_factor <- lfs|>
  filter(is.na(bc_region),
         !is.na(noc_5))|>
  group_by(year, noc_5)|>
  summarize(employment=sum(employment))|>
  tsibble(key = noc_5, index = year)|> #some implicit missing in the occupational data
  fill_gaps(employment=0, .full=TRUE)|> #replace implicit missing with 0
  tibble()|>
  group_by(noc_5)|>
  nest()|>
  mutate(factors=map(data, get_factor, 5, occupation_weight))|>
  unnest(factors)|>
  select(-data)|>
  rename(occupation_factor=factor)

# PRE MODIFICATION SHARES --------------------------
pre_mod_shares <- left_join(base_share, regional_factor)|>
  left_join(industry_factor)|>
  left_join(occupation_factor)|>
  mutate(adjusted_share=base_share*regional_factor*industry_factor*occupation_factor)|>
  group_by(year)|>
  mutate(final_share=adjusted_share/sum(adjusted_share, na.rm = TRUE))|> #make proportions sum to 1
  arrange(desc(employment))|>
  select(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry, year, pre_mod_share=final_share)

write_rds(pre_mod_shares, here("out","pre_mod_shares.rds"))

#aggregate historic data by margin
no_aggregates <- lfs|>
  filter(!is.na(bc_region),
         !is.na(noc_5)
  )|>
  tsibble(index=year, key=c(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry))|>
  tsibble::fill_gaps(employment=0, .full=TRUE)|>
  tibble()

agg_and_save(bc_region)
agg_and_save(lmo_ind_code, lmo_detailed_industry)
agg_and_save(noc_5)







