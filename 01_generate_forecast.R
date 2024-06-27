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
regional_weight <- .5
industry_weight <- .25
occupation_weight <- .05
# regional_weight <- 0
# industry_weight <- 0
# occupation_weight <- 0

cagr_horizon <- 10
base_years <- c(2018:2019, 2021:2023)
#functions-----------------------
get_cagr <- function(tbbl, horizon){
  max_year <- max(tbbl$year)
  start <- tbbl$employment[tbbl$year==max_year-horizon]
  end <-  tbbl$employment[tbbl$year==max_year]
  if_else(start<=500, 1, (end/start)^(1/horizon)) #if start<500 the data is probably shitty: assume zero growth
}

get_factor <- function(tbbl, horizon, cagr_weight){
  max_year <- max(tbbl$year)
  cagr <- get_cagr(tbbl, horizon)
  growth_factor <- cagr_weight*cagr+(1-cagr_weight)*bc_cagr
  factor <- growth_factor^(0:10)
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
  summarize(employment=sum(employment))|>
  mutate(series="LFS")

#growth rate for all of bc: shrink towards for dodgy disaggregates----------------
bc_cagr <- bc|>
  get_cagr(cagr_horizon)

#CREATE BC FORECAST---------------------------------------
budget <- read_csv(here("data","constraint.csv"))|>
  mutate(series="budget forecast")

bind_rows(bc, budget)|>
  tsibble(index = year)|>
  model(ETS=ETS(employment),
        log_ETS=ETS(log(employment)),
        tslm=TSLM(employment~year),
        log_tslm=TSLM(log(employment)~year)
  )|>
  forecast(h = 6)|>
  tibble()|>
  select(.model, year, .mean)|>
  rename(employment=.mean,
         series=.model)|>
  group_by(year)|>
  summarize(employment=mean(employment))|>
  mutate(series="our forecast")|>
  bind_rows(bc, budget)|>
  arrange(year)|>
  write_rds(here("out", "bc_forecast.rds"))

# BASELINE SHARES: based on recent data (2020=COVID)------------

base_share <- lfs|>
  filter(year %in% base_years,
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
  mutate(factors=map(data, get_factor, cagr_horizon, regional_weight))|>
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
  mutate(factors=map(data, get_factor, cagr_horizon, industry_weight))|>
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
  mutate(factors=map(data, get_factor, cagr_horizon, occupation_weight))|>
  unnest(factors)|>
  select(-data)|>
  rename(occupation_factor=factor)

# PRE MODIFICATION SHARES --------------------------
pre_mod_shares <- left_join(base_share, regional_factor)|>
  left_join(industry_factor)|>
  left_join(occupation_factor)|>
  mutate(adjusted_share=base_share*regional_factor*industry_factor*occupation_factor)|>
  group_by(year)|>
  mutate(pre_mod_share=adjusted_share/sum(adjusted_share, na.rm = TRUE))|> #make proportions sum to 1
  select(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry, year, pre_mod_share)|>
  filter(pre_mod_share>0)

write_rds(pre_mod_shares, here("out","modified_shares","shares.rds"))
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
