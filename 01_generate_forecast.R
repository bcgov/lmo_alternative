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
regional_weight <- 1
industry_weight <- .1
occupation_weight <- .01
#functions-----------------------
get_cagr <- function(tbbl, horizon){
  max_year <- max(tbbl$year)
  start <- tbbl$employment[tbbl$year==max_year-horizon]
  end <-  tbbl$employment[tbbl$year==max_year]
  if_else(start==0, 1, (end/start)^(1/horizon)) #if start=0, the data is probably shitty
}

get_factor <- function(tbbl, horizon, cagr_weight){
  max_year <- max(tbbl$year)
  cagr <- get_cagr(tbbl, horizon)
  growth_factor <- cagr_weight*cagr+(1-cagr_weight)*top_cagr
  factor <- growth_factor^(1:11)
  year <- (max_year+1):(max_year+11)
  tibble(year=year, factor=factor)
}

ets_forecast <- function(tsbbl, horizon){
  tsbbl |>
    model(ETS(forecast_var))|> #square root keeps forecast non-negative
    forecast(h = horizon)|>
    tibble()|>
    select(-forecast_var, -.model)|>
    rename(forecast_var=.mean)
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
#top level historic data----------------

top_historic <- lfs|>
  filter(is.na(bc_region) & is.na(noc_5))|>
  group_by(year)|>
  summarize(employment=sum(employment))|>
  mutate(series="LFS")

top_cagr <- get_cagr(top_historic, 5)

#the budget forecast------------------

budget <- read_csv(here("data", "constraint.csv"))|>
  mutate(series="Budget forecast")
#forecast last 6 years based on historic + budget forecast(treated as observations)-------------

top_forecast <- bind_rows(top_historic, budget)|>
  rename(forecast_var=employment)|>
  tsibble(index = year)|>
  ets_forecast(6)|>
  rename(employment=forecast_var)|>
  mutate(series="Exponential Smoothing Forecast")

top <- bind_rows(budget, top_forecast)
# BASELINE SHARES: pool data since 2021------------

base_share <- lfs|>
  filter(year %in% c(2021:2023),
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
  tsibble(key = noc_5, index = year)|>
  fill_gaps(employment=0, .full=TRUE)|>
  tibble()|>
  group_by(noc_5)|>
  nest()|>
  mutate(factors=map(data, get_factor, 5, occupation_weight))|>
  unnest(factors)|>
  select(-data)|>
  rename(occupation_factor=factor)

# MAKE FORECAST--------------------------

employment_forecast <- full_join(base_share, regional_factor)|>
  full_join(industry_factor)|>
  full_join(occupation_factor)|>
  mutate(adjusted_share=base_share*regional_factor*industry_factor*occupation_factor)|>
  group_by(year)|>
  mutate(final_share=adjusted_share/sum(adjusted_share, na.rm = TRUE))|> #make sure forecast proportions sum to 1
  arrange(desc(employment))|>
  select(-employment)|>
  full_join(top|>select(-series))|>
  mutate(employment=final_share*employment,
         series="forecast")|>
  select(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry, year, employment, series)

