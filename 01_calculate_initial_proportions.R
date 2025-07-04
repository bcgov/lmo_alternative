#libraries-----------------------
library(tidyverse)
library(here)
library(vroom)
library(janitor)
library(readxl)
library(fpp3)
library(conflicted)
conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::lag)
#constants---------------------
pval_power <- .1 #we raise p-values to this power to create weight on bc's growth rate.
base_years <- c(2022:2024) #need to change
base_plus <- 3:13 #lfs base year midpoint is 2023, census was 2021, so forecast starts 3 years later than midpoint of base years.
#functions-----------------------
get_factor <- function(tbbl, base_plus){
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
  factor <- growth_factor^(base_plus)#forecast starts 3 years later than middle of period used to calculate base shares
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
#read in mapping files----------------------
mapping <- read_excel(here("data", "mapping", "industry_mapping_2025_with_stokes_agg.xlsx"))|>
  select(naics_5, lmo_detailed_industry, lmo_ind_code)

lmo_nocs <-read_csv(here("data", "mapping", "noc21descriptions.csv"))|>
  select(lmo_noc)|>
  distinct()|>
  mutate(lmo_noc=as.character(lmo_noc),
         lmo_noc=str_pad(lmo_noc, "0", side = "left", width=5))

lmo_nocs <- bind_rows(lmo_nocs, tibble(lmo_noc=NA_character_)) #add in the NA so we keep the aggregate

census_mapping <- read_excel(here("data","mapping","mapping_census_to_lmo_63.xlsx"))

#read in the raw data----------------------

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

lfs_no_aggregates <- lfs|>
  filter(!is.na(bc_region),
         !is.na(noc_5),
         lmo_detailed_industry!="Total, All Industries"
  )|>
  tsibble(index=year, key=c(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry))|>
  tsibble::fill_gaps(employment=0, .full=TRUE)|>
  tibble()

census <- tibble(files=list.files(here("data","census")))|>
  mutate(paths=here("data","census", files),
         data=map(paths, vroom))|>
  separate(files, into = c("bc_region", "extension"), sep = "\\.")|>
  select(bc_region, data)|>
  mutate(bc_region=if_else(bc_region=="North Coast", "North Coast & Nechako", bc_region),
         bc_region=if_else(bc_region=="Nechako", "North Coast & Nechako", bc_region)
  )|>
  unnest(data)|>
  group_by(bc_region, noc_5=`NOC (821)`)|>
  summarize(across(where(is.numeric), \(x) sum(x, na.rm = TRUE)))|>
  pivot_longer(cols=-c(bc_region, noc_5),
               names_to = "census_column_heading",
               values_to = "employment")|>
  mutate(noc_5 = word(noc_5, 1))|>
  filter(employment>0,
         str_length(noc_5) == 5,
         noc_5!="Total"
  )|>
  inner_join(census_mapping)|>
  group_by(bc_region, lmo_ind_code, lmo_detailed_industry, noc_5)|>
  summarize(employment=sum(employment, na.rm = TRUE))

#CREATE BC FORECAST---------------------------------------

budget <- read_excel(here("data","constraint.xlsx"))|>
  mutate(series="budget forecast",
         cagr=(employment[year==max(year)]/employment[year==min(year)])^(1/(max(year)-min(year)))-1
  )

bc <- lfs|>
  filter(is.na(bc_region) & is.na(noc_5) & lmo_detailed_industry=="Total, All Industries")|>
  group_by(year)|>
  summarize(employment=sum(employment))|>
  mutate(series="LFS")

bc_with_cagr <- bc|>
  mutate(cagr=(employment[year==max(year)]/employment[year==(max(year)-10)])^.1-1)

bc_growth_factor <- lm(log(employment)~year, data=bc)|>
  broom::tidy()|> #the estimated growth factor for this series
  filter(term=="year")|>
  pull(estimate)|>
  exp()

our_forecast <- tibble(year=(max(budget$year)+1):(max(budget$year)+6))|>
  mutate(employment=tail(budget$employment, n=1)*bc_growth_factor^(year-max(budget$year)),
         series="our forecast",
         cagr=NA_real_ #user will be able to alter slope, so delay cagr calculation
  )

bc_forecast <- our_forecast|>
  bind_rows(bc_with_cagr, budget)|>
  arrange(year)

#' BASELINE LFS SHARES: (2022-2024)------------

lfs_base_share <- lfs|>
  filter(year %in% base_years, #note that some NOCs have either missing or zero employment for the base years
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
  mutate(factors=map(data, get_factor, base_plus))|>
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
  mutate(factors=map(data, get_factor, base_plus))|>
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
  mutate(factors=map(data, get_factor, base_plus))|>
  unnest(factors)|>
  select(-data)|>
  rename(occupation_factor=factor)

# LFS forecast shares --------------------------
lfs_shares <- left_join(lfs_base_share, regional_factor)|>
  left_join(industry_factor)|>
  left_join(occupation_factor)|>
  mutate(adjusted_share=base_share*(regional_factor*industry_factor*occupation_factor)^(1/3))|>
  group_by(year)|>
  mutate(lfs_share=adjusted_share/sum(adjusted_share, na.rm = TRUE))|> #make proportions sum to 1
  select(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry, year, lfs_share)

#census by region-----------------------------
census|>
  group_by(bc_region)|>
  summarize(employment=sum(employment))|>
  mutate(year=2021,
         series="census")|>
  write_rds(here("out","census_region.rds"))

#census by industry---------------------------

census|>
  group_by(lmo_ind_code, lmo_detailed_industry)|>
  summarize(employment=sum(employment))|>
  mutate(year=2021,
         series="census")|>
  write_rds(here("out","census_industry.rds"))

#census by occupation--------------------------

census|>
  group_by(noc_5)|>
  summarize(employment=sum(employment))|>
  mutate(year=2021,
         series="census")|>
  write_rds(here("out","census_occupation.rds"))

#calculate census shares-------------------

census_base_share <- census|>
  ungroup()|>
  mutate(base_share=employment/sum(employment))|>
  select(-lmo_detailed_industry)

census_shares <- left_join(census_base_share, regional_factor)|>
  left_join(industry_factor)|>
  left_join(occupation_factor)|>
  mutate(adjusted_share=base_share*(regional_factor*industry_factor*occupation_factor)^(1/3))|>
  group_by(year)|>
  mutate(census_share=adjusted_share/sum(adjusted_share, na.rm = TRUE))|> #make proportions sum to 1
  select(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry, year, census_share)

#write data to disk----------------------

agg_and_save(lfs_no_aggregates, bc_region)
agg_and_save(lfs_no_aggregates, lmo_ind_code, lmo_detailed_industry)
agg_and_save(lfs_no_aggregates, noc_5)

weighted_shares <- full_join(lfs_shares, census_shares)|>
  mutate(across(where(is.numeric), ~replace_na(.x, 0)),
         pre_mod_share=(lfs_share+census_share)/2)|>
  write_rds(here("out", "weighted_shares.rds"))

lfs_shares|>
  rename(pre_mod_share=lfs_share)|>
  write_rds(here("out","lfs_shares.rds"))

census_shares|>
  rename(pre_mod_share=census_share)|>
  write_rds(here("out","census_shares.rds"))

write_rds(bc_forecast, here("out","modified","bc_forecast.rds"))
