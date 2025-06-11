#' Take the proportions, apply them to the top level employment, then calculate expansion and replacement demand
library(tidyverse)
library(here)
library(vroom)
library(janitor)
library(conflicted)
conflicts_prefer(dplyr::filter)
#constants--------------------------------
replacement_adjustment <- 14.25 #increase this to make replacement demand go down.
#read in data-----------------
mapping <- read_csv(here("data","mapping", "tidy_2024_naics_to_lmo.csv"))|>
  select(naics, lmo_detailed_industry)
mod_shares <- read_rds(here("out","modified", "shares.rds")) #the shares after adjustment
bc_fcast <- read_rds(here("out","modified", "bc_forecast.rds")) #the top level forecast after adjustment

# start by calculating employment and difference in employment----------------
emp_and_diff <- left_join(mod_shares, bc_fcast, by = join_by(year))|>
  mutate(employment=employment*pre_mod_share)|>
  select(-pre_mod_share, -series)|>
  group_by(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry)|>
  mutate(emp_diff=c(NA_real_, diff(employment)))

#'calculate the proportions used to calculate replacement demand------------------------------
#'replacement demand assumed to depend on the proportion of workers that are old (age>50)
region_replace_prop <- vroom(here("data",list.files(here("data"), pattern = "agereg")))|>
  clean_names()|>
  mutate(bc_region=if_else(is.na(bc_region), "British Columbia", bc_region))|>
  na.omit()|>
  group_by(bc_region)|>
  mutate(region_prop=count/sum(count))|>
  filter(age_group=="old")|>
  select(bc_region, region_prop)|>
  transmute(region_replace_prop=region_prop/replacement_adjustment) #ad hoc transform to match stokes for BC as a whole

noc_replace_prop <- vroom(here("data", list.files(here("data"), pattern = "agenoc")))|>
  clean_names()|>
  mutate(noc_5=if_else(noc_5 %in% c("00011", "00012", "00013", "00014", "00015"), "00018", noc_5))|>
  group_by(noc_5, age_group)|>
  summarize(count=sum(count, na.rm = TRUE))|>
  na.omit()|>
  group_by(noc_5)|>
  mutate(noc_prop=count/sum(count))|>
  filter(age_group=="old")|>
  select(noc_5, noc_prop)|>
  transmute(noc_replace_prop=noc_prop/replacement_adjustment) #same transform


industry_replace_prop <- vroom(here("data",list.files(here("data"), pattern = "agenaics")))|>
  clean_names()|>
  na.omit()|>
  mutate(naics_5=as.numeric(naics_5))|>
  left_join(mapping, by=c("naics_5"="naics"))|>
  group_by(lmo_detailed_industry, age_group)|>
  summarise(count=sum(count))|>
  group_by(lmo_detailed_industry)|>
  mutate(industry_prop=count/sum(count))|>
  filter(age_group=="old")|>
  select(lmo_detailed_industry, industry_prop)|>
  transmute(industry_replace_prop=industry_prop/replacement_adjustment) #same transform

replace_prop <- crossing(region_replace_prop, industry_replace_prop, noc_replace_prop)|>
  mutate(replace_prop=(region_replace_prop+industry_replace_prop+noc_replace_prop)/3)|>
  select(bc_region, lmo_detailed_industry, noc_5, replace_prop)

#' expansion demand is the change in employment, scaled up to account for "normal" unemployment
#' i.e. expansion demand is the change in employment multiplied by the ratio (employed+normal unemployed)/employed

region_expand_ratio <- vroom(here("data",
                    "employed_unemployed",
                    list.files(here("data","employed_unemployed"),
                               pattern = "eureg")))|>
  clean_names()|>
  mutate(bc_region=if_else(is.na(bc_region),"British Columbia", bc_region))|>
  filter(lf_stat %in% c("Employed", "Unemployed"))|>
  group_by(bc_region)|>
  mutate(total=sum(count),
         region_ratio=total/count)|>
  filter(lf_stat=="Employed")|>
  select(bc_region, region_ratio)

#occupation factors-------------------------
occupation_expand_ratio <- vroom(here("data",
                    "employed_unemployed",
                    list.files(here("data","employed_unemployed"),
                               pattern = "eunoc")))|>
  clean_names()|>
  mutate(noc_5=if_else(noc_5 %in% c("00011", "00012", "00013", "00014", "00015"), "00018", noc_5))|>
  group_by(noc_5, lf_stat)|>
  summarize(count=sum(count, na.rm = TRUE))|>
  na.omit()|>
  filter(lf_stat %in% c("Employed","Unemployed"))|>
  group_by(noc_5)|>
  mutate(total=sum(count),
         noc_ratio=total/count)|>
  filter(lf_stat=="Employed",
         count>0)|>
  select(noc_5, noc_ratio)
#industry factors--------------------------
industry_expand_ratio<- vroom(here("data",
                        "employed_unemployed",
                        list.files(here("data","employed_unemployed"),
                                   pattern = "eunaics")))|>
  clean_names()|>
  filter(lf_stat %in% c("Employed","Unemployed"))|>
  mutate(naics_5=as.numeric(naics_5))|>
  left_join(mapping, by=c("naics_5"="naics"))|>
  na.omit()|>
  group_by(lf_stat, lmo_detailed_industry)|>
  summarize(count=sum(count))|>
  group_by(lmo_detailed_industry)|>
  mutate(total=sum(count),
         industry_ratio=total/count)|>
  filter(lf_stat=="Employed")|>
  select(lmo_detailed_industry, industry_ratio)

expand_ratio <- crossing(industry_expand_ratio, region_expand_ratio, occupation_expand_ratio)|>
  mutate(expansion_demand_factor=(industry_ratio*region_ratio*noc_ratio)^(1/3))|>#assume factors are independent
  select(bc_region, lmo_detailed_industry, noc_5, expansion_demand_factor)

#calculate expansion and replacement demand-------------------------

richs_forecast <- left_join(emp_and_diff, expand_ratio)|>
  mutate(expansion_demand=emp_diff*expansion_demand_factor)|>
  left_join(replace_prop)|>
  mutate(replacement_demand=employment*replace_prop,
         replacement_demand=if_else(year==min(year), NA_real_, replacement_demand))|>
  select(bc_region, noc_5,
         lmo_ind_code,
         lmo_detailed_industry,
         year,
         employment,
         expansion_demand,
         replacement_demand)

write_rds(richs_forecast, here("out","richs_forecast.rds"))
