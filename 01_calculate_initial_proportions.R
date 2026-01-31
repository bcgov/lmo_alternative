#libraries-----------------------
tictoc::tic()
library(tidyverse)
library(here)
library(vroom)
library(janitor)
library(readxl)
library(fpp3)
library(conflicted)
conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::lag)
conflicts_prefer(vroom::cols)
conflicts_prefer(vroom::col_double)
conflicts_prefer(vroom::col_character)
#constants---------------------
census_weight <- 0.789473684 #long form 2021 census covered 4.8M workers, NINE years of LFS covered 1.28M (distinct) workers.
base_years <- c(2017:2025) #LFS years used, centered on census 2021. (add 1 year on both sides to keep centered on 2021, until 2026 census available...  2028 forecast???)
base_plus <- 5:15 #base year 2021, so forecast starts some years later.(need to increment start and end each year)
bc_emp_rate <- .81 #the proportion of 15-64 year olds that are employed.
#functions-----------------------
source(here("..","shared_functions", "pond_utilities.R"))

get_region <- function(path) {
  x <- basename(path)
  x <- tools::file_path_sans_ext(x)
  sub("^.*_", "", x)
}

regress <- function(tbbl) {
  max_year <- max(tbbl$year)
  year_term <- lm(log(employment+1) ~ year, data=tbbl)|>
    broom::tidy()|>
    filter(term=="year")
  data=tibble(year=(max_year+1):(max_year+11))
  tibble(slope=year_term$estimate, var=year_term$std.error^2, forecast=list(data))
}

exponentiate <- function(tbbl, growth_factor){
  tbbl|>
    mutate(multiplier=growth_factor^base_plus)
}

agg_and_save <- function(tbbl, var1, var2=NULL){
  tbbl|>
    group_by(year, {{  var1  }}, {{  var2  }})|>
    summarize(employment=sum(employment))|>
    mutate(series="historic")|>
    write_rds(here("out",paste0("historic_",as.character(substitute(var1)),".rds")))
}

get_size <- function(tbbl){
  mean(tbbl$employment)
}

#read in mapping files----------------------
mapping <- read_excel(resolve_current("industry_mapping_with_stokes_agg.xlsx"))|>
  select(naics_5, lmo_detailed_industry, lmo_ind_code)

lmo_nocs <-read_csv(resolve_current("noc_descriptions.csv"))|>
  select(lmo_noc)|>
  distinct()|>
  mutate(lmo_noc=as.character(lmo_noc),
         lmo_noc=str_pad(lmo_noc, "0", side = "left", width=5))

lmo_nocs <- bind_rows(lmo_nocs, tibble(lmo_noc=NA_character_)) #add in the NA so we keep the aggregate

census_mapping <- read_excel(resolve_current("mapping_census_to_lmo_63.xlsx"))

#read in the raw data----------------------

lfs_files <- c(resolve_current("stat0005p1.csv"),
               resolve_current("stat0005p2.csv"),
               resolve_current("stat0610p1.csv"),
               resolve_current("stat0610p2.csv"),
               resolve_current("stat1115p1.csv"),
               resolve_current("stat1115p2.csv"),
               resolve_current("stat1620p1.csv"),
               resolve_current("stat1620p2.csv"),
               resolve_current("stat2125p1.csv"),
               resolve_current("stat2125p2.csv")
               )


lfs <- vroom(lfs_files,
             col_types = cols(SYEAR = col_double(),
                              BC_REGION = col_character(),
                              NAICS_5 = col_character(),
                              NOC_5 = col_character(),
                              `_COUNT_` = col_double()))|>
  clean_names()|>
  rename(year=syear,
         employment=count)|>
  mutate(employment=employment/12,
         noc_5=if_else(noc_5 %in% c("00011", "00012", "00013", "00014", "00015"), "00018", noc_5))|>
  filter(!is.na(year))|># & year<max(year, na.rm=TRUE))|> #typically we want to disregard partial year at end.
  inner_join(mapping)|>
  group_by(year, bc_region, noc_5, lmo_ind_code, lmo_detailed_industry)|>
  summarize(employment=sum(employment))

# allocate noc 41229 across 41220 and 41221--------------------------

reallocated <- lfs|>
  filter(noc_5 %in% c(41220, 41221, 41229))|>
  pivot_wider(names_from = noc_5, values_from = employment)|>
  mutate(prop_41220= `41220`/(`41220`+`41221`),
         prop_41221= `41221`/(`41220`+`41221`),
         `41220`=`41220`+ `41229`*prop_41220,
         `41221`=`41221`+`41229`*prop_41221)|>
  select(-`41229`, -prop_41220, -prop_41221)|>
  pivot_longer(cols=c(`41220`, `41221`),
               names_to = "noc_5",
               values_to = "employment")|>
  filter(employment>0)

lfs <- lfs|>
  filter(!noc_5 %in% c(41220, 41221, 41229))|>
  bind_rows(reallocated)

lfs_no_aggregates <- lfs|>
  filter(!is.na(bc_region),
         !is.na(noc_5),
         lmo_detailed_industry!="Total, All Industries"
  )|>
  tsibble(index=year, key=c(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry))|>
  tsibble::fill_gaps(employment=0, .full=TRUE)|>
  tibble()

write_rds(lfs_no_aggregates, here("out","lfs_no_aggregates.rds"))

#census data-----------------------------

census_files <- c(resolve_current("census_Lower Mainland-Southwest.csv"),
                  resolve_current("census_Vancouver Island and Coast.csv"),
                  resolve_current("census_Thompson-Okanagan.csv"),
                  resolve_current("census_Kootenay.csv"),
                  resolve_current("census_Cariboo.csv"),
                  resolve_current("census_Nechako.csv"),
                  resolve_current("census_North Coast.csv"),
                  resolve_current("census_Northeast.csv")
                  )


census <- tibble(paths=census_files)|>
  mutate(data=map(paths, vroom),
         bc_region=get_region(paths)
         )|>
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

budget <- read_excel(resolve_current("constraint.xlsx"))|>
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

#' we are going to use working age population to form our employment forecast that
#' extends beyond the finance forecast.

our_range <- (max(budget$year)+1):(max(budget$year)+6)

our_forecast <- read_csv(resolve_current("Population_Projections.csv"))|>
  filter(Region.Name=="British Columbia",
         Gender=="T")|>
  select(-contains("region"), -Type, -Gender, -Total)|>
  pivot_longer(cols=-Year, names_to = "age", values_to = "employment")|>
  filter(age %in% 15:64,
         Year %in% our_range
  )|>
  group_by(year=Year)|>
  summarize(employment=sum(employment)*bc_emp_rate)|>
  mutate(series="our forecast",
         cagr=NA_real_) #user will be able to alter, so delay cagr calculation

#bc_slope serves as our prior for the empirical baysian shrinkage
bc_slope <- lm(log(employment)~year, data=bc)|>
  broom::tidy()|>
  filter(term=="year")|>
  pull(estimate)

bc_forecast <- our_forecast|>
  bind_rows(bc_with_cagr, budget)|>
  arrange(year)

#' BASELINE LFS SHARES: (for base_years) ------------

lfs_base_share <- lfs|>
  filter(year %in% base_years,
         !is.na(bc_region),
         !is.na(noc_5),
         lmo_detailed_industry!="Total, All Industries"
         )|>
  group_by(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry)|>
  summarize(employment=sum(employment))|> #sum treats implicit missing employment same as 0s
  ungroup()|>
  mutate(base_share=employment/sum(employment))|>
  filter(base_share>0)|>
  arrange(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry)

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
  mutate(regress=map(data, regress))|>
  select(-data)|>
  unnest(regress)|>
  ungroup()|>
  mutate(bc_slope=bc_slope,
         tau2=var(slope-bc_slope),
         weight=tau2 / (tau2 + var),
         shrunk_slope = weight * slope + (1 - weight) * bc_slope,
         growth_factor=exp(shrunk_slope)
  )|>
  select(bc_region, growth_factor, forecast)|>
  mutate(forecast=map2(forecast, growth_factor, exponentiate))|>
  select(-growth_factor)|>
  unnest(forecast)|>
  rename(regional_factor=multiplier)

#INDUSTRY GROWTH FACTORS---------------
industry_factor <- lfs|>
  filter(is.na(bc_region),
         is.na(noc_5),
         lmo_detailed_industry!="Total, All Industries"
         )|>
  group_by(lmo_ind_code, lmo_detailed_industry)|>
  select(-bc_region, -noc_5)|>
  nest()|>
  mutate(regress=map(data, regress))|>
  unnest(regress)|>
  select(-data)|>
  ungroup()|>
  mutate(bc_slope=bc_slope,
         tau2=var(slope-bc_slope),
         weight=(tau2 / (tau2 + var)),
         shrunk_slope = weight * slope + (1 - weight) * bc_slope,
         growth_factor=exp(shrunk_slope)
  )|>
  select(contains("lmo"), growth_factor, forecast)|>
  mutate(forecast=map2(forecast, growth_factor, exponentiate))|>
  select(-growth_factor)|>
  unnest(forecast)|>
  rename(industry_factor=multiplier)

#OCCUPATION GROWTH FACTORS---------------------------------
for_shrinkage_plot <- lfs|>
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
  mutate(regress=map(data, regress),
         size=map_dbl(data, get_size)
         )|>
  select(-data)|>
  unnest(regress)|>
  ungroup()|>
  mutate(bc_slope=bc_slope,
         tau2=var(slope-bc_slope),
         weight=(tau2 / (tau2 + var)),
         shrunk_slope = weight * slope + (1 - weight) * bc_slope,
         growth_factor=exp(shrunk_slope)
  )

for_shrinkage_plot|>
  select(noc_5, size, contains("slope"))|>
  write_rds(here("out", "for_shrinkage_plot.rds"))

occupation_factor <- for_shrinkage_plot|>
  select(noc_5, growth_factor, forecast)|>
  mutate(forecast=map2(forecast, growth_factor, exponentiate))|>
  select(-growth_factor)|>
  unnest(forecast)|>
  rename(occupation_factor=multiplier)

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

#census by industry:region---------------------

census|>
  group_by(bc_region, lmo_ind_code, lmo_detailed_industry)|>
  summarize(employment=sum(employment))|>
  mutate(year=2021,
         series="census")|>
  write_rds(here("out","census_industry_region.rds"))


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
         pre_mod_share=(1-census_weight)*lfs_share+census_weight*census_share)

write_rds(weighted_shares, here("out","modified","shares.rds"))
write_rds(bc_forecast, here("out","modified","bc_forecast.rds"))
tictoc::toc()
