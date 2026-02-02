#' This script takes the modified shares from the adjust proportions app,
#' and imposes the driver forecasts as the last step in the employment forecast
#' process.

library(tidyverse)
library(here)
library(readxl)
library(janitor)
library(bcgovpond)
library(conflicted)

bc_forecast <- read_rds(here("out", "modified", "bc_forecast.rds"))
modified_shares <- read_rds(here("out", "modified", "shares.rds"))

industry_driver_shares <- read_excel(resolve_current("driver.xlsx"))|>
  pivot_longer(cols=starts_with("2"), names_to = "year")|>
  clean_names()|>
  rename(lmo_detailed_industry=ind_des)|>
  mutate(year=as.numeric(year)+1,
         value=value*1000)|>
  left_join(bc_forecast)|>
  mutate(share=value/employment)|>
  select(lmo_detailed_industry, year, share)

modified_industry_shares <- modified_shares|>
  group_by(lmo_ind_code, lmo_detailed_industry, year)|>
  summarize(original_share=sum(pre_mod_share))

industry_driver_factors <- inner_join(industry_driver_shares, modified_industry_shares)|>
  mutate(scale_by=share/original_share)|>
  select(lmo_ind_code, year, scale_by)

shares_post_driver <- left_join(modified_shares,
                                industry_driver_factors,
                                by = join_by(lmo_ind_code, year))|>
  mutate(scale_by=if_else(is.na(scale_by), 1, scale_by),
         post_mod_share=pre_mod_share*scale_by,
         post_mod_share=post_mod_share/sum(post_mod_share) #normalize
         )

#some sanity checks-------------------------------
#Does incorporating driver forecast change shares?
with(shares_post_driver, cor(pre_mod_share, post_mod_share))

#Are both the pre and post mod shares normalized (probs sum to one)
shares_post_driver|>
  group_by(year)|>
  summarize(pre_mod_share=sum(pre_mod_share),
            post_mod_share=sum(post_mod_share))

#How badly does the normalization affect the driver forecasts?
temp <- shares_post_driver|>
  group_by(lmo_detailed_industry, year)|>
  summarize(post_mod_share=sum(post_mod_share))|>
  right_join(industry_driver_shares)
with(temp, cor(post_mod_share, share))

#write driver adjusted shares to disk

shares_post_driver|>
  select(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry, year, post_mod_share)|>
  write_rds(here("out","modified", "shares_post_driver.rds"))


