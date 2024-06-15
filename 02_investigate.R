library(tidyverse)
library(here)
library(vroom)
library(fpp3)
library(conflicted)
conflicts_prefer(vroom::cols)
conflicts_prefer(vroom::col_character)
conflicts_prefer(vroom::col_double)
conflicts_prefer(dplyr::filter)
#functions------------------
ets_forecast <- function(tsbbl, horizon){
  tsbbl |>
    model(ETS(forecast_var))|>
    forecast(h = horizon)|>
    tibble()|>
    select(-forecast_var, -.model)|>
    rename(forecast_var=.mean)
}
#read in the data---------------------------
pre_mod_shares <- vroom(here("out","pre_mod_shares.csv"),
                        col_types = cols(
                          bc_region = col_character(),
                          noc_5 = col_character(),
                          lmo_ind_code = col_character(),
                          lmo_detailed_industry = col_character(),
                          base_share = col_double(),
                          year = col_double(),
                          regional_factor = col_double(),
                          industry_factor = col_double(),
                          occupation_factor = col_double(),
                          adjusted_share = col_double(),
                          final_share = col_double()
                        ))
top_historic <- read_csv(here("out","top_historic.csv"))
no_aggregates <- vroom(here("out","no_aggregates.csv"))
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

employment_forecast <- pre_mod_shares|>
  full_join(top|>select(-series))|>
  mutate(employment=final_share*employment,
         series="forecast")|>
  select(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry, year, employment, series)

all_data <- no_aggregates|>
  tibble()|>
  mutate(series="LFS")|>
  bind_rows(employment_forecast)|>
  arrange(bc_region, noc_5, lmo_ind_code, year)

# a subset of the data for testing----------------------

small <- all_data|>
  filter(noc_5 %in% c("64100","65201","60020"),
         lmo_ind_code %in% c("ind25","ind59","ind11"),
         bc_region %in% c("Lower Mainland-Southwest",
                          "Vancouver Island and Coast",
                          "Thompson-Okanagan"))

write_csv(small, here("out","small.csv"))

#regional----------

small|>
  group_by(year, bc_region, series)|>
  summarize(employment=sum(employment))|>
  ggplot(aes(year, employment, colour=series))+
  geom_line()+
  scale_y_continuous(labels = scales::comma)+
  facet_wrap(~fct_reorder(bc_region, employment, .desc = TRUE), scales = "free_y")+
  labs(x=NULL,y=NULL, title="Employment by Region")+
  expand_limits(y = 0)+
  theme_minimal()

#industry--------------------------------

small|>
  group_by(year, lmo_ind_code, series)|>
  summarize(employment=sum(employment))|>
  ggplot(aes(year, employment, colour=series))+
  geom_line()+
  scale_y_continuous(labels = scales::comma)+
  facet_wrap(~fct_reorder(lmo_ind_code, employment, .desc = TRUE), scales = "free_y")+
  labs(x=NULL,y=NULL, title="Employment by Industry")+
  expand_limits(y = 0)+
  theme_minimal()+
  theme(text=element_text(size=8))

# 260 largest occupations-----------------------

largest_occ <- small|>
  group_by(year, noc_5, series)|>
  summarize(employment=sum(employment))|>
  group_by(noc_5)|>
  mutate(total=sum(employment))|>
  group_by(noc_5, total)|>
  arrange(desc(total))|>
  nest()|>
  ungroup()|>
  mutate(prop=total/sum(total),
         cumsum_prop=cumsum(prop)) #coverage of top occupations

largest_occ|>
  slice_max(total, n=260)|>
  unnest(cols = c(data))|>
  ggplot(aes(year, employment, colour=series))+
  geom_line()+
  scale_y_continuous(labels = scales::comma)+
  facet_wrap(~fct_reorder(noc_5,total, .desc = TRUE), scales = "free_y")+
  labs(x=NULL,y=NULL, title="The top 260 occupations (that make up over 90% of the labour market)")+
  expand_limits(y = 0)+
  theme_minimal()+
  theme(text=element_text(size=8),
        axis.text.x=element_blank(),
        axis.ticks.x=element_blank(),
        axis.text.y=element_blank(),
        axis.ticks.y=element_blank()
        )

