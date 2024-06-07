library(tidyverse)
library(here)
library(vroom)

no_nas <- lfs|>
  filter(!is.na(bc_region),
         !is.na(noc_5)
         )|>
  tsibble(index=year, key=c(bc_region, noc_5, lmo_ind_code, lmo_detailed_industry))|>
  tsibble::fill_gaps(employment=0, .full=TRUE)

all_data <- no_nas|>
  tibble()|>
  mutate(series="LFS")|>
  bind_rows(employment_forecast)|>
  arrange(bc_region, noc_5, lmo_ind_code, year)

#write_csv(all_data, here("out","all_data.csv"))
#all_data <- vroom(here("out","all_data.csv"))

#regional----------

all_data|>
  na.omit()|>
  group_by(year, bc_region, series)|>
  summarize(employment=sum(employment))|>
  ggplot(aes(year, employment, fill=series))+
  geom_col()+
  facet_wrap(~bc_region, scales = "free_y")










