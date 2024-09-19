library(tidyverse)
library(here)
library(vroom)
library(janitor)
library(conflicted)
library(readxl)
conflicts_prefer(dplyr::filter)

rich_vs_stokes <- function(tbbl, var1, var2, cor){
  plt <- ggplot(tbbl,  aes({{  var1  }},
                           {{  var2  }},
                           colour=year,
                           text=paste0(
                             "Region: ",
                             bc_region,
                             "\n Industry: ",
                             lmo_detailed_industry,
                             "\n Year: ",
                             year,
                             "\n Rich: ",
                             scales::comma({{  var1  }}, accuracy = 1),
                             "\n Stokes: ",
                             scales::comma({{  var2  }}, accuracy = 1)
                           )
  )
  )+
    geom_abline(slope = 1, intercept = 0, colour="white", lwd=2)+
    geom_point(size=.5)+
    scale_colour_viridis_c()+
    scale_x_continuous(trans="log10", labels=scales::comma)+
    scale_y_continuous(trans="log10", labels=scales::comma)+
    labs(title=paste("The correlation between the industry/region forecasts is",round(cor,3)))

  plotly::ggplotly(plt, tooltip="text")
}



richs_forecast <- read_rds(here("out","richs_forecast.rds"))

#to compare with stokes demand-----------------------------
sum(richs_forecast$expansion_demand, na.rm = TRUE)
sum(richs_forecast$replacement_demand, na.rm = TRUE)

#aggregate rich's forecast

richs_region_industry <- richs_forecast|>
  group_by(bc_region, lmo_ind_code, lmo_detailed_industry, year)|>
  summarise(richs_employment=sum(employment),
            richs_expansion_demand=sum(expansion_demand),
            richs_replacement_demand=sum(replacement_demand))

richs_region_occupation <- richs_forecast|>
  group_by(bc_region, noc_5, year)|>
  summarise(richs_employment=sum(employment),
            richs_expansion_demand=sum(expansion_demand),
            richs_replacement_demand=sum(replacement_demand))

#correct names---------------------

correct_industries <- read_csv(here("data","mapping", "tidy_2024_naics_to_lmo.csv"))|>
  select(naics, lmo_detailed_industry)|>
  select(lmo_detailed_industry)|>
  distinct()|>
  ungroup()

correct_regions <- richs_region_industry|>
  ungroup()|>
  select(bc_region)|>
  distinct()

#industry-------------------------
stokes_employment_industry <- read_excel(here("data","stokes_data", "employment_industry.xlsx"), skip = 3)|>
  pivot_longer(cols=starts_with("2"), names_to = "year", values_to = "stokes_employment")|>
  clean_names()|>
  ungroup()|>
  rename(lmo_detailed_industry=industry)|>
  fuzzyjoin::stringdist_join(correct_industries)|>
  rename(lmo_detailed_industry=lmo_detailed_industry.y)|>
  select(bc_region=geographic_area, lmo_detailed_industry, year, stokes_employment)

stokes_demand_industry <-  read_excel(here("data","stokes_data", "demand_industry.xlsx"), skip = 3)|>
  pivot_longer(cols=starts_with("2"), names_to = "year")|>
  clean_names()|>
  filter(variable %in% c("Expansion Demand","Replacement Demand"))|>
  pivot_wider(names_from=variable, values_from = value, names_prefix = "stokes", names_sep = "_")|>
  clean_names()|>
  rename(lmo_detailed_industry=industry)|>
  fuzzyjoin::stringdist_join(correct_industries)|>
  rename(lmo_detailed_industry=lmo_detailed_industry.y)|>
  select(bc_region=geographic_area, lmo_detailed_industry, year, contains("stokes"))

stokes_industry <- full_join(stokes_employment_industry, stokes_demand_industry)|>
  filter(bc_region!="British Columbia")|>
  mutate(year=as.numeric(year),
         bc_region=if_else(bc_region=="Mainland South West","Lower Mainland-Southwest", bc_region),
         bc_region=if_else(bc_region=="Vancouver Island Coast","Vancouver Island and Coast", bc_region)
  )|>
  fuzzyjoin::stringdist_full_join(correct_regions)|>
  rename(bc_region=bc_region.y)|>
  select(-bc_region.x)

region_industry <- full_join(richs_region_industry, stokes_industry)|>
  mutate(richs_employment=if_else(is.na(richs_employment), 0, richs_employment),
         richs_expansion_demand=if_else(is.na(richs_expansion_demand) & !is.na(stokes_expansion_demand), 0, richs_expansion_demand),
         richs_replacement_demand=if_else(is.na(richs_replacement_demand) & !is.na(stokes_replacement_demand), 0, richs_replacement_demand))

industry_employment_cor <- cor(region_industry$richs_employment, region_industry$stokes_employment, method = "pearson")
industry_expansion_cor <- cor(region_industry$richs_expansion_demand, region_industry$stokes_expansion_demand, method = "pearson", use="pairwise.complete.obs")
industry_replace_cor <- cor(region_industry$richs_replacement_demand, region_industry$stokes_replacement_demand, method = "pearson", use="pairwise.complete.obs")


rich_vs_stokes(region_industry, richs_employment, stokes_employment, industry_employment_cor)
rich_vs_stokes(region_industry, richs_expansion_demand, stokes_expansion_demand, industry_expansion_cor)
rich_vs_stokes(region_industry, richs_replacement_demand, stokes_replacement_demand, industry_replace_cor)




