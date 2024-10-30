rm(list = ls())

# Set the working directory to the script's directory if running via Rscript
install.packages("here")


library("here")
here::i_am("README.md")
source(here("scripts/config.R"))

rmarkdown::render(here("notebooks/4.01-ym-oct2024-report.Rmd"), 
output_file = here("output/report-oct2024.html"))

 