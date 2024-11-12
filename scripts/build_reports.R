rm(list = ls())

# Set the working directory to the script's directory if running via Rscript
options(repos = c(CRAN = "https://cran.rstudio.com/"))
install.packages("here")


library("here")
here::i_am("README.md")
source(here("scripts/config.R"))

rmarkdown::render(here("notebooks/4.04-ym-nov2024-report.Rmd"), 
output_file = here("output/report-nov2024.docx"))

