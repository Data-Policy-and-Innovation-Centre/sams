rm(list = ls())

# Set the working directory to the script's directory if running via Rscript
options(repos = c(CRAN = "https://cran.rstudio.com/"))
install.packages("here")


library("here")
here::i_am("README.md")
source(here("scripts/config.R"))

# Nov 2024 -- PPT
rmarkdown::render(here("notebooks/4.05-ym-nov2024-presentation.Rmd"), 
output_file = here("output/ppt-nov2024.pptx"))


# Nov 2024 -- Report
rmarkdown::render(here("notebooks/4.04-ym-nov2024-report.Rmd"), 
output_file = here("output/report-nov2024.docx"))

