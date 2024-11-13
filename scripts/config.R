packages <- c("tidyverse","rprojroot","knitr", "ggplot2", "arrow", "readxl","openxlsx",
              "kableExtra","here","rmarkdown","prettydoc","ggsankey","reticulate",
              "map","tmap","fs","yaml")

# Loop through the package names and install if not already installed
options(repos = c(CRAN = "https://cran.rstudio.com/"))
for (pkg in packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
}
rm(pkg)
library(yaml)
library(fs)
library(here)

# Load data catalog
here::i_am("README.md")
catalog <- yaml.load_file(file.path(here(),"config","datasets.yaml"))
datasets <- catalog$datasets


get_path <- function(name) {
  file.path(here(),datasets[[name]]$path)
}

# Directory structure
paths <- c()
paths$DATA_DIR = file.path(here(),"data")
paths$RAW_DATA_DIR = file.path(paths$DATA_DIR, "raw")
paths$INTERIM_DATA_DIR = file.path(paths$DATA_DIR, "interim")
paths$PROCESSED_DATA_DIR = file.path(paths$DATA_DIR, "processed")
paths$OUTPUT_DIR = file.path(here(),"output")
paths$TABLES_DIR = file.path(paths$OUTPUT_DIR, "tables")
paths$FIGURES_DIR = file.path(paths$OUTPUT_DIR, "figures")


# Load figure constants
optfig.labfontsize = 20 + 8
optfig.axfontsize = 18 + 8
optfig.legfontsize = 16
optfig.ticklength = -.2
optfig.font = 'Times'
optfig.axisweight = 1
