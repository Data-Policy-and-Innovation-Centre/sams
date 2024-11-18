packages <- c("tidyverse","rprojroot","knitr", "ggplot2", "arrow", "readxl","openxlsx",
              "kableExtra","here","rmarkdown","prettydoc","ggsankey","reticulate",
              "map","tmap","fs","yaml", "stargazer", "webshot")

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
library(webshot)
library(knitr)

if (!webshot::is_phantomjs_installed()) {
  webshot::install_phantomjs()
}

# Load data catalog
here::i_am("README.md")
catalog <- yaml.load_file(file.path(here(),"config","catalog.yaml"))
datasets <- catalog$datasets
exhibits <- catalog$exhibits

get_path <- function(name) {
  file.path(here(),datasets[[name]]$path)
}

get_exhibit <- function(name) {
  file.path(here(),exhibits[[name]]$path)
}

# Directory structure
paths <- c()
paths$DATA_DIR <- file.path(here(),"data")
paths$RAW_DATA_DIR <- file.path(paths$DATA_DIR, "raw")
paths$INTERIM_DATA_DIR <- file.path(paths$DATA_DIR, "interim")
paths$PROCESSED_DATA_DIR <- file.path(paths$DATA_DIR, "processed")
paths$OUTPUT_DIR <- file.path(here(),"output")
paths$TABLES_DIR <- file.path(paths$OUTPUT_DIR, "tables")
paths$FIGURES_DIR <- file.path(paths$OUTPUT_DIR, "figures")


# Load figure constants
optfig <- c()
optfig$labfontsize <- 20 + 8
optfig$axfontsize <- 18 + 8
optfig$legfontsize <- 16
optfig$ticklength <- -.2
optfig$font <- "Times"
optfig$axisweight <- 1

# Print tables to word
print_table_to_word <- function(html_table) {
  # Write the HTML table to a temporary file
  html_file <- tempfile(fileext = ".html")
  writeLines(html_table, html_file)

  # Capture the HTML as an image using webshot
  img_file <- tempfile(fileext = ".png")
  webshot::webshot(html_file, file = img_file, selector = "table")

  # Include the image in the Word document
  knitr::include_graphics(img_file)
}
