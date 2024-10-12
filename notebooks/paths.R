packages <- c("tidyverse","RSQLite","rprojroot","knitr",
              "kableExtra","here","rmarkdown","prettydoc","ggsankey","reticulate")

# Loop through the package names and install if not already installed
options(repos = c(CRAN = "https://cran.rstudio.com/"))
for (pkg in packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
}
rm(pkg)


library(here)
library(reticulate)


# Load paths
here::i_am("README.md")
root <- here()
config <- import_from_path("config", path = file.path(root, "sams", "config.py"))

# Load figure constants
optfig.labfontsize = 20 + 8
optfig.axfontsize = 18 + 8
optfig.legfontsize = 16
optfig.ticklength = -.2
optfig.font = 'Times'
optfig.axisweight = 1
