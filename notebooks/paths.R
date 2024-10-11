packages <- c("tidyverse","RSQLite","rprojroot","knitr",
              "kableExtra","here","rmarkdown","prettydoc","ggsankey")

# Loop through the package names and install if not already installed
options(repos = c(CRAN = "https://cran.rstudio.com/"))
for (pkg in packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
}
rm(pkg)


library(here)


# Load paths
here::i_am("README.md")
root <- here()
data <- file.path(root,"data")
raw_data <- file.path(data,"raw")

# Load figure constants
optfig.labfontsize = 20 + 8
optfig.axfontsize = 18 + 8
optfig.legfontsize = 16
optfig.ticklength = -.2
optfig.font = 'Times'
optfig.axisweight = 1
