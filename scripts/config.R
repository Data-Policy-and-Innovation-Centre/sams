packages <- c("tidyverse","RSQLite","rprojroot","knitr",
              "kableExtra","here","rmarkdown","prettydoc","ggsankey","reticulate",
              "map","tmap")

# Loop through the package names and install if not already installed
options(repos = c(CRAN = "https://cran.rstudio.com/"))
for (pkg in packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
}
rm(pkg)

# Load python env
library(reticulate)
use_condaenv("skills", required=TRUE)

# Import the necessary Python standard library
importlib <- import("importlib")

# Configure using python configuration file
config <- import_from_path("sams.config")
importlib$reload(config)

# Load figure constants
optfig.labfontsize = 20 + 8
optfig.axfontsize = 18 + 8
optfig.legfontsize = 16
optfig.ticklength = -.2
optfig.font = 'Times'
optfig.axisweight = 1
