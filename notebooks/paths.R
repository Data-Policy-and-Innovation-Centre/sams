if (interactive()) {
  script_path <- getwd()  # Current working directory
} else {
  script_path <- normalizePath(commandArgs(trailingOnly = FALSE)[grep("--file=", commandArgs(trailingOnly = FALSE))])
  script_path <- sub("--file=", "", script_path)
}
root <- dirname(script_path)
data <- file.path(root,"data")
raw_data <- file.path(data,"raw")