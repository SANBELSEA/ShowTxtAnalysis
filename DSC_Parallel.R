library(multidplyr)
library(tidyverse, warn.conflicts = FALSE)
library(readr)

# Detect the number of available clusters
parallel::detectCores()

cluster <- new_cluster(parallel::detectCores()-1) # use at least one less than available to allow for other resource needs
cluster

# define raw data file path and allow library to create the best partitions
path <- FILEPATH
files <- dir(path, full.names = TRUE)
cluster_assign_partition(cluster, files = files)
cluster_send(cluster, descriptors <- readr::read_csv(files))

# read the files onto each worker
descriptors <- party_df(cluster, "descriptors")
descriptors |> rename(TV_SHOW = Title) |> select(-1) |> mutate(across(everything(), ~replace(., .=='NULL', NA))) |>
  group_by(TV_SHOW) |> collect() |> tidyr::fill(Description, .direction = "down") |>
  mutate(Description = if_else(is.na(Description) == TRUE, TV_SHOW, Description)) |>
  ungroup() |> distinct()
