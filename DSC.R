library(RODBC)
library(DBI)
library(httr)
library(jsonlite)
library(tidyverse)
library(aws.s3)
library(rowr)

# Create connection with Snowflake database
conn <- odbcConnect(ACC, uid=M, pwd=PW)

sqlQuery(conn, paste0("use role", ROLE))
sqlQuery(conn, paste0("use warehouse", WAREHOUSE))

un <- UN
pw <- PWD
url <- URL

base <- BASE

type <- TYPE
typep <- TYPEP
showID <- SID
progID <- PID
progID <- PID
series <- SER
api <- KEY

seriestmsid <- sqlQuery(conn, "SELECT distinct (series_tms_id), show_name FROM DB.SCH.TBL1")
gn_desc_sno <- sqlQuery(conn, "select distinct TV_SHOW from DB.SCH.TBL2")
seriestmsid3 <- seriestmsid %>% rename(TV_SHOW = SHOW_NAME) %>% anti_join(gn_desc_sno)
sertmsNoid <- paste0("'",as.vector(seriestmsid3 %>% filter(SERIES_TMS_ID == 0 | is.na(SERIES_TMS_ID)) %>% select(2) %>% mutate(TV_SHOW = gsub("'","''",TV_SHOW)) %>% t()),"'")
sertmsid <-  seriestmsid3 %>% mutate_if(is.integer, function(x) ifelse(is.na(x), 0, x)) %>% filter(SERIES_TMS_ID > 0)

# Get Shows with no tms id's and query vizio_content_ips table (limited to 16384 shows in snowflake)
missing_tms1 <- sqlQuery(conn, paste0("select distinct content_tms_id, show_name from DB.SCH.TBL1
where show_name IN (", paste(sertmsNoid, collapse = ", "),")")) %>% rename(CONTENT_TITLE = SHOW_NAME)

# Copy in all from last month
last <- do.call("bind_rows", lapply(dir(FILEPATH, full.names = TRUE), read.csv)) %>% select(Title) %>% rename(TV_SHOW = Title) %>% distinct()
prev <- gn_desc_sno %>% anti_join(last, by = 'TV_SHOW')
# sertmsid2 <- sertmsid %>% rename(CONTENT_TMS_ID = SERIES_TMS_ID, CONTENT_TITLE = TV_SHOW) %>%
#   mutate(CONTENT_TMS_ID = as.factor(CONTENT_TMS_ID)) %>%
#   bind_rows(missing_tms1) %>% group_by(CONTENT_TITLE) %>% top_n(-1) %>% ungroup() %>% distinct()

sertmsid <- as.vector(prev %>% slice(1001:2001) %>% select(1) %>% t())


trans_jsonD2 <- list()
call2 <- list()
get_show_text2 <- list()
get_show2 <- list()

for(t in 1:1000){
  call2[[t]] <- paste(base, typep, sertmsid[[t]], api, sep = "")
  get_show2[[t]] <- GET(call2[[t]])
  get_show_text2[[t]] <- content(get_show2[[t]], as = "text")
  trans_jsonD2[[t]] <- (fromJSON(get_show_text2[[t]], flatten = T))
  print(t)
  date_time<-Sys.time()
  while((as.numeric(Sys.time()) - as.numeric(date_time))<1.1){} #dummy while loop
}

Titles <- list()
Genres <- list()
Moods <- list()
Themes <- list()
Shows <- list()
Description <- list()

# Transfer the json values to a master list called 'Shows'
for(df in 1:length(trans_jsonD2)){
  Titles[[df]] <- trans_jsonD2[[df]]$title %>% as_tibble()
  Titles[[df]] <- if(nrow(Titles[[df]]) >= 1){Titles[[df]]} else{Titles[[df]] <- tibble(value = "NULL")}
  Genres[[df]] <- trans_jsonD2[[df]]$genres %>% as_tibble()
  Genres[[df]] <- if(nrow(Genres[[df]]) >= 1){Genres[[df]]} else{Genres[[df]] <- tibble(value = "NULL")}
  Moods[[df]] <- trans_jsonD2[[df]]$keywords$Mood %>% as_tibble()
  Moods[[df]] <- if(nrow(Moods[[df]]) >= 1){Moods[[df]]} else{Moods[[df]] <- tibble(value = "NULL")}
  Themes[[df]] <- trans_jsonD2[[df]]$keywords$Theme %>% as_tibble()
  Themes[[df]] <- if(nrow(Themes[[df]]) >= 1){Themes[[df]]} else{Themes[[df]] <- tibble(value = "NULL")}
  Description[[df]] <- trans_jsonD2[[df]]$longDescription %>% as_tibble()
  Description[[df]] <- if(nrow(Description[[df]]) >= 1){Description[[df]]} else{Description[[df]] <- tibble(value = "NULL")}
  Shows[[df]] <- Titles[[df]] %>% rename(Title = value) %>%
    cbind.fill(Genres[[df]]) %>% rename(Genre = value) %>%
    cbind.fill(Moods[[df]]) %>% rename(Mood = value) %>%
    cbind.fill(Themes[[df]], fill = "NULL") %>% rename(Theme = value) %>%
    cbind.fill(Description[[df]], fill = "NULL") %>% rename(Description = value)
}

Final <- bind_rows(Shows)

write.csv(Final, paste0("Data/longDescription_Prev", Sys.Date(), ".csv"))


## Connecting to S3
Access <- Sys.setenv("AWS_ACCESS_KEY_ID" = AWS_ID,
                     "AWS_SECRET_ACCESS_KEY" = AWS_SEC,
                     "AWS_DEFAULT_REGION" = AWS_REG)

# write to S3
zz <- rawConnection(raw(0), "r+")
readr::write_csv(Final %>% select(1:4), zz)
name <- paste("Dsc_Data",Sys.Date(),".csv", sep ="")
put_object(rawConnectionValue(zz), object = paste(BUCKET,name))
# close the connection
close(zz)#dummy while loop
# Save Image to S3
s3save_image(paste0("Dsc31k", Sys.Date(), ".RData"), bucket = BUCKET, opts = NULL)


# Remove all data from the Global Environment except the raw target ID's and the scrape reults.
toremove <- grep("^sertmsid2|^trans_jsonD2$", ls(),
                 invert = TRUE,
                 value = TRUE)
rm(list = c(toremove, "toremove"))

