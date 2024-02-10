library(tidyverse)
library(aws.s3)
library(tensorflow)
library(keras)
library(jsonlite)
library(rowr)

install_tensorflow()
install_keras()

## Connecting to S3
Access <- Sys.setenv("AWS_ACCESS_KEY_ID" = AWS_ID,
                     "AWS_SECRET_ACCESS_KEY" = AWS_SEC,
                     "AWS_DEFAULT_REGION" = AWS_REG)

# Read object from S3 to dataframe
b <- BUCKET
get_bucket(b)
s3_path <- 's3://'
e <- new.env()

s3load(object=OBJECT, bucket=b, envir = e)
ls(e)
df <- get_bucket_df(bucket = b)

s3read_using(FUN = load, object=OBJECT, bucket=b)

  # Create lists for each aspect of the descriptor data
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

# Create a master dataframe for Shows
Showsdf <- bind_rows(Shows) %>% mutate(Description = na_if(Description, "NULL")) %>% group_by(Title) %>%
  fill(Description, .direction = 'down') %>% ungroup()

# Extract all Shows with no Genre
NewGenre <- Showsdf %>% filter(Genre == 'NULL' & Title != 'NULL' & !is.na(Description))

# Extract all Shows with no Genre
TestGenre <- Showsdf %>% filter(Genre != 'NULL' & Title != 'NULL' & !is.na(Description))

# Split into training & test sets
training_id <- sample.int(nrow(TestGenre), size = nrow(TestGenre)*0.8)
training <- TestGenre[training_id,]
testing <- TestGenre[-training_id,]

# Import all csv documents, clean and remove null titles
filePath <- "C:/GN_Missing_Descriptors/MissingDescriptors/Data/"
file_names <- "longDescription2021-05-17.csv"#dir(filePath)
files <- paste0(filePath, file_names)
longDesc <- do.call(rbind,lapply(files,read.csv)) %>% mutate(across(where(is.character), ~na_if(., "NULL"))) %>%
  group_by(Title) %>% fill(Description, .direction = 'down') %>% select(2:6) %>% mutate(Description = gsub("[[:punct:]]", "", Description)) %>%
  mutate(Description = tolower(Description)) %>% mutate(Description = str_squish(str_replace_all(Description, "[^[A-Za-z,]]", " "))) %>%
  filter(!is.na(Title))

# Subset the data into two dfs, 1 = Null Genres, 2 = No Null Genres, also we can't have null descriptions
NullGen <- longDesc %>% filter(is.na(Genre)) %>% filter(!is.na(Description)) %>% as_tibble() %>% select(1,2,5)
#NullGen %>% unnest_tokens(word, Description) %>% anti_join(stop_words) %>% pivot_longer(names_from = "Title", values_from = "word")
NoNullGen <- longDesc %>% filter(!is.na(Genre)) %>% filter(!is.na(Description)) %>% as_tibble() %>% select(1,2,5)

# Extract the description and tokenize
text <- NoNullGen$Description
max_features <- 1000
tokenizer <- text_tokenizer(num_words = max_features)

tokenizer %>%
  fit_text_tokenizer(text)
tokenizer$document_count
tokenizer$word_index %>%
  head()

text_seqs <- texts_to_sequences(tokenizer, text)

text_seqs %>%
  head()

# Set parameters:
maxlen <- 100
batch_size <- 32
embedding_dims <- 50
filters <- 64
kernel_size <- 3
hidden_dims <- 50
epochs <- 5

x_train <- text_seqs %>%
  pad_sequences(maxlen = maxlen)
dim(x_train)

y_train <- NoNullGen$Genre
length(y_train)

# Defining Model ------------------------------------------------------

#Initialize model
model <- keras_model_sequential()

model %>%
  # Start off with an efficient embedding layer which maps
  # the vocab indices into embedding_dims dimensions
  layer_embedding(max_features, embedding_dims, input_length = maxlen) %>%
  layer_dropout(0.2) %>%

  # Add a Convolution1D, which will learn filters
  # Word group filters of size filter_length:
  layer_conv_1d(
    filters, kernel_size,
    padding = "valid", activation = "relu", strides = 1
  ) %>%
  # Apply max pooling:
  layer_global_max_pooling_1d() %>%

  # Add a vanilla hidden layer:
  layer_dense(hidden_dims) %>%

  # Apply 20% layer dropout
  layer_dropout(0.2) %>%
  layer_activation("relu") %>%

  # Project onto a single unit output layer, and squash it with a sigmoid

  layer_dense(1) %>%
  layer_activation("sigmoid")

# Compile model
model %>% compile(
  loss = "binary_crossentropy",
  optimizer = "adam",
  metrics = "accuracy"
)

# Training ----------------------------------------------------------------

model %>%
  fit(
    x_train, imdb$train$y,
    batch_size = batch_size,
    epochs = epochs,
    validation_data = list(x_test, imdb$test$y)
  )
