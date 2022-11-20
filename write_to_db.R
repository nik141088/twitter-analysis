# code to put all twitter downloaded data onto DB

library(DBI)

# only continue if machine is acer laptop (condition: DEVICE == "ACER-LAPPY")
# due to space limitation continuing this in Factiva hard disk
# DB directory and file
DB_dir = paste0(twitter_dir, "twitter_DB/")
DB_file = paste0(DB_dir, "twitter.db")

all_files = list.files(paste0(twitter_dir, "twitter_raw/"), "final_.*.RData", full.names = T);

for(i in 1:len(all_files)) {
  
  TWEET_CAT(paste0("Into write_to_db.R for file: ", all_files[i]));
  
  iter = all_files[i] %>% str_split("/", simplify = T) %>% as.vector %>% last %>% str_replace_all("final_", "") %>% str_replace_all("\\.RData", "");
  
  done_file = paste0(DB_dir, "done/", iter, ".done_file")
  
  if(file.exists(done_file)) {
    TWEET_CAT(paste0("Done file: ", done_file, " exists, moving to next!"));
    next;
  }
  
  final = readRDS(all_files[i]);
  
  # delete all columns with URLs
  del_cols = grep("url", names(final), ignore.case = T, value = T)
  final[, c(del_cols) := NULL]
  
  # delete all columns with media
  del_cols = grep("media", names(final), ignore.case = T, value = T)
  final[, c(del_cols) := NULL]
  
  # delete all columns with coords except geo_coords
  del_cols = grep("coords", names(final), ignore.case = T, value = T)
  del_cols = setdiff(del_cols, "geo_coords")
  final[, c(del_cols) := NULL]
  
  # convert status_id from numeric to character
  final[, status_id := sprintf("%0.0f", status_id)]
  # convert display_test_width to integer
  final[, display_text_width := as.integer(display_text_width)]
  
  # convert POSIX types to integer (convert them back when fetched)
  # columns: created_at, quoted_created_at, retweet_created_at, account_created_at
  # to integer: as.integer(created_at)
  # to POSIXct: as.POSIXct(created_at, tz = "UTC", origin = origin)
  TWEET_CAT(paste0("Converting columns: created_at, quoted_created_at, retweet_created_at, account_created_at to integer from POSIXct"));
  final[, created_at         := as.integer(created_at)];
  final[, quoted_created_at  := as.integer(quoted_created_at)];
  final[, retweet_created_at := as.integer(retweet_created_at)];
  final[, account_created_at := as.integer(account_created_at)];
  
  # remove account_lang (not needed since its NA)
  final[, account_lang := NULL];
  
  # convert logicals to integers as well
  int_cols = which(sapply(final, class) == "logical") %>% names;
  for(c in int_cols) {
    TWEET_CAT(paste0("Converting column: ", c, " to integer from logical"));
    final[, eval(c) := get(c) %>% as.integer]
  }
  
  # convert list types to characters separated by comma
  setkey(final, status_id);
  list_cols = which(sapply(final, class) == "list") %>% names;
  for(c in list_cols) {
    TWEET_CAT(paste0("Converting column: ", c, " to character from list"));
    final[, eval(c) := paste0(get(c) %>% unlist %>% sort, collapse = ", "), by = status_id]
    final[get(c) == "NA", eval(c) := ""]
    final[, eval(c) := get(c) %>% unlist]
  }
  
  # change column names with a . to _
  names(final) = str_replace_all(names(final), "\\.", "_");
  
  # create DB if it do not exists
  if(!file.exists(DB_file)) {
    
    # connect to DB
    DBCON = dbConnect(RSQLite::SQLite(), DB_file)
    
    TWEET_CAT(paste0("DB doesn't exists! Creating at: ", DB_file));
    
    # create table
    dbCreateTable(DBCON, "twitter", fields = final)
    
  } else {
    
    # connect to DB
    DBCON = dbConnect(RSQLite::SQLite(), DB_file)
    
  }
  
  # write to db
  TWEET_CAT(paste0("Writing ", round(object.size(final)/2^30, 2),
                   " GBs into DB using file: ", all_files[i]));
  dbWriteTable(DBCON, "twitter", final, append = T)
  
  # close connection
  TWEET_CAT(paste0("Disconnecting from DB: ", DB_file));
  dbDisconnect(DBCON);
  
  # create done file
  TWEET_CAT(paste0("Creating done file: ", done_file));
  file.create(done_file);
  
}


# create index for the following columns
# indexing needs to be done just once
if(FALSE) {
  index_cols = c("status_id", "user_id", "created_at", "screen_name", "display_text_width",
                 "is_quote", "is_retweet", "hashtags", "symbols", "lang",
                 "favorite_count", "retweet_count", "quote_count", "reply_count",
                 "place_full_name", "place_type", "country",
                 "followers_count", "friends_count", "listed_count",
                 "statuses_count", "favourites_count",
                 "account_created_at", "verified", "symbols_len", "symbols2", "symbols2_len");
  
  index_cols = c("created_at", "symbols", "symbols2", "symbols_len", "symbols2_len")
  
  DBCON = dbConnect(RSQLite::SQLite(), DB_file)
  
  for(c in index_cols) {
    TWEET_CAT(paste0("Creating index for twitter DB table for column: ", c));
    query = paste0("create index ", c, " on ", "twitter", "(", c, ");");
    res = dbSendQuery(DBCON, query)
    dbClearResult(res)
  }
  
  
  # two-column index: symbols and symbols_len
  TWEET_CAT("Creating index for twitter DB table for columns: symbols and symbols_len");
  # one-way
  query = paste0("create index symbols_with_len on twitter(symbols, symbols_len);");
  res = dbSendQuery(DBCON, query)
  dbClearResult(res)
  # other-way
  query = paste0("create index len_with_symbols on twitter(symbols_len, symbols);");
  res = dbSendQuery(DBCON, query)
  dbClearResult(res)
  
  
  # two-column index: symbols and created_at
  TWEET_CAT("Creating index for twitter DB table for columns: symbols and created_at");
  # one-way
  query = paste0("create index symbols_with_created_at on twitter(symbols, created_at);");
  res = dbSendQuery(DBCON, query)
  dbClearResult(res)
  # other-way
  query = paste0("create index created_at_with_symbols on twitter(created_at, symbols);");
  res = dbSendQuery(DBCON, query)
  dbClearResult(res)
  
  
  # two-column index: symbols2 and symbols2_len
  TWEET_CAT("Creating index for twitter DB table for columns: symbols2 and symbols2_len");
  # one-way
  query = paste0("create index symbols2_with_len on twitter(symbols2, symbols2_len);");
  res = dbSendQuery(DBCON, query)
  dbClearResult(res)
  # other-way
  query = paste0("create index len_with_symbols2 on twitter(symbols2_len, symbols2);");
  res = dbSendQuery(DBCON, query)
  dbClearResult(res)
  
  
  # two-column index: symbols2 and created_at
  TWEET_CAT("Creating index for twitter DB table for columns: symbols2 and created_at");
  # one-way
  query = paste0("create index symbols2_with_created_at on twitter(symbols2, created_at);");
  res = dbSendQuery(DBCON, query)
  dbClearResult(res)
  # other-way
  query = paste0("create index created_at_with_symbols2 on twitter(created_at, symbols2);");
  res = dbSendQuery(DBCON, query)
  dbClearResult(res)
  
  
  dbDisconnect(DBCON);
}


# function to fetch from DB
fetch_from_DB = function(con = DBCON, query, n = -1) {
  res = dbSendQuery(con, query)
  data = dbFetch(res, n)
  setDT(data)
  dbClearResult(res)
  # dbDisconnect(con)
  return(data)
}


# Note: `symbols` are directly supplied by twitter while `symbols2` are extracted using cashtags

# Querying the DB
# the twitter DB is huge containing 148 Mn tweets. All tweets have a uniue status_id.
# The date of tweet is captured in created_at. this is an integer representing POSIX date/time. To convert this to R date use `as.POSIXct(1666666666, tz = "Asia/Kolkata", origin = "1970-01-01 00:00:00 UTC")`. Note that `as.POSIXct("1970-01-01 00:00:00 UTC", tz = "UTC") %>% as.integer` is 0
# The text of the tweets are captured in the field `text`. It contains cashtags, URLs, hashtags, mentions etc
# `symbols` are the twitter supplied stock tickers used in the tweets. If there are multiple tickers then they are separated by a comma and a space (`, `). `symbols_len` gives the number of tickers in `symbols`
# `symbols2` and `symbols2_len` works exactly the same expect that they are computed by searching tweets for cashtags (words starting with a dollar `$` sign)
# The DB has index for the following five variables: created_at, symbols, symbols2, symbols_len, symbols2_len
# DB can only be queried using the above symbols in SQL queries. Other queries will work but will be much slower.
# DB also has two way indexing for the following variable combinations: (symbols, symbols_len), (symbols, created_at), (symbols2, symbols2_len), (symbols2, created_at). Queries involving these two wil also be fast.




# show all tweets containing apple, google, tesla, and microsoft. Note that we always store multiple tickers in a sorted way (separated by comma and a space)
# fetch_from_DB(query = "select created_at, text from twitter where symbols='AAPL, GOOG, MSFT, TSLA'")

# show top 100 tweets which contain tesla. Note that we can't do an unlimited search this way. It will be very very slow.
# fetch_from_DB(query = "select created_at, symbols from twitter where symbols like '%TSLA%' limit 100")

# show tweet count by symbols_len
# fetch_from_DB(query = "select symbols_len, count(*) as N from twitter group by symbols_len")

# show tweet count by single tickets
# fetch_from_DB(query = "select symbols, count(*) as N from twitter where symbols_len=1 group by symbols")

# show tweet count by double tickets
# fetch_from_DB(query = "select symbols, count(*) as N from twitter where symbols_len=2 group by symbols")

# show apple tweets during Q4/2021 earnings (i.e. 2 PM, October 28, 2021, pacific time)
# start = as.POSIXct("2021-10-28 00:00:01", tz = "PST8PDT") %>% as.integer
# end   = as.POSIXct("2021-10-28 23:59:59", tz = "PST8PDT") %>% as.integer
# query = paste0("select created_at, text from twitter where symbols='AAPL' and created_at >= ", start, " and created_at <= ", end)
# fetch_from_DB(query = query)













