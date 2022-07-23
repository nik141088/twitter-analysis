# code to put all twitter downloaded data onto DB

library(DBI)

# only continue if machine is acer laptop (condition: DEVICE == "ACER-LAPPY")
# due to space limitation continuing this in Factiva hard disk
if(TRUE) {
  
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
                   "is_quote", "is_retweet", "hashtags", "symbols", "media_type", "lang",
                   "favorite_count", "retweet_count", "quote_count", "reply_count",
                   "place_full_name", "place_type", "country",
                   "followers_count", "friends_count", "listed_count",
                   "statuses_count", "favourites_count",
                   "account_created_at", "verified", "symbols_len", "symbols2", "symbols2_len");
    
    DBCON = dbConnect(RSQLite::SQLite(), DB_file)
    
    for(c in index_cols) {
      TWEET_CAT(paste0("Creating index for twitter DB table for column: ", c));
      query = paste0("create index ", c, " on ", "twitter", "(", c, ");");
      res = dbSendQuery(DBCON, query)
      dbClearResult(res)
    }
    
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
  
}

















