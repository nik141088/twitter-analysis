
DEVICE = NULL;

NUM_DAYS_GAP = 4;

serial_num = system("wmic bios get serialnumber", intern = T);
serial_num = serial_num[2] %>% strsplit("  ") %>% {.[[1]][1]};


if(serial_num == "CNU0242K92") { # HP-lappy
  DEVICE = "HP-LAPPY";
  source2("C:/Users/nikhil/Documents/signal_dispersion_ESSENTIALS.R")
  code_dir = "C:/Users/nikhil/Documents/twitter_analysis/";
  twitter_dir = "F:/";
  setwd(code_dir);
} else if (serial_num == "NHQ3HSI007926041133400") { # acer predator
  DEVICE = "ACER-LAPPY";
  source2(paste0(base_dir, "signal_dispersion_ESSENTIALS.R"));
  code_dir = "C:/Users/nikhi/Dropbox/research/news_edgar_and_twitter/extraction_code/twitter_analysis/";
  twitter_dir = "F:/";
  setwd(code_dir);
} else if (serial_num == "EBN0CX66745847C") { # asus-small-lappy
  DEVICE = "ASUS-LAPPY";
  source2("signal_dispersion_ESSENTIALS.R")
  setwd("C:/Users/nikhil/Desktop/twitter_analysis/");
  twitter_dir = "C:/Users/nikhil/Desktop/twitter_analysis/";
}


TWEET_CAT = function(..., file = "", sep = "", fill = F, labels = NULL, append = F) {
  cat("\n", sprintf("%s__%s: ", DEVICE, Sys.time()), ..., "\n", file = file, sep = sep, fill = fill, labels = labels, append = append);
}


tok = yaml::yaml.load(readLines("twitter.yaml"));
token = rtweet::create_token("niks.iisc", consumer_key = tok$api_key, consumer_secret = tok$api_secret,
                             access_token = tok$access_token, access_secret = tok$access_token_secret);

# a = rtweet::rate_limit() %>% as.data.table(); a[query %in% c("application/rate_limit_status", "search/tweets")]; a[limit != remaining];

firm_list = fread("firms_as_on_dec_2019.csv");
setorder(firm_list, -MCAP);
firm_list = firm_list[, .SD[1], by = TICKER];
firm_list[, TICKER2 := paste0("$", TICKER)];


# twitter search is much more effective (and fast) if we leverage multiple tweets per request for smaller firms. We can search like:
# 1) first 1 firm at a time for the first 25 firms   -- 1*25
# 2) then 5 firms at a time for the next 75 firms    -- 5*15
# 3) then 25 firms at a time for the next 1000 firms -- 25*40
# 4) then 40 firms at a time for the next 2440 firms -- 40*61
# 5) remaining 16 firsm together
firm_indices = list();
firm_indices = append(firm_indices, lapply(1:25, function(i) seq(from = 1    + (i-1)*1,  length.out = 1)));
firm_indices = append(firm_indices, lapply(1:15, function(i) seq(from = 26   + (i-1)*5,  length.out = 5)));
firm_indices = append(firm_indices, lapply(1:40, function(i) seq(from = 101  + (i-1)*25, length.out = 25)));
firm_indices = append(firm_indices, lapply(1:61, function(i) seq(from = 1101 + (i-1)*40, length.out = 40)));
firm_indices = append(firm_indices, list(3541:nrow(firm_list)));

# save for future reference
saveRDS(firm_indices, paste0(twitter_dir, "twitter_firm_indices.RData"));


# create data and safe directory if it doesn't exists
data_dir = paste0(twitter_dir, "twitter_raw/data/");
if(!dir.exists(data_dir)) {
  dir.create(data_dir);
}

# create safe directory. Files from here
safe_dir = paste0(twitter_dir, "twitter_raw/safe_to_move/");
if(!dir.exists(safe_dir)) {
  dir.create(safe_dir);
}


# there should be no common files in data and safe directory
tmp = list.files(data_dir, "*.RData", full.names = F);
tmp = c(tmp, list.files(safe_dir, "*.RData", full.names = F));
if(sum(duplicated(tmp)) > 0) {
  stop("Fatal Error!");
}


# The below will append all the output to the log file as well as console output.
sink(paste0(twitter_dir, "twitter_raw/log.txt") , append = T, split = T);


# mark the start of the process
TWEET_CAT("Starting a new run of data_from_twitter.R");


all_files = list.files(data_dir, pattern = ".*.RData", full.names = F);
all_files = substr(all_files, 1, nchar(all_files) - 6);

file_table = data.table(iteration = strsplit(all_files, "_") %>% sapply(function(i) i[1]) %>% as.integer,
                        firm_idx = strsplit(all_files, "_") %>% sapply(function(i) i[2]) %>% as.integer);
file_table[, file_name := list.files(data_dir, ".*.RData", full.names = T)];
file_table[, last_modified := file.mtime(file_name)];
file_table[, elapsed_time := difftime(Sys.time(), file_table$last_modified, units = "days") %>% as.numeric];
setorder(file_table, iteration, -elapsed_time);


# move old files to safe dir
all_files = copy(file_table);
# For each firm index with two or more iterations mark all files except the ones with highest iteration as safe
setorder(all_files, firm_idx, -iteration);
unsafe_files = all_files[, .SD[1], firm_idx];
setkey(all_files, firm_idx, iteration);
setkey(unsafe_files, firm_idx, iteration);
safe_files = all_files[!unsafe_files]; # anti-join

# move safe files
if(nrow(safe_files) > 0) {
  safe_files[, new_loc := paste0(safe_dir, iteration, "_", firm_idx, ".RData")];
  safe_files[, file.rename(file_name, new_loc)];
  TWEET_CAT(" Moving files: ", safe_files[, substr(file_name, 21, nchar(file_name))] %>% paste0(collapse = ", "), " to the safe directory");
  # modify remaining files
  all_files = all_files[!safe_files];
}

# Download new files
setorder(all_files, -elapsed_time);
for(i in 1:nrow(all_files)) {
  
  # TWEET_CAT(all_files[i, paste0("Currently working on: ", file_name)]);
  
  # skip download if sufficient time has not elapsed since last download
  if(all_files[i, elapsed_time] <= NUM_DAYS_GAP) {
    # TWEET_CAT(all_files[i, paste0("Skipping: ", file_name, ", last modified at ", as.character(last_modified), " with ", sprintf("%0.3f", elapsed_time), " days of elapsed time")]);
    next;
  }
  
  idx = firm_indices[[all_files[i, firm_idx]]];
  n = length(idx);
  
  TWEET_CAT("Iteration: ", all_files[i, iteration] + 1, ", firm_idx: ", all_files[i, firm_idx],
            " -- count(s): ", idx[1], ":", idx[n], "/", nrow(firm_list),
            " --- Working on: ", firm_list$COMNAM[idx] %>% paste0(collapse = " -- & -- "),
            ". Using ticker(s): ", firm_list$TICKER[idx] %>% paste0(collapse = ", "));
  
  ticker = firm_list$TICKER2[idx] %>% paste0(collapse = " OR ");
  
  dt = TRY_CATCH(expression = rtweet::search_tweets(ticker, n = 1e8, parse = T, retryonratelimit = T),
                 print.attempts = T, max.attempts = 10, ret_val = NULL);
  
  if(is.null(dt)) {
    TWEET_CAT("Fatal error occurred for tickers: ", ticker);
    TWEET_CAT("See: ", all_files[i] %>% unlist %>% paste0(collapse = "___"));
    stop("Fatal error!");
  }
  
  setDT(dt);
  
  TWEET_CAT(nrow(dt), " tweets in iter: ", all_files[i, iteration] + 1, ", firm_idx: ", all_files[i, firm_idx], " starting at: ", dt[, min(created_at)] %>% as.character, " and ending at: ", dt[, max(created_at)] %>% as.character, " spanning ", dt[, range(created_at)] %>% {difftime(.[2], .[1], units = "days")}, " days");
  
  # only keep new tweets
  status_id_file = paste0(twitter_dir, "twitter_raw/", "all_status_ids.RData");
  unq_status_id = readRDS(status_id_file);
  TWEET_CAT("Before len(unq_status_id): ", len(unq_status_id));
  dt[, status_id := as.numeric(status_id)];
  setorder(dt, status_id, -created_at);
  dt = dt[, .SD[1], by = status_id];
  setkey(dt, status_id);
  match_by = data.table(status_id = unq_status_id);
  setkey(match_by, status_id);
  dt = dt[!match_by];
  unq_status_id = c(unq_status_id, dt$status_id);
  TWEET_CAT("After len(unq_status_id): ", len(unq_status_id));
  saveRDS(unq_status_id, status_id_file, compress = F);
  
  TWEET_CAT(nrow(dt), " tweets (after processing) in iter: ", all_files[i, iteration] + 1, ", firm_idx: ", all_files[i, firm_idx], " starting at: ", dt[, min(created_at)] %>% as.character, " and ending at: ", dt[, max(created_at)] %>% as.character, " spanning ", dt[, range(created_at)] %>% {difftime(.[2], .[1], units = "days")}, " days");
  
  new_file_name = paste0(all_files[i, iteration] + 1, "_", all_files[i, firm_idx], ".RData");
  
  TWEET_CAT("Saving ", nrow(dt), " tweets in file: ", new_file_name);
  saveRDS(dt, paste0(data_dir, new_file_name), compress = F);
  
  
  # # send mail of the overall activity.
  # num_tweets = nrow(final);
  # file_size_kb = paste0(round(file.size(filename)/2^10, 2), " MB");
  # all_files = list.files(paste0(twitter_dir, "twitter_raw/"), "*.RData", full.names = T);
  # avg_file_size = paste0(round(mean(file.size(all_files))/2^20, 2), " KB");
  # send.mail(from = "nikhil141088@gmail.com",
  #           to = c("nikhil141088@gmail.com", "nikhil.vidhani16@iimb.ac.in"),
  #           subject = paste0("Twitter iteration: ", iteration, " completed at: ", format(Sys.time(), '%F %r, %Z')),
  #           body = paste0("Total tweets: ", num_tweets, ". Downloaded file size: ", file_size_kb, ". Average file size: ", avg_file_size, "."),
  #           smtp = list(host.name = "smtp.gmail.com", port = 465, user.name = "nikhil141088", passwd = "naasvbqqbsbszbha", ssl = TRUE),
  #           authenticate = TRUE,
  #           send = TRUE,
  #           attach.files = file_firm_specific_macros,
  #           file.names = strsplit(file_firm_specific_macros, "/") %>% unlist %>% last,
  #           file.descriptions = "Details of number of tweets by firm",
  #           debug = TRUE);
  
}



# It is safe to run the below two files as long as they are not modified
source2(paste0(code_dir, "safe_to_final.R"), echo = F);
# source2(paste0(code_dir, "write_to_db.R"), echo = F);
source2(paste0(code_dir, "preliminary_analysis.R"), echo = F);

# stop sinking to log file
sink(NULL);








