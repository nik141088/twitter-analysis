# the code below moves tweets from safe_to_move folder, processes them ad store as final_xxx.RData

final_files = list.files(paste0(twitter_dir, "twitter_raw/"), "final_.*.RData", full.names = F);
final_num = substr(final_files, 7, 10) %>% as.integer;

last_final = max(final_num);

files = data.table(filename = list.files(paste0(twitter_dir, "twitter_raw/safe_to_move/"), ".*.RData"));
files[, full_name := list.files(paste0(twitter_dir, "twitter_raw/safe_to_move/"), ".*.RData", full.names = T)];
files[, iter_idx := substr(filename, 1, nchar(filename) - 6)];
files[, iteration := strsplit(iter_idx, "_") %>% sapply(function(i__) i__[[1]]) %>% as.integer];
files[, idx := strsplit(iter_idx, "_") %>% sapply(function(i__) i__[[2]]) %>% as.integer];
# check
if(sum(files[, paste0(iteration, "_", idx) == iter_idx]) != nrow(files)) {
  stop("Fatal Error!");
}
files[, iter_idx := NULL];

# We need to find all files with iteration last_final +1, +2, +3 etc
firm_indices = readRDS(paste0(twitter_dir, "twitter_firm_indices.RData"));
files_summ = files[, .N, iteration];
files_summ[, completed := ifelse(len(firm_indices) == N & iteration > last_final, T, F)];

safe_iterations = files_summ[completed == T, iteration];
safe_files = files[iteration %in% safe_iterations];

for(iter in safe_iterations) {
  
  dt = safe_files[iteration == iter, full_name] %>% lapply(readRDS) %>% rbindlist;

  # below two lines make twitter supplied symbols in a list form. Symbols are as per their tickers.
  dt[, symbols := symbols %>% toupper %>% gsub('C\\(\"', '', .) %>% gsub('\\)', '', .) %>% gsub('\"', '', .) %>% gsub("\\$", "", .) %>% strsplit(", ")];
  dt[, symbols_len := length(symbols[[1]]), by = status_id];
  
  # in below I find symbols from actual tweet's text. Symbols are then saved in a single string separated by comma. Here symbols start with a leading $. For searching this needs to be taken care of.
  dt[, symbols2 := stringr::str_extract_all(text, "\\$[A-Za-z]\\w+")];
  dt[, symbols2_len := sapply(symbols2, length)];
  dt[, symbols2 := sapply(symbols2, paste0, collapse = ", ")];
  dt[, symbols2 := toupper(symbols2)];

  # save file  
  filename = paste0(twitter_dir, "twitter_raw/final_", sprintf("%0.4d", iter), ".RData");
  saveRDS(dt, filename, compress = F);
  # remove files from safe folder
  safe_files[iteration == iter, file.remove(full_name)];
  
}



