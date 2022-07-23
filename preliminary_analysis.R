# preliminary analysis
library(syuzhet);
library(stringr);


# remove all punctions excpet a few
regex_rem_punct = function(str, match_spcl_chars = NULL, replace_spcl_chars = NULL) {
  
  if(!is.null(match_spcl_chars)) {
    # preseve special characters
    n = len(match_spcl_chars);
    random_string = stringi::stri_rand_strings(n, 32);
    for(i in 1:n) {
      str = gsub(match_spcl_chars[i], random_string[i], str);
    }
    
    str = gsub("[[:punct:]]", "", str);
    
    for(i in 1:n) {
      str = gsub(random_string[i], replace_spcl_chars[i], str);
    }

  } else {
    
    str = gsub("[[:punct:]]", "", str);
    
  }
  
  return(str);
  
}




# replace matches one at a time on several strings
regex_replace_multi = function(str, pattern, replacement) {
  for(i in 1:len(pattern)) {
    str = str_replace_all(str, pattern[i], replacement[i]);
  }
  return(str);
}





# clean tweets as per 2017 JBF paper: "Intraday online investor sentiment and return patterns in the U.S. stock market". See section 3.3.
# Unicode emojis are handled as per SSRN paper: What Makes Cryptocurrencies Special? Investor Sentiment and Return Predictability During the Bubble. Access at: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3398423. See section 3.2
# l1 sentiment is available at http://www.thomas-renault.com/l1_lexicon.csv while emoji sentiment is available at https://sites.google.com/site/professorcathychen/resume/emoji_sentiment_ranking.csv?attredirects=0&d=1
regex_clean_tweets = function(str) {
  
  str %>%
    str_to_lower %>% # convert all text to lower case. Keep upper-case for punctuation and unicode emojis
    str_replace_all("\\$[a-z]+\\s*", " cashtag ") %>% # replace all occurrences beginning with $ followed by one or more letters
    str_replace_all("@\\S+\\s*", " usertag ") %>% # replace all occurrences beginning with @ followed by one or more non-space chars
    str_replace_all("http\\S+\\s*", " linktag ") %>% # replace all occurrences beginning with http followed by one or more non-space chars
    str_replace_all("#\\S+\\s*", " hashtag ") %>% # replace all occurrences beginning with # followed by one or more non-space chars
    str_replace_all("\\s*\\b(a|an|the)\\b\\s*", " ") %>% # replace all occurrences of 'a', 'an' and 'the' around word boundaries
    str_replace_all("\\b\\d+\\b\\s*", " numbertag ") %>% # replace all occurrences of one or more digits around word boundaries
    str_replace_all("(\\:\\)|\\;\\)|\\:\\-\\)|\\=\\)|\\:[D])", " emojipos ") %>% # replace all positive text emojis
    str_replace_all("(\\:\\(|\\:\\-\\(|\\=\\()", " emojineg ") %>% # replace all negative text emojis
    regex_replace_multi(paste0("\\", l1_spcl_chars), l1_spcl_names) %>% # assign special upper-case names for punctuation
    regex_rem_punct() %>% # remove all other punctuations.
    # You may only assign special characters (like negtag_ below) after this line. All punctuations will be lost in prev command
    str_replace_all("\\b(not|no|none|neither|never|nobody)\\b\\s*", " negtag_") %>% # replace ngative words by negtag_
    regex_replace_multi(emoji$unicode, paste0(" ", emoji$tag, " ")) %>% # assign special upper-case names for unicode emojis
    str_replace_all("\\s+", " ") %>% # replace multiple spaces with a single space
    str_replace_all("(^\\s|\\s$)", ""); # remove beginning and trailing spaces introduced in previous step
}





# Use dict and the sentiment (sw) therein to compute overall sentiment
find_sentiment = function(cleaned_str, src = c("l1", "emoji"), return_dt = F) {
  
  sent = rep(NA, len(cleaned_str));
  
  for(i in 1:len(cleaned_str)) {
    
    d = str_count(cleaned_str[i], dict$search_pattern); # count the number of occurrences of each pattern
    match_found = which(d > 0);
    
    c = dict[match_found];
    c[, num_matches := d[match_found]];
    
    # remove those words which are matched either by a 2-gram or a higher sentiment pattern.
    c = c[order(-n_words, -abs(sw))];
    idx = match_found[match(c$keyword, dict$keyword[match_found])]; # Faster than: match(c$keyword, dict$keyword)
    # rowSums doesn't work with 1x1 matrix
    if(len(idx) > 1) {
      rem_idx = which(Matrix::rowSums(pre_suf_mx[idx, idx]) > 0);
      if(len(rem_idx) > 0) {
        c = c[-rem_idx];
      }
    }
    
    sum_den = c[source %in% src, sum(num_matches)];
    if(sum_den == 0) {
      sent[i] = 0;
    } else {
      sent[i] = c[source %in% src, sum(sw*num_matches)/sum_den];
    }
    
  }
  
  if(return_dt == F | len(cleaned_str) > 1) {
    return(sent);
  } else {
    return(list(dt = c, sent = sent));
  }
  
  
}






if(.Platform$OS.type == "windows") {
  setwd(paste0(twitter_dir, "twitter_raw/"))
} else {
  setwd("/scratch/iimb/n1621009/signal_disp/TWEETS/");
}






# See: section 3 of 2017 - JBF - Intraday online investor sentiment and return patterns in the U.S. stock market. The L1 stocktwits lexicon is available from author's webpage http://www.thomas-renault.com/data.php. I have downloaded and kept it at "thomas_renault_l1_lexicon.csv". See the paper for more details.
# Note that the sentiment in l1 below (variable: sw) ranges from -1 to -0.2 and 0.22 to 1. That is there are no neutral words with sentiment between -0.2 to 0.22. Thus, all n-grams that are not in the 8000 list of n-grams in L1 have a sentiment of 0.
if(.Platform$OS.type == "windows") {
  l1 = fread(paste0(twitter_dir, "thomas_renault_l1_lexicon.csv"), sep = ";")
} else {
  l1 = fread("/scratch/iimb/n1621009/signal_disp/TWEETS/thomas_renault_l1_lexicon.csv", sep = ";")
}
l1[, n_words := str_count(keyword, "\\s+") + 1]; # 1 + number of occurrences of one or more white-space characters. This method assumes that words are separated by white-spaces.

# take extra care of the below chars: +, !, $, %, ?, -
l1_spcl_chars = str_replace_all(l1$keyword, "[a-z _]", "") %>% paste0(collapse = "") %>% strsplit("") %>% unlist %>% unique;
l1_spcl_names = rep(NA, len(l1_spcl_chars));
for(i in 1:len(l1_spcl_chars)) {
  if(l1_spcl_chars[i] == "+") {
    l1_spcl_names[i] = "PLUS";
  } else if (l1_spcl_chars[i] == "!") {
    l1_spcl_names[i] = " EXCLAMATION "; # extra spaces around
  } else if (l1_spcl_chars[i] == "$") {
    l1_spcl_names[i] = "DOLLAR";
  } else if (l1_spcl_chars[i] == "%") {
    l1_spcl_names[i] = "PERCENTAGE";
  } else if (l1_spcl_chars[i] == "?") {
    l1_spcl_names[i] = " QUESTION "; # extra spaces around
  } else if (l1_spcl_chars[i] == "-") {
    l1_spcl_names[i] = "MINUS";
  }
}


l1[, search_pattern := keyword];
for(i in 1:len(l1_spcl_chars)) {
  l1[, search_pattern := str_replace_all(search_pattern, paste0("\\", l1_spcl_chars[i]), trimws(l1_spcl_names[i]))];
}
l1[, search_pattern := paste0("\\b", search_pattern, "\\b")];

# this is used for creating prefix/suffix matrix
l1[, keyword2 := regex_replace_multi(keyword, paste0("\\", l1_spcl_chars), trimws(l1_spcl_names))];


# Also look at emoji sentiment from file "emoji_sentiment_ranking.csv". This is available on https://sites.google.com/site/professorcathychen/ and discussed in paper https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3398423.
if(.Platform$OS.type == "windows") {
  emoji = fread(paste0(twitter_dir, "emoji_sentiment_ranking.csv"))
} else {
  emoji = fread("/scratch/iimb/n1621009/signal_disp/TWEETS/emoji_sentiment_ranking.csv")
}
setnames(emoji, "Unicode codepoint", "hex_uni");
emoji[, emj_int := as.integer(hex_uni)];
emoji[, uni := NA_character_];
for(i in 1:nrow(emoji)) {
  emoji$uni[i] = intToUtf8(emoji$emj_int[i]);
}
emoji = emoji[, .(unicode = uni, name = `Unicode name`, type = `Unicode block`, Occurrences, Negative, Neutral, Positive, Position)];
# I only consider somewhat popular emoji for which sentiment computation is accurrate. I hence require each emoji to occure atleast 100 times in tweets. I find sentiment in the usual way.
emoji[Positive + Negative >= 100, sw_emoji := (Positive - Negative) / (Positive + Negative)];
emoji = emoji[!is.na(sw_emoji), .(unicode, name, type, sw_emoji)];
emoji[, tag := paste0("UNICODE_EMOJI_", sprintf("%0.3d", .I), "_", str_replace_all(name, "\\s+", "_"))];



# combined dictionary for l1 and emoji
temp = data.table(keyword = emoji$name, sw = emoji$sw_emoji, n_words = 1, search_pattern = paste0("\\b", emoji$tag, "\\b"), keyword2 = emoji$tag);

dict = rbind(l1, temp);
dict[, source := c(rep("l1", nrow(l1)), rep("emoji", nrow(emoji)))];








# Efficient (and faster) way to generate prefix-suffix matrix. sparse version is only 1.2 MB.
# pre_suf_mx is a F x F matrix where F is the number of features (bag-of-words in the dictionary). pre_suf_mx[i,j] tells whether i-th word occurs in j-th word. word here means bag-of-words. For instance word "up" occurs in "up above" and "way up" but not in "upside" and "dup".
# pre_suf_mx will have all ones in it's diagonal by construction.
# As long as dict is constant, pre_suf_mx will not change. Hence it can be stored for fast access.
pre_suf_mx_fname = ifelse(.Platform$OS.type == "windows",
						  paste0(twitter_dir, "pre_suf_mx.RData"),
                          "/scratch/iimb/n1621009/signal_disp/TWEETS/pre_suf_mx.RData");
if(!file.exists(pre_suf_mx_fname)) {
  row_idx = c();
  col_idx = c();
  for(i in 1:nrow(dict)) {
    print(i);
    idx = which(str_detect(dict$keyword2, dict$search_pattern[i]) == T);
    if(len(idx) > 0) {
      row_idx = c(row_idx, rep(i, len(idx)));
      col_idx = c(col_idx, idx);
    }
  }
  # it may be useful to remove the diagonal elements
  diag_idx = which(row_idx == col_idx);
  row_idx = row_idx[-diag_idx];
  col_idx = col_idx[-diag_idx];
  # generate matrix
  pre_suf_mx = Matrix::sparseMatrix(i = row_idx, j = col_idx, x = TRUE, dims = c(nrow(dict), nrow(dict)));
  rownames(pre_suf_mx) = colnames(pre_suf_mx) = dict$keyword;
  saveRDS(pre_suf_mx, pre_suf_mx_fname);
} else {
  pre_suf_mx = readRDS(pre_suf_mx_fname);
}







# test
# a = "$AAPL! $AAPL! not a good way =( bs =( @niks_Nikhil #NaMo said so! See http://iimb.ac.in to follow 10/20 strategy :) Will the rally stop? $$$ on my mind. valuation $-10. shown 20% correction\U0001F620\U0001F645\U0001F620";
# b = regex_clean_tweets(a);
# sent = find_sentiment(b, return_dt = T);
# 
# # the below is my latest tweet. Try: rtweet::get_timeline("Niks_nikhil", n = 1);
# t = "this is a tweet. \U0001f600 \U0001f606?\U0001f60e$AAPL\U0001f621#JoJo\U0001f611. The idea is to search emoticons!\U0001f61a!\U0001f61d!";
# which(str_detect(t, emoji$unicode) == T);









# Process each of the final_xxxx.RData one by one
all_files = list.files(".", "final_.*.RData");
all_files = sort(all_files);

for(l in 1:len(all_files)) {
  
  print(l);
  
  # if it is already processed then skip it
  M_file    = str_replace(all_files[l], "final_", "M_");
  sent_file = str_replace(all_files[l], "final_", "sent_");
  if(file.exists(M_file) & file.exists(sent_file)) {
    next;
  }
  
  f = readRDS(all_files[l]);
  f[, file := substr(all_files[l], 1, nchar(all_files[l]) - 6)];
  
  # use only english language tweets
  f = f[lang == "en"];
  
  # keep only those tweets which discuss about one firm. What's the best way to do this?
  # f = f[symbols_len == 1 & symbols2_len == 1];
  # I think the twitter provided symbols are better!
  f = f[symbols_len == 1];
  
  # sort by user_id and time of creation
  setorder(f, user_id, created_at);
  
  # only keep original tweets. that is delete retweets and replies
  f = f[is_retweet == F & is.na(reply_to_status_id)];
  
  # unlist symbols
  f[, symbols := unlist(symbols)];
  # drop symbols2
  f[, symbols2 := NULL];
  f[, symbols2_len := NULL];
  
  # clean tweets. It takes care of unicode emojis as well
  f[, cleaned_tweets := regex_clean_tweets(text)]; # takes 1-2 min for each file
  
  # compute occurrences of each dictionary word in every tweet. The below takes roughly 30 mins to 60 mins for each file. It depends on the number of tweets
  row_idx = c();
  col_idx = c();
  data = c();
  start = Sys.time();
  for(i in 1:nrow(dict)) {
    a = str_count(f$cleaned_tweets, dict$search_pattern[i]);
    if(len(a) > 0) {
      # create row and col index.
      idx = which(a != 0);
      row_idx = c(row_idx, rep(i, len(idx)));
      col_idx = c(col_idx, idx);
      data = c(data, a[idx]);
    }
    cat("file: ", all_files[l], " i: ", i, " out of ", nrow(dict), ". Completed ", round(100*i/nrow(dict), 3), "% of search in ", format(Sys.time() - start), "\n", sep = "");
  }
  M = Matrix::sparseMatrix(i = row_idx, j = col_idx, x = data, dims = c(nrow(dict), nrow(f)));
  rownames(M) = dict$keyword2;
  colnames(M) = f$status_id;
  saveRDS(M, M_file, compress = F); # M_xxxx.RData
  # Note that M and final_xxxx have different number of rows. If at all you need to match tweets in final to that of M then use status_id from final_xxxx and match with colnames(M). Example below:
  # searching: which(colnames(M) %in% f$status_id[1:5])
  # subsetting dict: which(rownames(M) %in% dict$keyword2[771:780])
  
  # sentiment
  # the below code gives explanations of how to find sentiment using matrix M and pre_suf_mx. Meaning of matrices I, X and P are also defined. Also mentioned is the computation of sentiment measures sent1 and sent2.
  if(FALSE) {
    # get logical version of M for subsetting dict
    M_log = Matrix::sparseMatrix(i = row_idx, j = col_idx, x = as.logical(data), dims = c(nrow(dict), nrow(f)));
    
    # to get the dict corresponding to 101-th tweet. The below is equivalent to `c` matrix in function find_sentiment().
    dict[M_log[, 101]];
    
    # M[, 101] is identical to `d` in find_sentiment().
    # which(M_log[,101]) and which(M[,101] > 0) both are identical to `match_found` in find_sentiment().
    # `d[match_found]` is simply M[M_log[, 101], 101].
    # Try: cbind(dict[M_log[, 101]], num_matches = M[M_log[, 101], 101]);
    
    # Sentiment of a single tweet can be computed as:
    t = 101;
    weighted.mean(dict$sw, M[,t] - pre_suf_mx %*% M[,t]); # Sentiment is first weighted by number of matches M[,t]. It is then adjusted for prefix/suffix keywords by subtracting `pre_suf_mx %*% M[,t]`. Note that we want weighted avg sentiment of keywords, hence a mere vector multiplication won't work. The latter would give weighted sum of sentiment instead.
    
    # We can write, `M[,t] - pre_suf_mx %*% M[,t]` as `(I - pre_suf_mx) %*% M[,t]` where I is an identity matrix.
    I = Matrix::sparseMatrix(t = 1:nrow(dict), j = 1:nrow(dict), x = TRUE); # 0.1 MB
    X = I - pre_suf_mx; # new matrix. This is just for ease of writing! Its 1.3 MB
    
    # The below two also provides sentiment for t-th tweet
    weighted.mean(dict$sw, (I - pre_suf_mx) %*% M[,t]);
    weighted.mean(dict$sw, X %*% M[,t]);
    
    # We can write the above in matrix form as:
    dict$sw %*% X %*% M[,t] / sum(X %*% M[,t]); # This also gives the correct sentiment.
    
    # The above can be genealized for all tweets as below:
    dict$sw %*% X %*% M / Matrix::colSums(X %*% M); # This gives the sentiment for all tweets in one-shot. It is also extremely fast compared to find_sentiment().
    
    # The above can be broken down: `X = I - pre_suf_mx` to give more clarity as to what is happening:
    dict$sw %*% (I - pre_suf_mx) %*% M / Matrix::colSums((I - pre_suf_mx) %*% M); # I %*% M is the sentiment of all matches irrespective of prefix/suffix. pre_suf_mx %*% M is the sentiment for the patterns that are prefix or suffix of some existing bigrams. Finally, we subtract second quantity from first to get the overall sentiment.
    
    # Note that if the text is "... going down to ..." and the dictionary containts "down", "down to" and "going down"; then the above computation of sentiment will add sentiment for "down", "down to" and "going down" and subtract twice the sentiment for "down". This is because "down" appears in both "down to" and "going down"!
    
    # Another way to approach this is to subtract sentiment of `down` only once. This can be achieved by setting all positive entries of `pre_suf_mx %*% M` to 1. This can be done by using `is.POS(pre_suf_mx %*% M)` instead of `pre_suf_mx %*% M`. Since both `pre_suf_mx` and `M` have all non-negative entires, `pre_suf_mx %*% M` also has all non-negative entries.
    
    # sentiment type-2
    P = is.POS(pre_suf_mx %*% M); # 405 MB
    dict$sw %*% (M - P) / Matrix::colSums(M - P);
    # Note that `X %*% M  -->  (I - pre_suf_mx) %*% M  -->  M - pre_suf_mx %*% M` and `P` is `is.POS(pre_suf_mx %*% M)`
  }
  
  I = Matrix::sparseMatrix(i = 1:nrow(dict), j = 1:nrow(dict), x = TRUE);
  X = I - pre_suf_mx; # new matrix. This is just for ease of writing!
  P = is.POS(pre_suf_mx %*% M);
  
  # If any element of Matrix::colSums(X %*% M) is 0 then the sentiment would be NaN (due to 0/0). Thus wherever sentiment is NaN it should be changed to 0.
  sent1 = as.vector(dict$sw %*% X %*% M / Matrix::colSums(X %*% M));
  sent2 = as.vector(dict$sw %*% (M - P) / Matrix::colSums(M - P));
  
  idx1 = which(is.nan(sent1));
  idx2 = which(is.nan(sent2));
  
  sent1[idx1] = 0;
  sent2[idx2] = 0;
  
  f[, sent1 := sent1];
  f[, sent2 := sent2];
  
  # save sentiment separately as well
  saveRDS(f[, .(status_id, symbols, cleaned_tweets, file, sent1, sent2)], sent_file, compress = F); # sent_xxxx.RData
  
}







# We do not need all the data from tweets. Only keep the relevant data in a brief file.
all_files = list.files(".", "final_.*.RData");

for(l in 1:len(all_files)) {
  
  print(l);
  
  num_replies_fname = str_replace(all_files[l], "final_", "num_replies_");
  num_quotes_fname = str_replace(all_files[l], "final_", "num_quotes_");
  brief_fname = str_replace(all_files[l], "final_", "brief_");
  
  if(file.exists(num_replies_fname) & file.exists(num_quotes_fname) & file.exists(brief_fname)) {
    next;
  }
  
  f = readRDS(all_files[l]);
  
  # Think about the connection between the following:
  # status_id
  # reply_to_status_id
  # quoted_status_id
  # quoted_statuses_count
  # retweet_status_id
  # retweet_statuses_count
  # statuses_count
  
  # Run the following:
  # t = rtweet::get_timeline("Niks_nikhil", n = 1e6); setDT(t);
  # t = t[1:5, .(text16 = substr(text, 1, 16), status_id, reply_to_status_id, quoted_status_id, quoted_statuses_count, retweet_status_id, retweet_statuses_count, statuses_count)]
  
  # num_replies below give the number of replies to a particular status_id. Similarly num_quotes.
  # num_replies = t[!is.na(reply_to_status_id), .N, .(status_id = reply_to_status_id)];
  # num_quotes = t[!is.na(quoted_status_id), .N, .(status_id = quoted_status_id)];
  
  # It is extremely important to understand that for num_replies and num_quotes to be DUPLICATION free, it must always be ran on fresh/new tweets. Otherwise there will be double/triple/... counting. If all tweets across all final_xxxx.RData files are unique, then this problem will not arise.
  # num_replies and num_quotes corresponding to each final_xxxx.RData file should be aggregated by status_id to give a full picture of the number of replies and quotes corresponding to a given tweet!
  num_replies = f[!is.na(reply_to_status_id), .N, .(status_id = reply_to_status_id)];
  num_quotes = f[!is.na(quoted_status_id), .N, .(status_id = quoted_status_id)];
  saveRDS(num_replies, num_replies_fname, compress = F);
  saveRDS(num_quotes, num_quotes_fname, compress = F);
  
  # keep only english language tweets
  f = f[lang == "en"];
  # only keep original tweets. that is delete retweets and replies. Note that we are keeping quotes!
  f = f[is_retweet == F & is.na(reply_to_status_id)];
  # keep only those tweets which discuss about one firm.
  f = f[symbols_len == 1];
  # unlist symbols
  f[, symbols := unlist(symbols)];
  # drop symbols2
  f[, symbols2 := NULL];
  f[, symbols2_len := NULL];
  
  f = f[, .(status_id, created_at, symbols, user_id,
            text_len = display_text_width, text_likes = favorite_count, text_retweets = retweet_count,
            has_media = !is.na(media_type), user_followers = followers_count,
            user_following = friends_count, user_tot_tweets = statuses_count)];
  saveRDS(f, brief_fname, compress = F);
  
}






















