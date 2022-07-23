# twitter-analysis

This repo download tweets by searching for a company's cashtag (like `$AAPL` for Apple Inc). Twitter only allows to download recent tweets (last 6-8 days) using their free API. Thus to get a long term dump of tweets, I have automated the process of downloading tweets so that the same code runs every 4-5 days, fetches all the tweets from last week, and keep the new ones, i.e. ones not already downloaded.

Tweets are then used to perform sentiment analysis using tweet and emoticon lexicons.

Currently, I download tweets for roughly 2500-3000 NYSE/NSDAQ firms taken in the end of Dec 2019.

**Note:** This is an ongoing research project.
