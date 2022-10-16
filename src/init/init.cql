CREATE KEYSPACE IF NOT EXISTS twitter 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE twitter; 

-- DROP TABLE IF EXISTS twitter_lake;
CREATE TABLE IF NOT EXISTS twitter_lake (
                            tweet_id double,
                            user_description text,
                            user_favourites double,
                            user_followers double,
                            user_friends double,
                            user_id double,
                            user_location text,
                            user_name text,
                            author text,
                            hashtags list<text>, 
                            tweet_text text,
                            retweet_count int,
                            retweeted boolean,
                            created_at text, 
                            PRIMARY KEY (tweet_id, created_at)
);

-- DROP TABLE IF EXISTS batch_layer;
CREATE TABLE IF NOT EXISTS batch_layer (
                            user_id double,
                            user_name text,
                            tweet_text text,
                            created_at text, 
                            PRIMARY KEY (user_id, created_at)
);

-- DROP TABLE IF EXISTS speed_layer;
CREATE TABLE IF NOT EXISTS speed_layer (
                            created_at text, 
                            hashtags list<text>, 
                            tweet_text text,
                            user_id double,
                            user_name text,
                            user_location text,
                            PRIMARY KEY (user_id, created_at)
);