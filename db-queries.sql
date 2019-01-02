-- create keyword_count table
CREATE TABLE "keyword_count" (
  "user_keyword" varchar(250) NOT NULL,
  "freq" int(11) NOT NULL,
  KEY "keyword_count_user_keyword_IDX" ("user_keyword","freq") USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE mydb.tweet_keywords (
	user_id BIGINT UNSIGNED NOT NULL,
	keywords varchar(300) NULL
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8
COLLATE=utf8_general_ci;
CREATE INDEX tweet_keywords_user_id_IDX USING BTREE ON mydb.tweet_keywords (user_id);