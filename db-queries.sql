-- create keyword_count table
CREATE TABLE "keyword_count" (
  "user_keyword" varchar(250) NOT NULL,
  "freq" int(11) NOT NULL,
  KEY "keyword_count_user_keyword_IDX" ("user_keyword","freq") USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
