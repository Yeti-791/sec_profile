CREATE TABLE "secwiki_detail" (
	`ts`	TEXT NOT NULL,
	`tag`	TEXT NOT NULL,
	`url`	TEXT NOT NULL,
	`title`	TEXT,
	`root_domain`	TEXT,
	`domain`	TEXT,
	`path`	TEXT,
	PRIMARY KEY(`ts`,`url`)
);
CREATE TABLE `xuanwu_detail` (
	`ts`	TEXT NOT NULL,
	`tag`	TEXT NOT NULL,
	`url`	TEXT NOT NULL,
	`title`	TEXT,
	`root_domain`	TEXT,
	`domain`	TEXT,
	`path`	TEXT,
	`author_id`	TEXT,
	PRIMARY KEY(`ts`,`url`)
);


/* select substr(ts,1,4) as day,count(distinct url) from secwiki_detail group by day;




*/