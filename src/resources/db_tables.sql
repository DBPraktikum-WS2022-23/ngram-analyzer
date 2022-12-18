CREATE TABLE IF NOT EXISTS word(
id serial,
str_rep varchar(200) NOT NULL,
type varchar(20),
PRIMARY KEY(id),
UNIQUE NULLS NOT DISTINCT (str_rep, type));

CREATE TABLE IF NOT EXISTS occurence(
id serial,
year int NOT NULL,
book_count int NOT NULL CHECK(book_count >= 0),
freq int NOT NULL CHECK(freq >= 0),
PRIMARY KEY(id, year),
FOREIGN KEY(id) REFERENCES word(id));