CREATE TABLE IF NOT EXISTS word(
id serial,
str_rep varchar(100) NOT NULL,
type varchar(20),
PRIMARY KEY(id));

CREATE TABLE IF NOT EXISTS occurence(
id serial,
year int NOT NULL,
book_count int NOT NULL CHECK(book_count >= 0),
freq int NOT NULL CHECK(freq >= 0),
PRIMARY KEY(id, year),
FOREIGN KEY(id) REFERENCES word(id));