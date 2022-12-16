CREATE TABLE IF NOT EXISTS word(
id serial,
str_rep varchar(50) NOT NULL,
typ varchar(10) NOT NULL,
PRIMARY KEY(id));

CREATE TABLE IF NOT EXISTS occurence(
id serial,
year int NOT NULL,
book_count int NOT NULL CHECK(book_count >= 0),
freq int NOT NULL CHECK(freq >= 0),
PRIMARY KEY(id, year),
FOREIGN KEY(id) REFERENCES word(id));