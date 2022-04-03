CREATE TABLE IF NOT EXISTS Countries (
	Country VARCHAR(200) NOT NULL,
	CountryCode VARCHAR(6),
	Slug VARCHAR(200),
	NewConfirmed INTEGER,
	TotalConfirmed INTEGER,
	NewDeaths INTEGER,
	TotalDeaths INTEGER,
	NewRecovered INTEGER,
	TotalRecovered INTEGER,
	Datemaj TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Global (
	NewConfirmed INTEGER,
	TotalConfirmed INTEGER,
	NewDeaths INTEGER,
	TotalDeaths INTEGER,
	NewRecovered INTEGER,
	TotalRecovered INTEGER,
	Datemaj TIMESTAMP
);