CREATE TABLE IF NOT EXISTS food (
    food_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drink (
    drink_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decoration (
    decoration_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);