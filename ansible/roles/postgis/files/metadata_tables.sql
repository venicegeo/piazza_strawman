CREATE TABLE IF NOT EXISTS metadata (
    id SERIAL PRIMARY KEY,
    locator VARCHAR,
    name VARCHAR,
    checksum VARCHAR,
    size BIGINT);

CREATE TABLE IF NOT EXISTS geometadata (
    id SERIAL PRIMARY KEY,
    locator VARCHAR,
    native_srid VARCHAR,
    native_bounds box2d,
    latlon_bounds box2d);
