CREATE TABLE IF NOT EXISTS metadata (
    id SERIAL PRIMARY KEY,
    locator VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    checksum VARCHAR NOT NULL,
    size BIGINT NOT NULL);

CREATE TABLE IF NOT EXISTS geometadata (
    id SERIAL PRIMARY KEY,
    locator VARCHAR NOT NULL,
    native_srid VARCHAR NOT NULL,
    native_format VARCHAR NOT NULL,
    native_bounds box2d NOT NULL,
    latlon_bounds box2d NOT NULL);

CREATE TABLE IF NOT EXISTS deployments (
    id SERIAL PRIMARY KEY,
    locator VARCHAR NOT NULL,
    server VARCHAR NOT NULL,
    deployed BOOLEAN NOT NULL);
