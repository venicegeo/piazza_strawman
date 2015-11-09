DROP TABLE IF EXISTS metadata CASCADE;
DROP TABLE IF EXISTS geometadata CASCADE;
DROP TABLE IF EXISTS deployments CASCADE;
DROP TABLE IF EXISTS servers CASCADE;
DROP TABLE IF EXISTS leases CASCADE;

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

CREATE TABLE IF NOT EXISTS servers (
    host VARCHAR NOT NULL,
    port INTEGER NOT NULL,
    local_path VARCHAR NOT NULL,
    id SERIAL PRIMARY KEY);

CREATE TABLE IF NOT EXISTS deployments (
    id SERIAL PRIMARY KEY,
    locator VARCHAR NOT NULL,
    server INTEGER REFERENCES servers (id),
    state VARCHAR CHECK(state IN ('starting', 'live', 'killing', 'dead')));

CREATE TABLE IF NOT EXISTS leases (
    id SERIAL PRIMARY KEY,
    locator VARCHAR NOT NULL,
    deployment INTEGER REFERENCES deployments (id),
    lifetime TIMESTAMP WITH TIME ZONE DEFAULT(now() + '1 hour'));

INSERT INTO servers (host, port, local_path) VALUES
    ('192.168.23.13', 8081, '/var/lib/geoserver_data/geoserver1/data'),
    ('192.168.23.13', 8082, '/var/lib/geoserver_data/geoserver2/data');
