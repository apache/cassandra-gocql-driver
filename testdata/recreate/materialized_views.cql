CREATE KEYSPACE gocqlx_mv WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '2'
};

CREATE TABLE gocqlx_mv.mv_buildings (
    name text,
    city text,
    built int,
    meters int,
    PRIMARY KEY (name)
);

CREATE MATERIALIZED VIEW gocqlx_mv.mv_building_by_city AS
    SELECT * FROM mv_buildings
    WHERE city IS NOT NULL
    PRIMARY KEY(city, name);

CREATE MATERIALIZED VIEW gocqlx_mv.mv_building_by_city2 AS
    SELECT meters FROM mv_buildings
    WHERE city IS NOT NULL
    PRIMARY KEY(city, name);
