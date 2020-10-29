CREATE KEYSPACE gocqlx_table WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '2'
};

CREATE TABLE gocqlx_table.monkeySpecies (
    species text PRIMARY KEY,
    common_name text,
    population varint,
    average_size int
) WITH comment='Important biological records'
   AND read_repair_chance = 1.0;

CREATE TABLE gocqlx_table.timeline (
    userid uuid,
    posted_month int,
    posted_time uuid,
    body text,
    posted_by text,
    PRIMARY KEY (userid, posted_month, posted_time)
) WITH compaction = { 'class' : 'LeveledCompactionStrategy' };

CREATE TABLE gocqlx_table.loads (
    machine inet,
    cpu int,
    mtime timeuuid,
    load float,
    PRIMARY KEY ((machine, cpu), mtime)
) WITH CLUSTERING ORDER BY (mtime DESC)
    AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
    AND compaction = {'compaction_window_size': '14',
    				  'compaction_window_unit': 'DAYS',
    				  'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'};

CREATE TABLE gocqlx_table.users_picture (
    userid uuid,
    pictureid uuid,
    body text static,
    posted_by text,
    PRIMARY KEY (userid, pictureid, posted_by)
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
