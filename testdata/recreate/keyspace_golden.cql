CREATE KEYSPACE gocqlx_keyspace WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': '2'
};
