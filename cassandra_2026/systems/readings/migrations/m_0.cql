
CREATE KEYSPACE IF NOT EXISTS sensor_ks
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    AND durable_writes = true;


-- CREATE KEYSPACE IF NOT EXISTS sensor_ks
--     WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3}
--     AND durable_writes = true;