CREATE KEYSPACE bsread_data WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE bsread_data;

DROP TABLE IF EXISTS channel_data;

CREATE TABLE channel_data (
   device_name text,
   pulse_id_mod bigint,
   channel_name text,
   pulse_id bigint,
   
   data blob,
   type text,
   shape frozen<list<int>>,
   encoding text,
   compression text,
   PRIMARY KEY ((device_name, pulse_id_mod), channel_name, pulse_id)
) 
WITH 
    CLUSTERING ORDER BY (channel_name ASC, pulse_id ASC)
    AND compaction = {'class': 'TimeWindowCompactionStrategy'}
    AND compression = {'class': 'LZ4Compressor'};


