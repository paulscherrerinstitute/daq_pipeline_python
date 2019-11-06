CREATE KEYSPACE bsread_data WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE bsread_data;

DROP TABLE IF EXISTS channel_data;

CREATE TABLE channel_data (
   channel_name text,
   pulse_id_mod bigint,
   pulse_id bigint,
   
   data blob,
   type text,
   shape frozen<list<int>>,
   encoding text,
   compression text,
   PRIMARY KEY ((channel_name, pulse_id_mod), pulse_id)
) 
WITH 
    CLUSTERING ORDER BY (pulse_id ASC)
    AND compaction = {'class': 'TimeWindowCompactionStrategy'}
    AND compression = {'class': 'LZ4Compressor'};


