CREATE EXTERNAL TABLE IF NOT EXISTS adozrl-stedi.machine_learning_curated (
  x FLOAT,
  y FLOAT,
  z FLOAT,
  user STRING,
  distancefromobject INT,
  timestamp BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://adozrl-udacity-stedi/machine_learning_curated/'
;