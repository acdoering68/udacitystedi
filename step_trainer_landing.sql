-- this sql file was converted from json by udacity Chatgpt
CREATE EXTERNAL TABLE step_trainer_landing (
  serialnumber STRING,
  distancefromobject INT,
  sensorreadingtime BIGINT
)
COMMENT ''
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
STORED AS TEXTFILE
LOCATION 's3://adozrl-udacity-stedi/step_trainer/landing/'

