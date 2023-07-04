-- this sql file was converted from json by udacity Chatgpt
CREATE EXTERNAL TABLE customer_landing (
    customername STRING,
    email STRING,
    phone STRING,
    birthday STRING,
    serialnumber STRING,
    registrationdate BIGINT,
    lastupdatedate BIGINT,
    sharewithresearchasofdate BIGINT,
    sharewithpublicasofdate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://adozrl-udacity-stedi/customer/landing'

