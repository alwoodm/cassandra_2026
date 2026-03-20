use sensor_ks;

CREATE TABLE readings (
    id      uuid PRIMARY KEY,
    city    text,
    created timestamp,
    value   float
);