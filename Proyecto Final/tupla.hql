
-- Crear una base de datos si no existe (opcional)

-- Crear la tabla para las palabras
CREATE TABLE IF NOT EXISTS tupla (
    word string,
    us string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
lines terminated by '\n';

LOAD DATA LOCAL INPATH 'log/tupla.txt' overwrite INTO TABLE tupla;

INSERT OVERWRITE LOCAL DIRECTORY 'log/tupla'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT us, COUNT(word) AS cantidad_palabras
FROM tupla
GROUP BY us
ORDER BY cantidad_palabras DESC
limit 15;
