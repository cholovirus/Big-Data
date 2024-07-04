
-- Crear una base de datos si no existe (opcional)

-- Crear la tabla para las palabras
CREATE TABLE IF NOT EXISTS palabras (
    word STRING,
    count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ':'
lines terminated by '\n';

LOAD DATA LOCAL INPATH 'log/count.txt' overwrite INTO TABLE palabras;

INSERT OVERWRITE LOCAL DIRECTORY 'log/count'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT word, SUM(count) AS total_count
FROM palabras
where word != ""
GROUP BY word
limit 15;

SELECT 'Se ha guardado el conteo de palabras en el archivo en /ruta/a/directorio_local' AS mensaje;

