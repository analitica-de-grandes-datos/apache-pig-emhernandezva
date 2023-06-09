/*
Pregunta
===========================================================================

Para el archivo `data.tsv` compute la cantidad de registros por letra de la 
columna 2 y clave de al columna 3; esto es, por ejemplo, la cantidad de 
registros en tienen la letra `b` en la columna 2 y la clave `jjj` en la 
columna 3 es:

  ((b,jjj), 216)

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f pregunta.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/

data = LOAD 'data.tsv' USING PigStorage('\t')
    AS (
            mayuscula:chararray,
            minuscula:chararray,
            claves:chararray          
        ); 

filtro1 = FOREACH data GENERATE mayuscula, FLATTEN(TOKENIZE(minuscula,',')) as minus, FLATTEN(TOKENIZE(claves,',')) as clave; 

filtro2 = FOREACH filtro1 GENERATE REPLACE (minus,'([^a-zA-Z\\s]+)','') as minus, REPLACE (clave,'([^a-zA-Z\\s]+)','') as clave;

filtro3 = FOREACH filtro2 GENERATE TOTUPLE(minus,clave) as tupla; 

filtro4 = GROUP filtro3 BY tupla; 

filtro5 = FOREACH filtro4 GENERATE group, COUNT(filtro3); 

STORE filtro5 INTO 'output/' USING PigStorage(',');
