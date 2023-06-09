/*
Pregunta
===========================================================================

Para el archivo `data.tsv` genere una tabla que contenga la primera columna,
la cantidad de elementos en la columna 2 y la cantidad de elementos en la 
columna 3 separados por coma. La tabla debe estar ordenada por las 
columnas 1, 2 y 3.

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

filtro1 = FOREACH data GENERATE mayuscula, TOKENIZE(minuscula,',') as minus, TOKENIZE(claves,',') as clave; 

filtro2 = FOREACH filtro1 GENERATE mayuscula, COUNT(minus) as minus, COUNT(clave) as clave; 

filtro3= ORDER filtro2 BY mayuscula asc, minus asc, clave asc; 

STORE filtro3 INTO 'output/' USING PigStorage(',');
