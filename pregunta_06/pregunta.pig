/*
Pregunta
===========================================================================

Para el archivo `data.tsv` Calcule la cantidad de registros por clave de la 
columna 3. En otras palabras, cuántos registros hay que tengan la clave 
`aaa`?

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f pregunta.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/

datos = LOAD 'data.tsv' USING PigStorage('\t')
    AS (
            mayuscula:chararray,
            minuscula:chararray,
            claves:chararray          
        ); 

filtro1 = FOREACH datos GENERATE claves;

filtro2 = FOREACH filtro1 GENERATE FLATTEN(TOKENIZE(claves)) as clave; 

filtro3 = FOREACH filtro2 GENERATE REPLACE (clave,'([^a-zA-Z\\s]+)','') as clave;

filtro4 = GROUP filtro3 BY clave; 

filtro5= FOREACH filtro4 GENERATE group, COUNT(filtro3); 

STORE filtro5 INTO 'output/' USING PigStorage(',');
