/*
Pregunta
===========================================================================

Para el archivo `data.tsv` compute Calcule la cantidad de registros en que 
aparece cada letra minúscula en la columna 2.

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f pregunta.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/
datos = LOAD 'data.tsv' USING PigStorage('\t')
    AS (
            mayuscula:chararray,
            minuscula:chararray,
            caracteres:int          
        ); 

columna_selecc = FOREACH datos GENERATE minuscula;

filtro1 = FOREACH columna_selecc GENERATE FLATTEN(TOKENIZE(minuscula)) as letra; 

filtro2 = FILTER filtro1 BY (letra MATCHES '.*[a-z].*');

filtro3 = GROUP filtro2 BY letra; 

filtro4= FOREACH filtro3 GENERATE group, COUNT(filtro2); 

STORE filtro4 INTO 'output/' USING PigStorage(',');
