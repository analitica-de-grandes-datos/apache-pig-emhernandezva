/*
Pregunta
===========================================================================

Obtenga los cinco (5) valores más pequeños de la 3ra columna.

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f pregunta.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/
datos = LOAD 'data.tsv' USING PigStorage('\t')
    AS (
            letra:chararray,
            fecha:chararray,
            num:int          
        );

resultado1= ORDER datos BY num asc; 

resultado2= FOREACH resultado1 GENERATE num; 

resultado3 = LIMIT resultado2 5;

STORE resultado3 INTO 'output/' USING PigStorage(',');
