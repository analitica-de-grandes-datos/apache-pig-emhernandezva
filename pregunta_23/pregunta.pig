/*
Pregunta
===========================================================================

Para responder la pregunta use el archivo `data.csv`.

Escriba el código equivalente a la siguiente consulta SQL.

   SELECT 
       firstname,
       color 
   FROM 
       u 
   WHERE 
       color REGEXP '[aeiou]$';

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f pregunta.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/

datos = LOAD 'data.csv' USING PigStorage(',')
    AS (
            id:int,
            nombre:chararray,
            apellido:chararray,
            f_nac:chararray,
            color:chararray,
            id2:int           
        ); 

filtro1 = FILTER datos BY ENDSWITH(color,'a') OR ENDSWITH(color,'e') OR ENDSWITH(color,'i') OR ENDSWITH(color,'o') OR ENDSWITH(color,'u'); 

filtro2 = FOREACH filtro1 GENERATE nombre, color; 

STORE filtro2 INTO 'output/' USING PigStorage(',');
