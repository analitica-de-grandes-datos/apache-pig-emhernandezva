/*
Pregunta
===========================================================================

Para responder la pregunta use el archivo `data.csv`.

Obtenga los apellidos que empiecen por las letras entre la 'd' y la 'k'. La 
salida esperada es la siguiente:

  (Hamilton)
  (Holcomb)
  (Garrett)
  (Fry)
  (Kinney)
  (Klein)
  (Diaz)
  (Guy)
  (Estes)
  (Jarvis)
  (Knight)

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

filtro1 = FILTER datos BY STARTSWITH(apellido,'D') OR STARTSWITH(apellido,'E') OR STARTSWITH(apellido,'F') OR STARTSWITH(apellido,'G') OR STARTSWITH(apellido,'H') OR STARTSWITH(apellido,'I') OR STARTSWITH(apellido,'J') OR STARTSWITH(apellido,'K'); 
filtro2 = ORDER filtro1 BY id asc;
filtro3 = FOREACH filtro2 GENERATE apellido;

STORE filtro3 INTO 'output/' USING PigStorage(',');
