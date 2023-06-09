/*
Pregunta
===========================================================================

Para responder la pregunta use el archivo `data.csv`.

Escriba el codigo en Pig para manipulación de fechas que genere la siguiente
salida.

   1971-07-08,08,8,jue,jueves
   1974-05-23,23,23,jue,jueves
   1973-04-22,22,22,dom,domingo
   1975-01-29,29,29,mie,miercoles
   1974-07-03,03,3,mie,miercoles
   1974-10-18,18,18,vie,viernes
   1970-10-05,05,5,lun,lunes
   1969-02-24,24,24,lun,lunes
   1974-10-17,17,17,jue,jueves
   1975-02-28,28,28,vie,viernes
   1969-12-07,07,7,dom,domingo
   1973-12-24,24,24,lun,lunes
   1970-08-27,27,27,jue,jueves
   1972-12-12,12,12,mar,martes
   1970-07-01,01,1,mie,miercoles
   1974-02-11,11,11,lun,lunes
   1973-04-01,01,1,dom,domingo
   1973-04-29,29,29,dom,domingo

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
filtro1 = FOREACH datos GENERATE f_nac, ToDate(f_nac,'yyyy-MM-dd') as fecha;
filtro2 = FOREACH filtro1 GENERATE f_nac, SUBSTRING(f_nac,8,10) as dia, GetDay(fecha) as dia2, LOWER(ToString(fecha,'EEEEE')) as dia3; 
filtro3 = FOREACH filtro2 GENERATE f_nac, dia, dia2, REPLACE(dia3,'monday','lunes') as dia3;  
filtro4 = FOREACH filtro3 GENERATE f_nac, dia, dia2, REPLACE(dia3,'tuesday','martes') as dia3;  
filtro5 = FOREACH filtro4 GENERATE f_nac, dia, dia2, REPLACE(dia3,'wednesday','miercoles') as dia3;  
filtro6 = FOREACH filtro5 GENERATE f_nac, dia, dia2, REPLACE(dia3,'thursday','jueves') as dia3;  
filtro7 = FOREACH filtro6 GENERATE f_nac, dia, dia2, REPLACE(dia3,'friday','viernes') as dia3;  
filtro8 = FOREACH filtro7 GENERATE f_nac, dia, dia2, REPLACE(dia3,'saturday','sabado') as dia3;  
filtro9 = FOREACH filtro8 GENERATE f_nac, dia, dia2, REPLACE(dia3,'sunday','domingo') as dia3;
filtro10 = FOREACH filtro9 GENERATE f_nac, dia, dia2, SUBSTRING(dia3,0,3), dia3;  

STORE filtro10 INTO 'output/' USING PigStorage(',');
