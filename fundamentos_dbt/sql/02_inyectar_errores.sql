-- Editor SQL. Ejecutar UNA vez sobre los datos base.
-- Para reiniciar: sql/01_datos.sql.
use catalog workspace;
-- Ejecutar UNA vez sobre el conjunto base. Para repetir, recargar 01_datos.sql.

insert into taller_dbt_raw.ventas select * from taller_dbt_raw.ventas where id_linea = '1';
update taller_dbt_raw.ventas set id_cliente = '999' where id_linea = '3';
update taller_dbt_raw.ventas set estado = 'regalada' where id_linea = '4';
update taller_dbt_raw.ventas set cantidad = '-1' where id_linea = '6';
update taller_dbt_raw.ventas set descuento_pct = '110' where id_linea = '8';
update taller_dbt_raw.ventas set id_cliente = null where id_linea = '10';
