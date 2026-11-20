-- Editor SQL. Resultados esperados en resultados_esperados.json.
use catalog workspace;
select count(*) as clientes from taller_dbt_analytics.dim_cliente;
select count(*) as productos from taller_dbt_analytics.dim_producto;
select count(*) as lineas from taller_dbt_analytics.fact_ventas;
select * from taller_dbt_analytics.mart_ventas_mes order by mes;
select * from taller_dbt_analytics.mart_ventas_categoria order by mes, categoria;
select sum(venta_neta) as venta_neta_pagada
from taller_dbt_analytics.fact_ventas where estado = 'pagada';

select count(distinct venta_id) as ventas_distintas from taller_dbt_analytics.fact_ventas;
select dni from taller_dbt_analytics.dim_cliente where cliente_id = 1;
select table_name, table_type from information_schema.tables
where table_schema = 'taller_dbt_analytics' and table_name like 'stg_%'
order by table_name;
