-- Ejecutar en el editor SQL de Databricks con el SQL Warehouse del alumno.
-- Cambiar workspace si el catálogo disponible tiene otro nombre.
-- Este archivo REEMPLAZA solo las tres tablas del esquema didáctico taller_dbt_raw.
-- Mantener el mismo catálogo/esquemas en config/env.* y en los tres scripts SQL.
use catalog workspace;

create schema if not exists taller_dbt_raw;
create schema if not exists taller_dbt_analytics;

create or replace table taller_dbt_raw.clientes (id_cliente string, nombre string, dni string, ciudad string) using delta;

insert into taller_dbt_raw.clientes values
('1', '  ana torres  ', '00123456', ' arequipa '),
('2', 'LUIS RAMOS', '00023456', 'LIMA'),
('3', ' maría paz ', '12345678', ' cusco'),
('4', 'josé díaz', '01234567', 'AREQUIPA'),
('5', 'lucía cruz ', '00456789', ' lima '),
('6', ' rosa león', '00999999', 'tacna');

create or replace table taller_dbt_raw.productos (id_producto string, nombre string, categoria string) using delta;

insert into taller_dbt_raw.productos values
('101', ' polo ', ' ROPA '),
('102', 'MOCHILA', 'accesorios'),
('103', ' taza', ' HOGAR'),
('104', 'cuaderno ', 'Papelería'),
('105', 'Gorra', ' accesorios ');

create or replace table taller_dbt_raw.ventas (id_linea string, id_venta string, fecha string, id_cliente string, id_producto string, cantidad string, precio_unitario string, descuento_pct string, estado string) using delta;

insert into taller_dbt_raw.ventas values
('1', '1001', '2026-01-03', '1', '101', '2', '50.00', '0', ' PAGADA '),
('2', '1001', '2026-01-03', '1', '103', '1', '20.00', '0', 'pagada'),
('3', '1002', '2026-01-10', '2', '102', '1', '80.00', '10', 'Pagada'),
('4', '1003', '2026-01-12', '3', '104', '3', '10.00', '0', 'pagada'),
('5', '1004', '2026-01-20', '1', '105', '2', '30.00', '0', ' CANCELADA'),
('6', '1005', '2026-02-02', '4', '101', '1', '50.00', '20', 'pagada'),
('7', '1005', '2026-02-02', '4', '104', '2', '10.00', '0', 'PAGADA'),
('8', '1006', '2026-02-10', '5', '102', '2', '80.00', '0', 'pagada'),
('9', '1007', '2026-02-15', '2', '103', '3', '20.00', '0', 'pendiente '),
('10', '1008', '2026-03-01', '6', '105', '1', '30.00', '0', 'pagada'),
('11', '1009', '2026-03-05', '3', '101', '3', '50.00', '10', 'pagada'),
('12', '1010', '2026-03-08', '5', '103', '2', '20.00', '0', 'pagada');
