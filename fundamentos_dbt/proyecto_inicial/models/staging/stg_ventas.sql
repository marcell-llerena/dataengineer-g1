select
    cast(id_linea as integer) as linea_id,
    cast(id_venta as integer) as venta_id,
    cast(fecha as date) as fecha,
    cast(id_cliente as integer) as cliente_id,
    cast(id_producto as integer) as producto_id,
    cast(cantidad as integer) as cantidad,
    cast(precio_unitario as decimal(12,2)) as precio_unitario,
    cast(descuento_pct as decimal(5,2)) as descuento_pct,
    lower(trim(estado)) as estado
    -- TODO: venta_id, fecha, cliente_id, producto_id, cantidad,
    -- precio_unitario, descuento_pct y estado
from {{ source('tienda', 'ventas') }}
