select
    linea_id, venta_id, fecha, cliente_id, producto_id,
    cantidad, precio_unitario, descuento_pct, estado,
    round(cantidad * precio_unitario, 2) as venta_bruta,
    round(cantidad * precio_unitario * descuento_pct / 100, 2) as descuento,
    round((cantidad * precio_unitario) - (cantidad * precio_unitario * descuento_pct / 100), 2) as venta_neta
    -- TODO: venta_bruta, descuento monetario y venta_neta
from {{ ref('stg_ventas') }}
