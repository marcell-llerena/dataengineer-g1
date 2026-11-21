select
    cast(id_producto as integer) as producto_id,
    initcap(trim(nombre)) as producto,
    lower(trim(categoria)) as categoria
    -- TODO: producto_id entero, producto con initcap, categoria en minúsculas
from {{ source('tienda', 'productos') }}
