select
    cast(id_cliente as integer) as cliente_id,
    initcap(trim(nombre)) as nombre,
    trim(dni) as dni,
    initcap(trim(ciudad)) as ciudad
    -- TODO: normalizar nombre, conservar DNI textual, normalizar ciudad
from {{ source('tienda', 'clientes') }}
