select
  cliente_id,
  nombre,
  dni,
  ciudad
from {{ ref('stg_clientes') }}
