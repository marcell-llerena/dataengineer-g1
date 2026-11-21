select
  producto_id,
  producto,
  categoria
from {{ ref('stg_productos') }}
