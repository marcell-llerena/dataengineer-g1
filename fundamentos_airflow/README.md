### Crear el virtual environment

```bash
uv venv --python 3.12
```

### Adicionar la librería airflow al environment

```bash
uv pip install \
  "apache-airflow==3.3.2" \
  "apache-airflow-providers-standard==1.19.0" \
  "apache-airflow-providers-postgres==7.0.2" \
  "apache-airflow-providers-common-sql==2.1.1" \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
```
