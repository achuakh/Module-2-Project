cd /home/swlai/NTU_DSAI/olist
conda activate dwh
dbt debug
dbt deps
dbt run
dbt test
dbt docs generate
dbt docs serve
http://localhost:8080    <-- DBT Documentation Portal
