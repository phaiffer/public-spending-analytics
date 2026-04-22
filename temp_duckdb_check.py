import duckdb

rel = duckdb.read_parquet(
    r"data/staging/portal_transparencia/recebimentos_recursos_por_favorecido/202601_RecebimentosRecursosPorFavorecido.parquet"
)

result = rel.aggregate(
    "count(*) as row_count, min(amount_received_brl) as min_amount, max(amount_received_brl) as max_amount"
).df()

print(result.to_string(index=False))
