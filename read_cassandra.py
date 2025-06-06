import time
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1']) 
session = cluster.connect('teste')

query = """
  SELECT periodo, valor_total
  FROM gasto_cliente_por_periodo
  WHERE id_cliente = 96cd1ae7-2998-4b10-b9ab-28a3d4041ab8
    AND periodo IN ('2025-03', '2025-04', '2025-05');"""

inicio = time.time()
rows = session.execute(query)
fim = time.time()

for row in rows:
    print(row)

print(f"Tempo de execução: {fim - inicio:.4f} segundos")
