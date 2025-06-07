from pymongo import MongoClient
# from datetime import datetime
from time import time

client = MongoClient("")
db = client["TABD"]


pipeline = [
  { "$match": { "email": "mcasa-grande@example.org" } },
  {
    "$project": {
      "nome": 1,
      "email": 1,
      "pedidos": { "$slice": [{ "$reverseArray": "$pedidos" }, 3] }
    }
  }
]

for _ in range(5):
    inicio = time()
    resultado = list(db.Cliente.aggregate(pipeline))
    if len(resultado) == 0:
        print("Query inválida.")
        break
    fim = time()

    print(f"Tempo de execução: {fim - inicio:.4f} segundos")
    