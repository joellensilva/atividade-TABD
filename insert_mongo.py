# pip install pymongo faker

from pymongo import MongoClient
from faker import Faker
import random
from datetime import datetime, timedelta
from time import time

fake = Faker('pt_BR')
client = MongoClient('mongodb+srv://tabdteste:tabdtesteifal@cluster0.jywnnc4.mongodb.net/')
db = client['TABD']
clientes_col = db['Cliente']
produtos_col = db['Produto']

clientes_col.delete_many({})
produtos_col.delete_many({})

input("Pressione Enter para continuar...")

print("Inserindo clientes e produtos no MongoDB...")
inicio = time()

# Clientes
clientes = []
for _ in range(20000):
    clientes.append({
        "nome": fake.name(),
        "email": fake.email(),
        "telefone": fake.phone_number(),
        "data_cadastro": datetime.combine(fake.date_between(start_date='-2y', end_date='today'), datetime.min.time()),
        "cpf": fake.cpf(),
        "pedidos": []
    })
clientes_col.insert_many(clientes)

# Produtos
categorias = ['Eletrônicos', 'Roupas', 'Livros', 'Alimentos', 'Móveis']
produtos = []
for _ in range(5000):
    produtos.append({
        "nome": fake.word(),
        "categoria": random.choice(categorias),
        "preco": round(random.uniform(10, 1000), 2),
        "estoque": random.randint(1, 1000)
    })
produtos_col.insert_many(produtos)

print("Inserindo pedidos com pagamentos aninhados...")
produtos = list(produtos_col.find())
clientes = list(clientes_col.find())

for _ in range(30000):
    cliente = random.choice(clientes)
    qtd_itens = random.randint(1, 5)
    itens = []
    total = 0
    for _ in range(qtd_itens):
        produto = random.choice(produtos)
        qtd = random.randint(1, 5)
        itens.append({
            "produto_id": produto['_id'],
            "nome": produto['nome'],
            "quantidade": qtd,
            "preco_unitario": produto['preco']
        })
        total += produto['preco'] * qtd

    pedido = {
        "_id": fake.uuid4(),
        "data_pedido": datetime.combine(fake.date_time_between(start_date='-1y', end_date='now'), datetime.min.time()),
        "status": random.choice(['pendente', 'enviado', 'entregue', 'cancelado']),
        "itens": itens,
        "valor_total": total,
        "pagamento": {
            "tipo": random.choice(['PIX', 'Cartão', 'Boleto']),
            "status": "concluido",
            "data_pagamento": datetime.combine(datetime.now() + timedelta(days=random.randint(0, 5)), datetime.min.time())
        }
    }

    clientes_col.update_one({"_id": cliente['_id']}, {"$push": {"pedidos": pedido}})

print(f"MongoDB: Dados inseridos em {time() - inicio:.2f}s")
