from cassandra.cluster import Cluster
from uuid import uuid4
from datetime import datetime, timedelta
from faker import Faker
import random
from time import time

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('teste')

inicio = time()

fake = Faker('pt_BR')

# Constantes de volume
NUM_CLIENTES = 20000
NUM_PRODUTOS = 5000
NUM_PEDIDOS = 30000

# Clientes
clientes = []

for _ in range(NUM_CLIENTES):
    cliente_id = uuid4()
    email = fake.unique.email()
    nome = fake.name()
    telefone = fake.phone_number()
    cpf = fake.cpf()
    clientes.append({
        "id": cliente_id,
        "email": email,
        "nome": nome,
        "telefone": telefone,
        "cpf": cpf
    })
    session.execute("""
        INSERT INTO clientes_by_email (email, id_cliente, nome, telefone, cpf)
        VALUES (%s, %s, %s, %s, %s)
    """, (email, cliente_id, nome, telefone, cpf))

# Produtos
produtos = []
vendas_count = {}  

categorias = ['Eletrônicos', 'Roupas', 'Livros', 'Brinquedos', 'Alimentos']
for _ in range(NUM_PRODUTOS):
    produto_id = uuid4()
    nome = fake.word().capitalize()
    categoria = random.choice(categorias)
    preco = round(random.uniform(10.0, 1000.0), 2)
    estoque = random.randint(10, 500)
    produtos.append({
        "id": produto_id,
        "nome": nome,
        "categoria": categoria,
        "preco": preco
    })
    vendas_count[produto_id] = 0
    session.execute("""
        INSERT INTO produtos_by_categoria (categoria, preco, id_produto, nome, estoque)
        VALUES (%s, %s, %s, %s, %s)
    """, (categoria, preco, produto_id, nome, estoque))

# Pedidos e pagamentos
periodo_gastos = {}  

status_pedidos = ['PENDENTE', 'CONCLUIDO', 'CANCELADO']
tipos_pagamento = ['PIX', 'CARTAO', 'BOLETO']

for _ in range(NUM_PEDIDOS):
    cliente = random.choice(clientes)
    cliente_id = cliente["id"]
    data_pedido = fake.date_time_between(start_date='-1y', end_date='now')
    id_pedido = uuid4()
    status = random.choice(status_pedidos)
    num_itens = random.randint(1, 5)

    itens = random.sample(produtos, num_itens)
    valor_total = 0.0
    for prod in itens:
        valor_total += prod["preco"]
        vendas_count[prod["id"]] += 1

    session.execute("""
        INSERT INTO pedidos_by_cliente (id_cliente, data_pedido, id_pedido, status, valor_total)
        VALUES (%s, %s, %s, %s, %s)
    """, (cliente_id, data_pedido, id_pedido, status, round(valor_total, 2)))

    tipo = random.choice(tipos_pagamento)
    data_pagamento = data_pedido + timedelta(days=random.randint(0, 5))
    session.execute("""
        INSERT INTO pagamentos_por_tipo_data (tipo, data_pagamento, id_pedido, status)
        VALUES (%s, %s, %s, %s)
    """, (tipo, data_pagamento, id_pedido, status))

    periodo = data_pedido.strftime("%Y-%m")
    chave = (cliente_id, periodo)
    periodo_gastos[chave] = periodo_gastos.get(chave, 0.0) + valor_total

for produto_id, total_vendido in vendas_count.items():
    nome = next(p["nome"] for p in produtos if p["id"] == produto_id)
    session.execute("""
        INSERT INTO produtos_mais_vendidos (id_produto, nome, total_vendido)
        VALUES (%s, %s, %s)
    """, (produto_id, nome, total_vendido))

for (cliente_id, periodo), valor_total in periodo_gastos.items():
    session.execute("""
        INSERT INTO gasto_cliente_por_periodo (id_cliente, periodo, valor_total)
        VALUES (%s, %s, %s)
    """, (cliente_id, periodo, round(valor_total, 2)))

print("População das tabelas Cassandra concluída com sucesso.")
print(f"CASSANDRA: Dados inseridos em {time() - inicio:.2f}s")
