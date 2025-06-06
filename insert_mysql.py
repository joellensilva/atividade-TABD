# pip install faker mysql-connector-python

from faker import Faker
from datetime import datetime, timedelta
import random
import mysql.connector
from time import time

fake = Faker('pt_BR')

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="sys"
)
cursor = conn.cursor()

print("Início da inserção...")
inicio = time()

# INSERIR CLIENTES
print("Inserindo clientes...")
clientes = [(fake.name(), fake.unique.email(), fake.phone_number(),
             fake.date_between(start_date='-2y', end_date='today'),
             fake.cpf()) for _ in range(20000)]
cursor.executemany("""
    INSERT INTO cliente (nome, email, telefone, data_cadastro, cpf)
    VALUES (%s, %s, %s, %s, %s)
""", clientes)
conn.commit()
print("Clientes inseridos.")

# INSERIR PRODUTOS
print("Inserindo produtos...")
categorias = ['Eletrônicos', 'Roupas', 'Livros', 'Alimentos', 'Móveis']
produtos = [(fake.word(), random.choice(categorias),
             round(random.uniform(10, 1000), 2),
             random.randint(1, 1000)) for _ in range(5000)]
cursor.executemany("""
    INSERT INTO produto (nome, categoria, preco, estoque)
    VALUES (%s, %s, %s, %s)
""", produtos)
conn.commit()
print("Produtos inseridos.")

# INSERIR PEDIDOS, ITENS E PAGAMENTOS
print("Inserindo pedidos, itens e pagamentos...")
status_pedidos = ['pendente', 'enviado', 'entregue', 'cancelado']
tipo_pagamento = ['PIX', 'Cartão', 'Boleto']

for i in range(1, 30001):
    id_cliente = random.randint(1, 20000)
    data_pedido = fake.date_time_between(start_date='-1y', end_date='now')
    status = random.choice(status_pedidos)

    qtd_itens = random.randint(1, 5)
    itens = []
    valor_total = 0

    for _ in range(qtd_itens):
        id_produto = random.randint(1, 5000)
        quantidade = random.randint(1, 5)
        cursor.execute("SELECT preco FROM produto WHERE id = %s", (id_produto,))
        preco = cursor.fetchone()[0]
        valor_total += preco * quantidade
        itens.append((id_produto, quantidade))

    cursor.execute("""
        INSERT INTO pedido (id_cliente, data_pedido, status, valor_total)
        VALUES (%s, %s, %s, %s)
    """, (id_cliente, data_pedido, status, valor_total))
    id_pedido = cursor.lastrowid

    for id_produto, quantidade in itens:
        cursor.execute("""
            INSERT INTO item_pedido (id_pedido, id_produto, quantidade)
            VALUES (%s, %s, %s)
        """, (id_pedido, id_produto, quantidade))

    pagamento = (
        id_pedido,
        random.choice(tipo_pagamento),
        'concluido',
        data_pedido + timedelta(days=random.randint(0, 5))
    )
    cursor.execute("""
        INSERT INTO pagamento (id_pedido, tipo, status, data_pagamento)
        VALUES (%s, %s, %s, %s)
    """, pagamento)

    if i % 1000 == 0:
        conn.commit()
        print(f"{i} pedidos inseridos...")

conn.commit()
print(f"Tudo inserido em {time() - inicio:.2f}s")
conn.close()
