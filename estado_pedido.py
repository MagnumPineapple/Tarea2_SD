from enum import Enum
import time
import random
import json
from confluent_kafka import Producer
from elasticsearch import Elasticsearch
import pandas as pd
import os

# Conexión a Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

class EstadoPedido(Enum):
    PROCESANDO = "Procesando"
    PREPARACION = "Preparación"
    ENVIADO = "Enviado"
    ENTREGADO = "Entregado"
    FINALIZADO = "Finalizado"

def simular_procesamiento_pedidos(pedidos):
    estados = ["Procesando", "Preparación", "Enviado", "Entregado", "Finalizado"]

    for pedido in pedidos:
        for estado in estados:
            tiempo_procesamiento = random.uniform(1, 5)
            time.sleep(tiempo_procesamiento)

            pedido["estado"] = estado
            print(f"Pedido {pedido['id_pedido']} ha cambiado de estado a: {estado}")

class Pedido:
    def __init__(self, id_pedido, productor, topic='estado_pedidos'):
        self.id_pedido = id_pedido
        self.estado = EstadoPedido.PROCESANDO  # Estado inicial
        self.estado_anterior = None
        self.productor = productor
        self.topic = topic
        self.inicio_estado = time.time()
        self.tiempos_procesamiento = {
            EstadoPedido.PROCESANDO: random.uniform(1, 3),
            EstadoPedido.PREPARACION: random.uniform(2, 5),
            EstadoPedido.ENVIADO: random.uniform(3, 6),
            EstadoPedido.ENTREGADO: random.uniform(1, 4),
        }

    def avanzar_estado(self):
        if self.estado == EstadoPedido.FINALIZADO:
            print("El pedido ya está en estado finalizado.")
            return
        
        tiempo_actual = time.time()
        latencia = tiempo_actual - self.inicio_estado
        print(f"Latencia para el estado '{self.estado.value}' del pedido {self.id_pedido}: 
              {latencia:.2f} segundos")

        self.estado_anterior = self.estado

        tiempo_espera = self.tiempos_procesamiento.get(self.estado, 1)
        time.sleep(tiempo_espera)

        if self.estado == EstadoPedido.PROCESANDO:
            self.estado = EstadoPedido.PREPARACION
        elif self.estado == EstadoPedido.PREPARACION:
            self.estado = EstadoPedido.ENVIADO
        elif self.estado == EstadoPedido.ENVIADO:
            self.estado = EstadoPedido.ENTREGADO
        elif self.estado == EstadoPedido.ENTREGADO:
            self.estado = EstadoPedido.FINALIZADO

        self.registrar_metricas_elasticsearch(latencia)
        self.inicio_estado = time.time()
        self.publicar_estado()

    def registrar_metricas_elasticsearch(self, latencia):
        tiempo_actual = time.time()
        data = {
            "id_pedido": self.id_pedido,
            "estado": self.estado.value,
            "estado_anterior": self.estado_anterior.value if self.estado_anterior else None,
            "latencia": latencia,
            "tiempo_procesamiento": tiempo_actual - self.inicio_estado,
            "timestamp": pd.Timestamp.now().isoformat(),
            "throughput_timestamp": int(tiempo_actual * 1000),
            "tiempo_inicio_estado": pd.Timestamp.fromtimestamp(self.inicio_estado).isoformat(),
            "tiempo_fin_estado": pd.Timestamp.fromtimestamp(tiempo_actual).isoformat(),
            "tiempo_espera_simulado": self.tiempos_procesamiento.get(self.estado, 1)
        }
        
        try:
            es.index(index="estado_pedidos", body=data)
            print(f"Métricas registradas en Elasticsearch: {data}")
        except Exception as e:
            print(f"Error al registrar métricas en Elasticsearch: {e}")

    def publicar_estado(self):
        data = {
            "id_pedido": self.id_pedido,
            "estado": self.estado.value,
            "timestamp": pd.Timestamp.now().isoformat()
        }
        try:
            kafka_data = json.dumps(data).encode('utf-8')
            self.productor.produce(self.topic, value=kafka_data)
            self.productor.flush()
            print(f"Estado del pedido {self.id_pedido} enviado a Kafka: {self.estado.value}")
        except Exception as e:
            print(f"Error al enviar el estado del pedido a Kafka: {e}")

def crear_productor_kafka():
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
    return Producer({
        'bootstrap.servers':'localhost:9093'
    })

def cargar_datos_csv(csv_path):
    data = pd.read_csv(csv_path)
    lista_pedidos = data['transaction_id'].tolist()
    return lista_pedidos

if __name__ == "__main__":
    csv_path = '/home/tomas/Descargas/online_retail_sales_dataset.csv'
    lista_pedidos = cargar_datos_csv(csv_path)
    productor_kafka = crear_productor_kafka()
    for id_pedido in lista_pedidos:
        pedido = Pedido(id_pedido=id_pedido, productor=productor_kafka)
        print(f"Estado inicial del pedido {pedido.id_pedido}: {pedido.estado.value}")

        pedido.avanzar_estado()  # Procesando -> Preparación
        pedido.avanzar_estado()  # Preparación -> Enviado
        pedido.avanzar_estado()  # Enviado -> Entregado
        pedido.avanzar_estado()  # Entregado -> Finalizado
        print(f"Pedido {pedido.id_pedido} finalizado.")
