import grpc
from concurrent import futures
from confluent_kafka import Producer
from elasticsearch import Elasticsearch
import json
import pandas as pd
import os
import logging
import compra_pb2_grpc
from compra_pb2 import CompraResponse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
class Compra:
    def __init__(self, nombre_producto, precio, pasarela_pago, marca_tarjeta, 
                 banco, region_envio, direccion_envio, correo_cliente):
        self.nombre_producto = nombre_producto
        self.precio = precio
        self.pasarela_pago = pasarela_pago
        self.marca_tarjeta = marca_tarjeta
        self.banco = banco
        self.region_envio = region_envio
        self.direccion_envio = direccion_envio
        self.correo_cliente = correo_cliente

    def to_dict(self):
        return {
            "nombre_producto": self.nombre_producto,
            "precio": self.precio,
            "pasarela_pago": self.pasarela_pago,
            "marca_tarjeta": self.marca_tarjeta,
            "banco": self.banco,
            "region_envio": self.region_envio,
            "direccion_envio": self.direccion_envio,
            "correo_cliente": self.correo_cliente
        }

class CompraService(compra_pb2_grpc.CompraServiceServicer):
    def __init__(self, productor):
        self.productor = productor

    def EnviarCompra(self, request, context):
        compra_data = {
            "nombre_producto": request.nombre_producto,
            "precio": request.precio,
            "pasarela_pago": request.pasarela_pago,
            "marca_tarjeta": request.marca_tarjeta,
            "banco": request.banco,
            "region_envio": request.region_envio,
            "direccion_envio": request.direccion_envio,
            "correo_cliente": request.correo_cliente
        }
        try:
            kafka_data =  json.dumps(compra_data).encode('utf-8')                 
            self.productor.produce('orders_topic', value=kafka_data)
            self.productor.flush()
            logging.info(f"Compra recibida y enviada a Kafka: {compra_data}")
            es.index(index="compras", body={
                "nombre_producto": request.nombre_producto,
                "precio": request.precio,
                "pasarela_pago": request.pasarela_pago,
                "marca_tarjeta": request.marca_tarjeta,
                "banco": request.banco,
                "region_envio": request.region_envio,
                "direccion_envio": request.direccion_envio,
                "correo_cliente": request.correo_cliente,
                "timestamp": pd.Timestamp.now().isoformat() 
            })
            logging.info(f"Métrica de compra registrada en Elasticsearch: {compra_data}")

            return CompraResponse(message="Compra recibida y enviada a Kafka y Elasticsearch.", 
                                  success=True)
        except Exception as e:
            logging.error(f"Error al enviar a Kafka o Elasticsearch: {e}")
            return CompraResponse(message="Error al enviar la compra a Kafka o Elasticsearch.", 
                                  success=False)

def iniciar_servidor():
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
    productor = Producer({
        'bootstrap.servers':'localhost:9093',
        'batch.num.messages':16384 
    })

    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    compra_pb2_grpc.add_CompraServiceServicer_to_server(CompraService(productor), servidor)
    servidor.add_insecure_port('[::]:50051')
    servidor.start()
    logging.info("Servidor gRPC iniciado en el puerto 50051.")
    servidor.wait_for_termination()

def cargar_datos_csv(csv_path):
    data = pd.read_csv(csv_path)
    print("Columnas en el dataset:", data.columns)
    return []

if __name__ == "__main__":
    csv_path = '/home/tomas/Descargas/online_retail_sales_dataset.csv' 
    compras = cargar_datos_csv(csv_path)
    iniciar_servidor()
