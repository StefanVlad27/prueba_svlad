from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient

kafka_conf = {
    'bootstrap.servers': 'b-2-public.arq6kafka.vhgjgn.c6.kafka.eu-west-1.amazonaws.com:9196,b-1-public.arq6kafka.vhgjgn.c6.kafka.eu-west-1.amazonaws.com:9196',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'kafka',
    'sasl.password': 'kafka1'
}

print('Creando el cliente administrativo Kafka...')
admin_client = AdminClient(kafka_conf)
print('Cliente administrativo Kafka creado.')

print('Obteniendo lista de tópicos...')
topics = admin_client.list_topics().topics
print('Tópicos obtenidos exitosamente.')

for topic in topics:
    print(f'Tópico: {topic}')
