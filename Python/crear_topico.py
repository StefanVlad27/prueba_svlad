from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

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

topic_name = input('Introduce el nombre del tópico a crear: ')
new_topic = NewTopic(topic_name, num_partitions=2, replication_factor=1)

print(f'Creando el tópico: {topic_name}')


fs = admin_client.create_topics([new_topic])


for topic, f in fs.items():
    try:
        f.result()  # El resultado en sí mismo es None
        print(f'Tópico {topic_name} creado exitosamente.')
    except Exception as e:
        if 'TopicAlreadyExistsError' in str(e):
            print(f'Tópico {topic_name} ya existe.')
        else:
            print(f'Falló la creación del tópico {topic_name}: {e}')
