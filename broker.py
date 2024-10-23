from concurrent import futures
import grpc
import time
import threading
import pickle
import os
import pubsub_pb2
import pubsub_pb2_grpc

# Persistența datelor
data_file = 'broker_data.pkl'
lock = threading.Lock()

if os.path.exists(data_file):
    with open(data_file, 'rb') as f:
        broker_data = pickle.load(f)
else:
    broker_data = {
        'topics': {},          # {'topic_name': {'publisher': 'publisher_name', 'messages': []}}
        'subscribers': {},     # {'subscriber_name': {'topics': set(), 'queue': []}}
        'publishers': set(),   # set de nume de publisheri
        'offline_messages': {} # {'subscriber_name': [Message, ...]}
    }

def save_data():
    with open(data_file, 'wb') as f:
        pickle.dump(broker_data, f)

# Definirea clasei care implementează serviciul de broker
class BrokerService(pubsub_pb2_grpc.BrokerServiceServicer):
    def ConnectPublisher(self, request, context):
        with lock:  # Blocăm accesul concurent pentru date
            publisher_name = request.publisher_name  # Extragem numele publisherului din request.
            broker_data['publishers'].add(publisher_name)
            # Obținem topicurile create de acest publisher
            topics = [topic for topic, data in broker_data['topics'].items() if data['publisher'] == publisher_name]
            save_data()
            return pubsub_pb2.PublisherConnectResponse(topics=topics) # Returnăm lista topicurilor publisherului.
    
    def CreateTopic(self, request, context):
        with lock:
            publisher_name = request.publisher_name
            topic = request.topic
            if topic in broker_data['topics']:
                return pubsub_pb2.CreateTopicResponse(success=False, message=f"Topic-ul '{topic}' există deja.")
            else:
                broker_data['topics'][topic] = {'publisher': publisher_name, 'messages': []}
                save_data()
                return pubsub_pb2.CreateTopicResponse(success=True, message=f"Topic-ul '{topic}' a fost creat.")
    
    def PublishMessage(self, request, context):
        with lock:
            publisher_name = request.publisher_name
            topic = request.topic
            message_content = request.message # Extragem conținutul mesajului publicat.

            if topic not in broker_data['topics']:
                return pubsub_pb2.PublishMessageResponse(success=False, message=f"Topic-ul '{topic}' nu există.")

            # Adăugăm mesajul la lista de mesaje a topicului
            broker_data['topics'][topic]['messages'].append(message_content)

            # Trimitem mesajul către subscriberi
            message = pubsub_pb2.Message(topic=topic, message=message_content)
            for subscriber_name, subscriber_data in broker_data['subscribers'].items():# Iterăm prin subscriberi.
                if topic in subscriber_data['topics']:
                    subscriber_queue = subscriber_data['queue']
                    subscriber_queue.append(message)  # Adăugăm mesajul în coada de mesaje a subscriberului.

            save_data()
            return pubsub_pb2.PublishMessageResponse(success=True, message="Mesaj publicat.")
    
    def ConnectSubscriber(self, request, context):
        with lock:
            subscriber_name = request.subscriber_name
            if subscriber_name not in broker_data['subscribers']:
                broker_data['subscribers'][subscriber_name] = {'topics': set(), 'queue': []}# Inițializăm abonatul cu topicuri și o coadă goală.
                offline_messages = []
            else:
                offline_messages = broker_data['offline_messages'].get(subscriber_name, []) # Obținem mesajele offline, dacă există.
                if subscriber_name in broker_data['offline_messages']:
                    del broker_data['offline_messages'][subscriber_name]
            topics = list(broker_data['subscribers'][subscriber_name]['topics'])
            save_data()
            return pubsub_pb2.SubscriberConnectResponse(
                topics=topics,
                offline_messages=offline_messages
            )
    
    def SubscribeToTopic(self, request, context):
        with lock:
            subscriber_name = request.subscriber_name
            topic = request.topic
            if topic not in broker_data['topics']:
                return pubsub_pb2.SubscribeToTopicResponse(success=False, message=f"Topic-ul '{topic}' nu există.")
            broker_data['subscribers'][subscriber_name]['topics'].add(topic)
            save_data()
            return pubsub_pb2.SubscribeToTopicResponse(success=True, message=f"Te-ai abonat la '{topic}'.")
    
    def ReceiveMessages(self, request, context):
        subscriber_name = request.subscriber_name
        if subscriber_name not in broker_data['subscribers']:
            context.set_details(f"Subscriber-ul '{subscriber_name}' nu este conectat.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return

        subscriber_data = broker_data['subscribers'][subscriber_name]
        queue = subscriber_data['queue']

        while True:
            with lock:
                if queue:
                    message = queue.pop(0) # Scoatem primul mesaj din coadă.
                    yield message
                else:
                    pass
            time.sleep(0.1)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pubsub_pb2_grpc.add_BrokerServiceServicer_to_server(BrokerService(), server)

    server.add_insecure_port('172.20.10.4:50051')

    server.start()
    print("Broker-ul rulează pe portul 50051 și este accesibil de pe alte calculatoare în aceeași rețea.")
    try:
        while True:
            time.sleep(86400)  # Menține serverul activ
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
