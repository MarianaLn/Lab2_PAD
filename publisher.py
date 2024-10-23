import grpc
import pubsub_pb2
import pubsub_pb2_grpc

def main():
    # Creăm un canal gRPC
    channel = grpc.insecure_channel('172.20.10.4:50051')
    stub = pubsub_pb2_grpc.BrokerServiceStub(channel)

    # Solicităm numele publisher-ului
    publisher_name = input("Introdu numele publisher-ului: ")

    # Conectare la broker
    response = stub.ConnectPublisher(pubsub_pb2.PublisherConnectRequest(publisher_name=publisher_name))
    topics = response.topics
    if topics:
        print(f"Conectat. Topic-urile tale: {', '.join(topics)}")
    else:
        print("Conectat. Nu ai niciun topic.")

    while True:
        print("\n1. Creează un topic nou")
        print("2. Trimite un mesaj")
        print("3. Ieșire")

        choice = input("Alege o opțiune: ")

        if choice == "1":
            topic = input("Introdu numele noului topic: ")
            response = stub.CreateTopic(pubsub_pb2.CreateTopicRequest(publisher_name=publisher_name, topic=topic))
            print(response.message)
        elif choice == "2":
            topic = input("Introdu topic-ul: ")
            message = input("Introdu mesajul: ")
            response = stub.PublishMessage(pubsub_pb2.PublishMessageRequest(
                publisher_name=publisher_name, topic=topic, message=message))
            print(response.message)
        elif choice == "3":
            print("Ieșire din publisher.")
            break
        else:
            print("Opțiune invalidă.")

if __name__ == '__main__':
    main()
