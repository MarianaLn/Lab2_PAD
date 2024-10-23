import grpc
import threading
import pubsub_pb2
import pubsub_pb2_grpc

def receive_messages(stub, subscriber_name):
    request = pubsub_pb2.SubscriberConnectRequest(subscriber_name=subscriber_name)
    # Începem să primim mesaje
    for message in stub.ReceiveMessages(request):
        print(f"\n[Mesaj nou] Topic: {message.topic} - {message.message}")

def main():
    # Creăm un canal gRPC
    channel = grpc.insecure_channel('172.20.10.4:50051')
    stub = pubsub_pb2_grpc.BrokerServiceStub(channel)

    # Solicităm numele subscriber-ului
    subscriber_name = input("Introdu numele subscriber-ului: ")

    # Conectare la broker
    response = stub.ConnectSubscriber(pubsub_pb2.SubscriberConnectRequest(subscriber_name=subscriber_name))
    topics = response.topics
    offline_messages = response.offline_messages

    if topics:
        print(f"Conectat. Ești abonat la: {', '.join(topics)}")
    else:
        print("Conectat. Nu ești abonat la niciun topic.")

    if offline_messages:
        print("Ai mesaje nelivrate:")
        for msg in offline_messages:
            print(f"[Offline] Topic: {msg.topic} - {msg.message}")

    # Pornim un thread pentru a primi mesaje
    threading.Thread(target=receive_messages, args=(stub, subscriber_name), daemon=True).start()

    while True:
        print("\n1. Abonează-te la un topic")
        print("2. Vezi topic-urile abonate")
        print("3. Ieșire")

        choice = input("Alege o opțiune: ")

        if choice == "1":
            topic = input("Introdu numele topic-ului: ")
            response = stub.SubscribeToTopic(pubsub_pb2.SubscribeToTopicRequest(
                subscriber_name=subscriber_name, topic=topic))
            print(response.message)
        elif choice == "2":
            # Obținem topic-urile abonate
            print("Încep conectarea la broker...")
            response = stub.ConnectSubscriber(pubsub_pb2.SubscriberConnectRequest(subscriber_name=subscriber_name))
            print("Conectare realizată")
            topics = response.topics
            if topics:
                print(f"Ești abonat la: {', '.join(topics)}")
            else:
                print("Nu ești abonat la niciun topic.")
        elif choice == "3":
            print("Ieșire din subscriber.")
            break
        else:
            print("Opțiune invalidă.")

if __name__ == '__main__':
    main()
