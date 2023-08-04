import socket
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('localhost', 12345))

def handshake():
    while True:
        try:
            message = client.recv(1024).decode('utf-8')
            if message == "Identity(C/P): ":
                print('welcome to the Producer Section!')
                client.send("P".encode('utf-8'))
            elif message == "Topic(s): ":
                n = input("Enter the name of the Topic : ")
                client.send(n.encode('utf-8'))
            elif message == "Data: ":
                n = input("Enter the Data : ")
                client.send(n.encode('utf-8'))
                msg = client.recv(1024).decode('utf-8')
                if msg == '1':
                    print('success!')
                else:
                    print('error in creating topic')
                print()
                print()
                break;

        except:
            print('Some Error occurred')
            client.close()
            exit()

def receive():

    while True:
        try:
            handshake()
        except:
            print('Some Error occurred')
            client.close()
            break

receive()