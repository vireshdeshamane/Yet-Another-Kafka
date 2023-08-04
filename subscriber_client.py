import socket
import argparse
from time import sleep
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('localhost', 12345))
a = argparse.ArgumentParser()
a.add_argument("--from_beginning", type=str, default="N")
args = a.parse_args()

def handshake():
    while True:
        try:
            message = client.recv(1024).decode('utf-8')
            if message == "Identity(C/P): ":
                print('welcome to the Consumer Section!')
                client.send("C".encode('utf-8'))
            elif message == "Topic(s): ":
                topics = input("Enter the name of the Topic : ")
                client.send(topics.encode('utf-8'))
                sleep(1)
                client.send(args.from_beginning.encode('utf-8'))
                if args.from_beginning == 'Y':
                    msg = client.recv(1024).decode('utf-8')
                    while msg != '0':
                        print(msg)
                        msg = client.recv(1024).decode('utf-8')
                while True:
                    n = input('You have to wait until the producer registers data, do you want to exit? (Y/N)')
                    client.send(n.encode('utf-8'))
                    msg = client.recv(1024).decode('utf-8')
                    if msg == '1':
                        break
                    print(msg)   
                print()
                print()
                break;

        except :
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