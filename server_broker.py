import socket
import threading
from time import sleep
import os
data = dict()

if 'broker1' not in os.listdir(os.getcwd()):
  os.makedirs(os.getcwd() + '/broker1')
def client(conn1, conn2):
    while True:
        conn1.send('Identity(C/P): '.encode('utf-8'))
        msg = conn1.recv(1024).decode('utf-8')
        conn1.send('Topic(s): '.encode('utf-8'))
        msg1 = conn1.recv(1024).decode('utf-8')
        if msg1 not in os.listdir(os.getcwd() + '/broker1'):
          os.makedirs(os.getcwd() + '/broker1' + '/' + msg1)
          part1 = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition1.txt', 'w')
          part2 = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition2.txt', 'w')
          part3 = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition3.txt', 'w')
          part1.close()
          part2.close()
          part3.close()
        if msg == "P":
            conn1.send('Data: '.encode('utf-8'))
            msg2 = conn1.recv(1024).decode('utf-8')
            if msg1 in data.keys():
              partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(data[msg1] + 1) + '.txt', 'a')
              partn.writelines(msg2 + '\n')
              partn.close()
              data[msg1] = (data[msg1] + 1) % 3
              conn1.send('1'.encode('utf-8'))
            else:
              data[msg1] = 0
              partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(data[msg1] + 1) + '.txt', 'a')
              partn.writelines(msg2 + '\n')
              partn.close()
              data[msg1] = (data[msg1] + 1) % 3
              conn1.send('1'.encode('utf-8'))
              
              '''   
              elif message=="delete":
              topicdel=conn.recv(1024).decode('utf-8')
              path=os.getcwd()+'/broker1'
              final=os.path.join(path,topicdel)
              shutil.rmtree(final)
              ans=" Deleted the topic  "+topicdel
              conn.send(ans.encode('utf-8'))
              '''
        elif msg == 'C':
            msg3 = conn1.recv(1024).decode('utf-8')
            if msg3 == 'Y':
              p1 = 1; p2 = 1; p3 = 1
              line = 0
              while p1 != 0 or p2 != 0 or p3 != 0:
                for i in range(3):
                  partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(i + 1) + '.txt', 'r')
                  dt = partn.readlines()
                  if len(dt) > line:
                    conn1.send(dt[line].encode('utf-8'))
                  else:
                    if i == 0:
                      p1 = 0
                    if i == 1:
                      p2 = 0
                    if i == 2:
                      p3 = 0
                  partn.close()
                line = line + 1
              conn1.send('0'.encode('utf-8'))
            if msg1 in data.keys():
              partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(data[msg1] + 1) + '.txt', 'r')
              dt = partn.readlines()
              size = len(dt)
              partn.close()
              msg4 = conn1.recv(1024).decode('utf-8')
              if msg4 == 'Y':
                conn1.send('1'.encode('utf-8'))
              while True:
                if msg4 == 'Y':
                  break
                partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(data[msg1] + 1) + '.txt', 'r')
                dt = partn.readlines()
                if len(dt) > size:
                  for i in range(size, len(dt)):
                    conn1.send(dt[i].encode('utf-8'))
                size = len(dt)
                partn.close()
            else:
              data[msg1] = 0
              partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(data[msg1] + 1) + '.txt', 'r')
              size = len(partn.readlines())
              partn.close()
              msg4 = conn1.recv(1024).decode('utf-8')
              if msg4 == 'Y':
                conn1.send('1'.encode('utf-8'))
              while True:
                if msg4 == 'Y':
                  break
                partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(data[msg1] + 1) + '.txt', 'r')
                dt = partn.readlines()
                if len(dt) > size:
                  for i in range(size, len(dt)):
                    conn1.send(dt[i].encode('utf-8'))
                size = len(dt)
                partn.close()
              conn1.send('1'.encode('utf-8'))

    conn1.close()


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('127.0.0.1', 12345))
server.listen()

special_server1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
special_server2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

special_server1.bind(('127.0.0.1', 11111))
special_server2.bind(('127.0.0.1', 11112))

special_server1.listen()
special_server2.listen()

while True:
  conn1, addr = server.accept()
  conn2, addr = special_server1.accept()
  if leader:
    thread = threading.Thread(target=client, args=(conn1, conn2))
  else:
    thread = threading.Thread(target=client, args=(conn2, conn1))
  thread.start()

  
