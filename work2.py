import socket
import threading
from time import sleep
import os
data = dict()

os.makedirs(os.getcwd() + '/broker2')
def client(conn, conn2, conn3):
  while True:
    print('Hiiii')
    conn.send('Identity(C/P): '.encode('utf-8'))
    msg = conn.recv(1024).decode('utf-8')
    print(msg)
    conn.send('Topic(s): '.encode('utf-8'))
    msg1 = conn.recv(1024).decode('utf-8')
    if msg1 not in os.listdir(os.getcwd() + '/broker1'):
      s2  = f"""os.makedirs(os.getcwd() + '/broker2' + '/' + '{msg1}')
part1 = open(os.getcwd() + '/broker2' + '/' + '{msg1}' + '/' + 'Partition1.txt', 'w')
part2 = open(os.getcwd() + '/broker2' + '/' + '{msg1}' + '/' + 'Partition2.txt', 'w')
part3 = open(os.getcwd() + '/broker2' + '/' + '{msg1}' + '/' + 'Partition3.txt', 'w')
part1.close()
part2.close()
part3.close()"""
      s3  = f"""
os.makedirs(os.getcwd() + '/broker3' + '/' + '{msg1}')
part1 = open(os.getcwd() + '/broker3' + '/' + '{msg1}' + '/' + 'Partition1.txt', 'w')
part2 = open(os.getcwd() + '/broker3' + '/' + '{msg1}' + '/' + 'Partition2.txt', 'w')
part3 = open(os.getcwd() + '/broker3' + '/' + '{msg1}' + '/' + 'Partition3.txt', 'w')
part1.close()
part2.close()
part3.close()"""
      conn2.send(s2.encode('utf-8'))
      conn3.send(s3.encode('utf-8'))
      os.makedirs(os.getcwd() + '/broker1' + '/' + msg1)
      part1 = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition1.txt', 'w')
      part2 = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition2.txt', 'w')
      part3 = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition3.txt', 'w')
      part1.close()
      part2.close()
      part3.close()
    if msg == "P":
      conn.send('Data: '.encode('utf-8'))
      msg2 = conn.recv(1024).decode('utf-8')
      print(data)
      if msg1 in data.keys():
        print(data)
        s2 = f"""
partn = open(os.getcwd() + '/broker2' + '/' + '{msg1}' + '/' + 'Partition' + str(data['{msg1}'] + 1) + '.txt', 'a')
partn.writelines('{msg2}' + '\\n')
partn.close()
data['{msg1}'] = (data['{msg1}'] + 1) % 3"""
        s3 = f"""
partn = open(os.getcwd() + '/broker3' + '/' + '{msg1}' + '/' + 'Partition' + str(data['{msg1}'] + 1) + '.txt', 'a')
partn.writelines('{msg2}' + '\\n')
partn.close()
data['{msg1}'] = (data['{msg1}'] + 1) % 3"""
        print(data)
        conn2.send(s2.encode('utf-8'))
        conn3.send(s3.encode('utf-8'))
        print('FFFFFF')
        partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(data[msg1] + 1) + '.txt', 'a')
        partn.writelines(msg2 + '\\n')
        partn.close()
        data[msg1] = (data[msg1] + 1) % 3
        conn.send('1'.encode('utf-8'))
      else:
        s2 = f"""
data['{msg1}'] = 0
partn = open(os.getcwd() + '/broker2' + '/' + '{msg1}' + '/' + 'Partition' + str(data['{msg1}'] + 1) + '.txt', 'a')
partn.writelines('{msg2}' + '\\n')
partn.close()
data['{msg1}'] = (data['{msg1}'] + 1) % 3"""
        s3 = f"""
data['{msg1}'] = 0
partn = open(os.getcwd() + '/broker3' + '/' + '{msg1}' + '/' + 'Partition' + str(data['{msg1}'] + 1) + '.txt', 'a')
partn.writelines('{msg2}' + '\\n')
partn.close()
data['{msg1}'] = (data['{msg1}'] + 1) % 3"""

        conn2.send(s2.encode('utf-8'))
        conn3.send(s3.encode('utf-8'))
        print('ddsddddddddd')
        data[msg1] = 0
        partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(data[msg1] + 1) + '.txt', 'a')
        partn.writelines(msg2 + '\\n')
        partn.close()
        data[msg1] = (data[msg1] + 1) % 3
        conn.send('1'.encode('utf-8'))

    elif msg == 'C':
      msg3 = conn.recv(1024).decode('utf-8')
      print('#', msg3)
      if msg3 == 'Y':
        p1 = 1; p2 = 1; p3 = 1
        line = 0
        while p1 != 0 or p2 != 0 or p3 != 0:
          for i in range(3):


            s2 = f"""
partn = open(os.getcwd() + '/broker2' + '/' + '{msg1}' + '/' + 'Partition' + str('{i}' + 1) + '.txt', 'r')
dt = partn.readlines()"""
            s3 = f"""
partn = open(os.getcwd() + '/broker3' + '/' + '{msg1}' + '/' + 'Partition' + str('{i}' + 1) + '.txt', 'r')
dt = partn.readlines()"""

            conn2.send(s2.encode('utf-8'))
            conn3.send(s3.encode('utf-8'))

            print("here")

            partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(i + 1) + '.txt', 'r')
            dt = partn.readlines()
            if len(dt) > line:
              conn.send(dt[line].encode('utf-8'))
            else:
              if i == 0:
                p1 = 0
              if i == 1:
                p2 = 0
              if i == 2:
                p3 = 0

            s2 = f"""
partn.close()"""
            s3 = f"""
partn.close()"""

            conn2.send(s2.encode('utf-8'))
            conn3.send(s3.encode('utf-8'))

            partn.close()
          line = line + 1
        conn.send('0'.encode('utf-8'))
      if msg1 in data.keys():

        s2 = f"""
partn = open(os.getcwd() + '/broker2' + '/' + '{msg1}' + '/' + 'Partition' + str(data['{msg1}'] + 1) + '.txt', 'r')
dt = partn.readlines()
size = len(dt)
partn.close()"""
        s3 = f"""
partn = open(os.getcwd() + '/broker3' + '/' + '{msg1}' + '/' + 'Partition' + str(data['{msg1}'] + 1) + '.txt', 'r')
dt = partn.readlines()
size = len(dt)
partn.close()"""

        conn2.send(s2.encode('utf-8'))
        conn3.send(s3.encode('utf-8'))

        partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(data[msg1] + 1) + '.txt', 'r')
        dt = partn.readlines()
        size = len(dt)
        partn.close()
        msg4 = conn.recv(1024).decode('utf-8')
        if msg4 == 'Y':
          conn.send('1'.encode('utf-8'))
        while True:
          if msg4 == 'Y':
            break

          s2 = f"""
partn = open(os.getcwd() + '/broker2' + '/' + '{msg1}' + '/' + 'Partition' + str(data['{msg1}'] + 1) + '.txt', 'r')
dt = partn.readlines()"""
          s3 = f"""
partn = open(os.getcwd() + '/broker3' + '/' + '{msg1}' + '/' + 'Partition' + str(data['{msg1}'] + 1) + '.txt', 'r')
dt = partn.readlines()"""

          conn2.send(s2.encode('utf-8'))
          conn3.send(s3.encode('utf-8'))

          partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(data[msg1] + 1) + '.txt', 'r')
          dt = partn.readlines()
          if len(dt) > size:
            for i in range(size, len(dt)):
              conn.send(dt[i].encode('utf-8'))

          size = len(dt)

          s2 = f"""
partn.close()"""
          s3 = f"""
partn.close()"""

          conn2.send(s2.encode('utf-8'))
          conn3.send(s3.encode('utf-8'))

          partn.close()
      else:
        s2 = f"""
data['{msg1}'] = 0
partn = open(os.getcwd() + '/broker2' + '/' + '{msg1}' + '/' + 'Partition' + str(data['{msg1}'] + 1) + '.txt', 'r')
partn.close()"""
        s3 = f"""
data['{msg1}'] = 0
partn = open(os.getcwd() + '/broker3' + '/' + '{msg1}' + '/' + 'Partition' + str(data['{msg1}'] + 1) + '.txt', 'r')
partn.close()"""

        conn2.send(s2.encode('utf-8'))
        conn3.send(s3.encode('utf-8'))

        data[msg1] = 0
        print('HERE')
        partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(data[msg1] + 1) + '.txt', 'r')
        size = len(partn.readlines())
        partn.close()
        msg4 = conn.recv(1024).decode('utf-8')
        if msg4 == 'Y':
          conn.send('1'.encode('utf-8'))
        while True:
          if msg4 == 'Y':
            break

          s2 = f"""
partn = open(os.getcwd() + '/broker2' + '/' + '{msg1}' + '/' + 'Partition' + str(data['{msg1}'] + 1) + '.txt', 'r')
dt = partn.readlines()"""
          s3 = f"""
partn = open(os.getcwd() + '/broker3' + '/' + '{msg1}' + '/' + 'Partition' + str(data['{msg1}'] + 1) + '.txt', 'r')
dt = partn.readlines()"""
          conn2.send(s2.encode('utf-8'))
          conn3.send(s3.encode('utf-8'))

          partn = open(os.getcwd() + '/broker1' + '/' + msg1 + '/' + 'Partition' + str(data[msg1] + 1) + '.txt', 'r')
          dt = partn.readlines()
          if len(dt) > size:
            for i in range(size, len(dt)):
              conn.send(dt[i].encode('utf-8'))
          size = len(dt)
          partn.close()
          s2 = f"""
partn.close()"""
          s3 = f"""
partn.close()"""
          conn2.send(s2.encode('utf-8'))
          conn3.send(s3.encode('utf-8'))

        conn.send('1'.encode('utf-8'))

  conn.close()

zk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
zk.connect(('localhost', 16969))

leader = int(zk.recv(1024).decode('utf-8'))

if leader == 2:
  lead = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  lead.connect(('localhost', 12341))

elif leader == 3:
  lead = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  lead.connect(('localhost', 12342))

while leader != 1:
  message = lead.recv(1024).decode('utf-8')
  exec(message)
  leader = int(zk.recv(1024).decode('utf-8'))

client_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_server.bind(('127.0.0.1', 12345))
client_server.listen()

broker_server_2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
broker_server_2.bind(('127.0.0.1', 12341))
broker_server_2.listen()

broker_server_3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
broker_server_3.bind(('127.0.0.1', 12342))
broker_server_3.listen()

conn2, addr = broker_server_2.accept()
conn3, addr = broker_server_3.accept()

while True:
  if leader == 1:
    zk.send("on".encode('utf-8'))
  conn, addr = client_server.accept()
  thread1 = threading.Thread(target=client, args=(conn,conn2,conn3))
  thread1.start()
  leader = int(zk.recv(1024).decode('utf-8'))