import socket
from time import sleep

zk_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
zk_server.bind(('127.0.0.1', 16969))
zk_server.listen()

conn1, addr1 = zk_server.accept()
conn2, addr2 = zk_server.accept()
conn3, addr3 = zk_server.accept()

print(addr1)
print(addr2)
print(addr3)
conn1.send("1".encode('utf-8'))
conn2.send("2".encode('utf-8'))
conn3.send("3".encode('utf-8'))

h1 = conn1.recv(1024).decode('utf-8')
while h1 == "on":
	conn1.send("1".encode('utf-8'))
	conn2.send("2".encode('utf-8'))
	conn3.send("3".encode('utf-8'))
	h1 = conn1.recv(1024).decode('utf-8')

# h2 = zk.recv(1024).decode('utf-8')
# while h2:
# 	conn2.send("1".encode('utf-8'))
# 	h2 = zk.recv(1024).decode('utf-8')

# h3 = zk.recv(1024).decode('utf-8')
# conn3.send("1".encode('utf-8'))
# while h2:
# 	conn2.send("2".encode('utf-8'))
# 	h2 = zk.recv(1024).decode('utf-8')