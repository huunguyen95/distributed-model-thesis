import os
import socket
import threading

'''
This module have responsibility:
1. Receive raw data from IoT Agent
2. Send result to dashboard
'''


class SocketServer:
    def __init__(self,
                 server_ip="127.0.0.1",
                 server_port=4456):
        super().__init__()
        self.addr = (server_ip, server_port)
        self.size = 4096
        self.format = "utf-8"
        self.server_data_path = "server_data/test_dataset"
        self.task_state = False
        print("[STARTING] Server is starting")
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(self.addr)
        self.server.listen()
        print(f"[LISTENING] Server is listening on {server_ip}:{server_port}.")

    def active_socket_server(self):
        while True:
            conn, addr = self.server.accept()
            thread = threading.Thread(target=self.handle_client, args=(conn, addr))
            thread.start()
            print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")

    def handle_client(self, conn, addr):
        print(f"[NEW CONNECTION] {addr} connected.")
        conn.send("OK@Welcome to the File Server.".encode(self.format))
        name = conn.recv(self.size).decode(self.format)
        print(f"[RECEIVING] {name}")

        conn.send("[RECEIVED] FILE NAME".encode(self.format))
        name = name.split("/")[-1]
        filepath = os.path.join(self.server_data_path, name)

        with open(filepath, "w") as f:
            while True:
                text = conn.recv(self.size).decode(self.format )
                # print(text)
                f.write(text)
                if not text:
                    break
        f.close()
        self.task_state = True
        print(f"[DISCONNECTED] {addr} disconnected")
        conn.close()
        print(f"[TASK] {name}\n")

    def get_task_state(self):
        return self.task_state

    def send_result(self, dashboard_ip="192.168.2.10", dashboard_port=4457, result_file="./model/result/pred_result"):
        self.task_state = False
        dashboard_addr = (dashboard_ip, dashboard_port)

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(dashboard_addr)
        with open(result_file, "r") as file:
            data = file.read()
        file.close()
        client.send(data.encode(self.format))
        print(f"UPLOADED: {data}")
        client.close()

