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
        '''
        CALL THE information-stategy model for event task assignment
        
        '''

        print(f"[DISCONNECTED] {addr} disconnected")
        conn.close()
        print(f"[TASK] {name}\n")

    def get_task_state(self):
        return self.task_state

    def send_result(self, predict_result="benign", dashboard_ip="192.168.2.10", dashboard_port=4456):
        self.task_state = False
        dashboard_addr = (dashboard_ip, dashboard_port)

        dataset = ["./client_data/scg/data/malware/", "./client_data/scg/data/benign/",
                   "./client_data/scg/data/new_malware/"]

        # client_data = "./client_data/scg/data/malware/"

        def main():
            for client_data in dataset:
                for filename in os.listdir(client_data):
                    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client.connect(ADDR)
                    data = client.recv(SIZE).decode(FORMAT)
                    print(data)
                    f = os.path.join(client_data, filename)
                    if os.path.isfile(f):
                        print("Uploading: ", f)
                        with open(f"{f}", "r") as file:
                            text = file.read()
                    send_filename = f"{f}"
                    client.send(send_filename.encode(FORMAT))
                    print(f"{client.recv(SIZE).decode(FORMAT)}")
                    send_data = f"{text}"
                    # print(send_data)
                    client.sendall(send_data.encode(FORMAT))
                    # ack1 = client.recv(SIZE).decode(FORMAT)
                    # print(f"Server ACK1: {ack1}")
                    print(f"UPLOADED: {filename}")
                    client.close()


'''
#IP = socket.gethostbyname(socket.gethostname())
IP = "192.168.2.10"
PORT = 4456
ADDR = (IP, PORT)
SIZE = 4096
FORMAT = "utf-8"
SERVER_DATA_PATH = ["server_data/malware", "server_data/benign", "server_data/new_malware"]


def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")
    conn.send("OK@Welcome to the File Server.".encode(FORMAT))
    name = conn.recv(SIZE).decode(FORMAT)
    print(f"[RECEIVING] {name}")

    conn.send("[RECEIVED] FILE NAME".encode(FORMAT))
    if "./client_data/scg/data/malware/" in name:
        name = name.split("/")[-1]
        filepath = os.path.join(SERVER_DATA_PATH[0], name)
    elif "./client_data/scg/data/benign/" in name:
        name = name.split("/")[-1]
        filepath = os.path.join(SERVER_DATA_PATH[1], name)
    else:
        name = name.split("/")[-1]
        filepath = os.path.join(SERVER_DATA_PATH[2], name)
    with open(filepath, "w") as f:
        while True:
            text = conn.recv(SIZE).decode(FORMAT)
            #print(text)
            f.write(text)
            if not text:
                break

    f.close()
    #receiving1 = f"RECEIVING CONTENT"
    #conn.send(receiving1.encode(FORMAT))

    print(f"[DISCONNECTED] {addr} disconnected")
    conn.close()


def main():
    print("[STARTING] Server is starting")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print(f"[LISTENING] Server is listening on {IP}:{PORT}.")

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")


if __name__ == "__main__":
    main()
'''