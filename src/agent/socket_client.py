import socket
import os

# IP = socket.gethostbyname(socket.gethostname())
IP = "192.168.2.10"
PORT = 4456
ADDR = (IP, PORT)
FORMAT = "utf-8"
SIZE = 4096
#dataset = ["./client_data/scg/data/malware/", "./client_data/scg/data/benign/", "./client_data/scg/data/new_malware/"]
#client_data = "./client_data/scg/data/malware/"
dataset = ["./client_data/scg/data/malware/"]

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
            #print(send_data)
            client.sendall(send_data.encode(FORMAT))
            # ack1 = client.recv(SIZE).decode(FORMAT)
            # print(f"Server ACK1: {ack1}")
            print(f"UPLOADED: {filename}")
            client.close()


if __name__ == "__main__":
    main()
