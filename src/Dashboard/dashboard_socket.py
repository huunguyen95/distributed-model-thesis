import os
import socket
import threading


IP = "192.168.2.20"
PORT = 4457
ADDR = (IP, PORT)
SIZE = 4096
FORMAT = "utf-8"
RESULT_PATH = "predicted_result"


def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")
    #conn.send("OK@Welcome to the dashboard Server.".encode(FORMAT))
    name = conn.recv(SIZE).decode(FORMAT)
    #print(f"[RECEIVING] {name}")

    filepath = os.path.join(RESULT_PATH, name)
    with open(filepath, "w") as f:
        f.write(name)
    f.close()

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
