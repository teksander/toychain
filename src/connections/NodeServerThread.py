import pickle
import threading

import socket
import time
import urllib.parse
from time import sleep

from toychain.src.connections.MessageHandler import MessageHandler

class NodeServerThreadUDP(threading.Thread):
    """
    Minimalist UDP-based node server thread with automatic message chunking and reassembly
    """

    CHUNK_SIZE = 1024  # Define the chunk size for UDP packets

    def __init__(self, node, host, port, id):
        super().__init__()
        self.id = id
        self.node = node
        self.host = host
        self.port = port
        self.message_handler = MessageHandler(self)
        self.terminate_flag = threading.Event()
        print(f"Node {self.id} starting on port {self.port}")

    def run(self):
        """ Listen for incoming UDP messages """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((self.host, self.port))

        while not self.terminate_flag.is_set():
            try:
                sock.settimeout(5)

                # Receive data and client address
                data, client_address = self.receive_large_message(sock)
                if data:
                    request = pickle.loads(data)
                    answer = self.message_handler.handle_request(request)
                    self.send_large_message(sock, pickle.dumps(answer), client_address)

            except socket.timeout:
                pass
            except Exception as e:
                raise e

            sleep(0.00001)

        sock.close()
        print(f"Node {self.id} stopped")

    def send_request(self, enode, request):
        """ Send a request via UDP and receive the response """
        parsed_enode = urllib.parse.urlparse(enode)
        address = (parsed_enode.hostname, parsed_enode.port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Send the request in chunks
        self.send_large_message(sock, pickle.dumps(request), address)

        # Receive the response in chunks
        data = self.receive_large_message(sock)[0]
        if data:
            answer = pickle.loads(data)
            self.message_handler.handle_answer(answer)

        sock.close()

    def send_large_message(self, sock, data, address):
        """ Send large messages by splitting them into chunks """
        chunks = [data[i:i + self.CHUNK_SIZE] for i in range(0, len(data), self.CHUNK_SIZE)]
        for chunk in chunks:
            sock.sendto(chunk, address)
        # Send an empty chunk to indicate the end of transmission
        sock.sendto(b'', address)

    def receive_large_message(self, sock):
        """ Reassemble large messages received in chunks and return the data and client address """
        data = bytearray()
        client_address = None
        while True:
            chunk, address = sock.recvfrom(self.CHUNK_SIZE)
            if not client_address:
                client_address = address  # Capture the client address from the first packet
            if not chunk:  # Empty chunk signals end of transmission
                break
            data.extend(chunk)
        return data if data else None, client_address

    def stop(self):
        self.terminate_flag.set()

class NodeServerThread(threading.Thread):
    """
    Thread answering to requests, every node has one
    """

    def __init__(self, node, host, port, id):
        super().__init__()

        self.sock = None
        self.id = id
        self.node = node
        self.host = host
        self.port = port
        self.max_packet = 6000000

        self.message_handler = MessageHandler(self)

        self.terminate_flag = threading.Event()

        print("Node " + str(self.id) + " starting on port " + str(self.port))

    def run(self):
        """
        Waiting for one other Node to connect
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))

        while not self.terminate_flag.is_set():
          try:
            self.sock.settimeout(5)
            self.sock.listen(1)
            client_sock, client_address = self.sock.accept()
            self.handle_connection(client_sock)

          except socket.timeout:
            pass

          except Exception as e:
            raise e

          sleep(0.00001)

        self.sock.shutdown(True)
        self.sock.close()
        print("Node " + str(self.id) + " stopped")

    def handle_connection(self, sock):
        """
        Answer with the asked information
        """

        # Receive request
        data = self.receive(sock)
        # data = sock.recv(4096)
        request = pickle.loads(data)

        # Send the answer
        answer = self.message_handler.handle_request(request)
        self.send(pickle.dumps(answer), sock)

    def send_request(self, enode, request):
        """
        Sends a request and returns the answer
        """
        parsed_enode = urllib.parse.urlparse(enode)
        address = (parsed_enode.hostname, parsed_enode.port)
        
        # Send the request
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
          sock.connect(address)
          self.send(pickle.dumps(request), sock)

        except Exception as e:
          print(f"Error Connecting to Address : {address}")
          raise e
          
        # Get the answer
        try:
            data = self.receive(sock)
        except:
            print("Error receiving data")

        try:
          answer = pickle.loads(data)
        except EOFError as e:
          print(data)
          raise e
        self.message_handler.handle_answer(answer)

        sock.close()

    def stop(self):
        self.terminate_flag.set()

    def send(self, data, sock):
        sock.sendall(data)

    def receive(self, sock):
        data = []
        sock.settimeout(50)  # Set a timeout for the socket to prevent getting stuck indefinitely
        try:
            while True:
                packet = sock.recv(4096)
                if not packet:
                    break  # No more data to receive
                if len(packet) > self.max_packet:
                    self.max_packet = len(packet)
                data.append(packet)
                if len(packet) < 4096:
                    break  # End of the message
        except socket.timeout:
            print("Socket timed out. No more data to receive.")
        except socket.error as e:
            print(f"Socket error occurred: {e}")
        finally:
            sock.settimeout(None)  # Reset the timeout to the default (blocking mode)
       
        return b"".join(data)