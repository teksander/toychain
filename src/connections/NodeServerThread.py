import pickle
import threading
import socket
import urllib.parse
from time import sleep
# for sending/receiving with length prefix
import struct 

from toychain.src.connections.MessageHandler import MessageHandler

# Use Length-Prefixed Protocol to avoid truncation issues
LPPROTO=True

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
        print("---------------------------------")

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
        if LPPROTO:
          #length-prefixed protocol to get around truncation
          data = self.receive_with_length(sock) 
        else:
          # has problems with truncation
          data = self.receive(sock) 
          # data = sock.recv(4096)
          
        request = pickle.loads(data)
        # Send the answer
        answer = self.message_handler.handle_request(request)
        if LPPROTO:
          #length-prefixed protocol to get around truncation
          self.send_with_length(pickle.dumps(answer), sock)
        else:
          # has problems with truncation
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
          if LPPROTO:
            #length-prefixed protocol to get around truncation
            self.send_with_length(pickle.dumps(request), sock) 
          else:
            # has problems with truncation
            self.send(pickle.dumps(request), sock) 

        except Exception as e:
          print(f"Error Connecting to Address : {address}")
          raise e
          
        # Get the answer
        try:
          if LPPROTO:
            #length-prefixed protocol to get around truncation
            data = self.receive_with_length(sock)
          else:
            # has problems with truncation
            data = self.receive(sock)
        except Exception as e:
            print(f"Error receiving data:\n{e}")
            #raise e # Commented out to allow handling of incomplete data
        try:
          # Unpickle the answer
          answer = pickle.loads(data)
        except EOFError as e:
          print(data)
          raise e
        # Handle the answer
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


    # Used if LPPROTO is True
    # Utility functions for sending/receiving with length prefix
    # to avoid truncation issues
    def send_with_length(self, data, sock):
        length = struct.pack('!I', len(data))  # 4 bytes, network byte order
        sock.sendall(length)
        sock.sendall(data)
    
    
    def receive_with_length(self, sock):
      sock.settimeout(50)  # Set a timeout for the socket to prevent getting stuck indefinitely
      try:
        # First, receive 4 bytes for the length
        length_data = b''
        while len(length_data) < 4:
            more = sock.recv(4 - len(length_data))
            if not more:
                raise ConnectionError("Socket closed before length received")
            length_data += more
        total_length = struct.unpack('!I', length_data)[0]
    
        # Now receive the actual data
        data = b''
        while len(data) < total_length:
            more = sock.recv(min(4096, total_length - len(data)))
            if not more:
                raise ConnectionError("Socket closed before all data received")
            data += more
      except socket.timeout:
          print("Socket timed out. No more data to receive.")
      except socket.error as e:
          print(f"Socket error occurred: {e}")
      finally:
          sock.settimeout(None)  # Reset the timeout to the default (blocking mode)
      return data