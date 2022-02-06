# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct

class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

    def chunk(self, data_bytes: bytes, size: int):
        for i in range(0, len(data_bytes), size):
            yield data_bytes[i:i+size]


    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!
        packet_size = 1472 # ! packet size is 1472 bytes
        # * if the data_bytes is larger than the size of a packet, then we need to chunk it. 
        data_chunks = list(self.chunk(data_bytes, packet_size))
        # for now I'm just sending the raw application-level data in one UDP payload
        seq = 0
        for chunk in data_chunks:
            packet = struct.pack(f'!i{len(chunk)}s', seq, chunk)
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        recv_buf = []
        # this sample code just calls the recvfrom method on the LossySocket
        data, addr = self.socket.recvfrom()
        curr_seq = 0
        while data:
            # ? Subtract 4 for sequence number offset
            recv_buf.append(struct.unpack(f'i{len(data) - 4}s', data))
            
            # For now, I'll just pass the full UDP payload to the app
            return data

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        pass
