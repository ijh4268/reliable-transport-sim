# do not import anything else from loss_socket besides LossyUDP
from concurrent.futures import ThreadPoolExecutor
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
from time import sleep
from threading import Timer
import struct
import heapq

TIMEOUT = 0.25
HEADER_SIZE = 6

class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.closed = False
        self.seq = 0
        self.expected_seq = 0
        self.recv_buf = []
        self.ack = False
        self.fin = False
        self.finack = False
        
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)

    def _chunk(self, data_bytes: bytes, size: int):
        for i in range(0, len(data_bytes), size):
            yield data_bytes[i:i+size]

    def _fetch_data(self):
        data, addr = self.socket.recvfrom()
        # ! -5 from len(data) to account for header size
        if data:
            unpacked_data = struct.unpack(f'!i{len(data) - HEADER_SIZE}s??', data)
            self.ack = unpacked_data[2]
            self.fin = unpacked_data[3]
            if self.fin and self.ack:
                print("FIN ACK received!")
                self.finack = True
            elif self.fin:
                print("FIN packet received.")
            if not self.ack and unpacked_data not in self.recv_buf: 
                heapq.heappush(self.recv_buf, unpacked_data)

    def _send_ack(self):
        ack = struct.pack(f'!i1s??', self.seq, b'', True, self.fin)
        self.socket.sendto(ack, (self.dst_ip, self.dst_port))

    def resend(self, packet):
        print(f"resending packet {packet}...")
        self.socket.sendto(packet, (self.dst_ip, self.dst_port))
        self.started = False

    def listener(self):
        while not self.closed:
            try:
                self._fetch_data()
                if not self.ack: self._send_ack()
            except Exception as e:
                print("ERROR: listener died!")
                print(e)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!
        packet_size = 1472 # ! packet size is 1472 bytes
        # * if the data_bytes is larger than the size of a packet, then we need to chunk it. 
        # ! -5 from packet size to account for header (4 byte seq int, 1 byte bool)
        data_chunks = list(self._chunk(data_bytes, packet_size-HEADER_SIZE))
        # for now I'm just sending the raw application-level data in one UDP payload
        for chunk in data_chunks:
            packet = struct.pack(f'!i{len(chunk)}s??', self.seq, chunk, False, self.fin)
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            self.started = False
            while not self.ack:
                if not self.started: 
                    self.timer = Timer(TIMEOUT, self.resend, [packet])
                    self.timer.start()
                    self.started = True
                sleep(0.01)
            self.timer.cancel()
            self.ack = False
            self.seq += 1


    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        # this sample code just calls the recvfrom method on the LossySocket
        while not self.closed:
            if self.recv_buf:
                curr_packet = heapq.heappop(self.recv_buf)
                # check if we have the packet we need
                if curr_packet[0] == self.expected_seq:
                    self.expected_seq += 1 # increment expected sequence number
                    return curr_packet[1] # return the data portion of the packet
                elif curr_packet[0] < self.expected_seq:
                    continue
                else:
                    heapq.heappush(self.recv_buf, curr_packet)
            
    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        fin_packet = struct.pack('!i1s??', -1, b'', False, True)
        self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
        self.started = False
        while not self.finack:
            if not self.started:
                self.timer = Timer(TIMEOUT, self.resend, [fin_packet])
                self.timer.start()
                self.started = True
        sleep(2)
        print('closing connection...')
        self.closed = True
        self.socket.stoprecv()
