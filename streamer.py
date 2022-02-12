# do not import anything else from loss_socket besides LossyUDP
from concurrent.futures import ThreadPoolExecutor
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
from time import sleep
from threading import Timer, Lock
import struct
import heapq
import hashlib

TIMEOUT = 0.25

class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.lock = Lock()
        self.closed = False
        self.seq = 0
        self.ackseq = 0
        self.expected_seq = 0
        self.recv_buf = []
        self.window = {}
        self.ack = False
        self.fin = False
        self.finack = False
        self.started = False
        self.hash = hashlib.md5()
        self.header_size = 6
        
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)

    def _chunk(self, data_bytes: bytes, size: int):
        for i in range(0, len(data_bytes), size):
            yield data_bytes[i:i+size]

    def _parse_data(self, data):
        format = f'!{len(data)-self.hash.digest_size}s{self.hash.digest_size}s'
        packet_hash = struct.unpack(format, data)
        format = f'!i{len(packet_hash[0])-self.header_size}s??'
        unpacked_data = struct.unpack(format, packet_hash[0])
        self.hash = hashlib.md5()
        self.hash.update(packet_hash[0])
        hash = self.hash.digest()
        
        return unpacked_data, hash, packet_hash

    def _fetch_data(self):
        data, addr = self.socket.recvfrom()
        # ! -5 from len(data) to account for header size
        if data:
            unpacked_data, hash, packet_hash = self._parse_data(data)
            if hash == packet_hash[1]:
                self.ack = unpacked_data[2]
                self.fin = unpacked_data[3]
                if not self.ack and unpacked_data not in self.recv_buf:
                    with self.lock:
                        heapq.heappush(self.recv_buf, unpacked_data)
                elif not self.ack and self.fin:
                    self._send_ack(unpacked_data[0])
                elif self.fin and self.ack:
                    self.timer.cancel()
                    self.finack = True
                elif self.ack and unpacked_data[0] in self.window.keys(): 
                    with self.lock:
                        self.started = False
                        self.ack = False
                        self.window.pop(unpacked_data[0])
                        self.ackseq += 1
                        print(f'received ACK {self.ackseq}!')
                    if len(self.window) > 10 or not self.window: self.timer.cancel()
            else: return True

    def _send_ack(self, seq):
        ack = struct.pack(f'!i1s??', seq, b'', True, self.fin)
        self.hash = hashlib.md5()
        self.hash.update(ack)
        hash = self.hash.digest()
        ack_packet = struct.pack(f'!{len(ack)}s{self.hash.digest_size}s', ack, hash)
        self.socket.sendto(ack_packet, (self.dst_ip, self.dst_port))
        print(f'sent ACK {seq}...')

    def resend(self):
        with self.lock:
            curr_window = self.window.copy()
            self.started = False
            for seq in curr_window:
                print(f"resending packet {seq}...")
                self.socket.sendto(self.window[seq], (self.dst_ip, self.dst_port))

    def listener(self):
        while not self.closed:
            try:
                self._fetch_data()
            except Exception as e:
                print("ERROR: listener died!")
                print(e)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!
        # while(self.seq - self.ackseq > 10): sleep(0.01)
        packet_size = 1472 # ! packet size is 1472 bytes
        # * if the data_bytes is larger than the size of a packet, then we need to chunk it.
        overhead = self.header_size+self.hash.digest_size 
        data_chunks = list(self._chunk(data_bytes, packet_size-overhead))
        # for now I'm just sending the raw application-level data in one UDP payload
        for chunk in data_chunks:
            format = f'!i{len(chunk)}s??'
            packet = struct.pack(format, self.seq, chunk, False, False)
            self.hash = hashlib.md5()
            self.hash.update(packet)
            hash = self.hash.digest()
            packet_hash = struct.pack(f'!{len(packet)}s{self.hash.digest_size}s', packet, hash)
            # ! ------------------ Critical section ------------------
            with self.lock:
                self.window[self.seq] = packet_hash
                self.socket.sendto(packet_hash, (self.dst_ip, self.dst_port))
                if not self.started:
                    self.timer = Timer(TIMEOUT, self.resend)
                    self.timer.start()
                    self.started = True
            # ! ------------------------------------------------------
            self.seq += 1


    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        # this sample code just calls the recvfrom method on the LossySocket
        while not self.closed:
            if self.recv_buf:
                curr_packet = heapq.heappop(self.recv_buf)
                if curr_packet[0] == self.expected_seq:
                    self.expected_seq += 1 # increment expected sequence number
                    self._send_ack(curr_packet[0])
                    return curr_packet[1] # return the data portion of the packet
                elif curr_packet[0] < self.expected_seq:
                    self._send_ack(curr_packet[0])
                else:
                    heapq.heappush(self.recv_buf, curr_packet)
            elif self.window:
                sleep(TIMEOUT)
                self.resend()
            
    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        fin_packet = struct.pack(f'!i1s??', -1, b'', False, True)
        self.hash = hashlib.md5()
        self.hash.update(fin_packet)
        hash = self.hash.digest()
        fin_packet_hash = struct.pack(f'!{len(fin_packet)}s{self.hash.digest_size}s', fin_packet, hash)
        print('sending FIN packet...')
        self.socket.sendto(fin_packet_hash, (self.dst_ip, self.dst_port))
        with self.lock:
            self.window[-1] = fin_packet_hash
        while not self.finack and self.recv_buf:
            curr_packet = heapq.heappop(self.recv_buf)
            self._send_ack(curr_packet[0])
        print('FIN ACK received...')
        sleep(2)
        print('closing connection...')
        self.closed = True
        self.socket.stoprecv()
