

import socket

from pygnssutils import FORMAT_BINARY, GNSSStreamer

# amend as required...
INPORT = "/dev/tty.usbmodem141101"
HOSTIP = "0.0.0.0"
OUTPORT = 50010

try:
    print(f"Opening TCP socket server {HOSTIP}:{OUTPORT}, waiting for client...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOSTIP, OUTPORT))
        sock.listen(1)
        conn, addr = sock.accept()
        with conn:
            print(f"Client {addr} has connected")
            print(f"Creating GNSSStreamer with serial port {INPORT}...")
            with GNSSStreamer(
                port=INPORT, format=FORMAT_BINARY, outputhandler=conn
            ) as gns:
                gns.run()
except (
    ConnectionRefusedError,
    ConnectionAbortedError,
    ConnectionResetError,
    BrokenPipeError,
    TimeoutError,
):
    print(f"Client {addr} has disconnected")
except KeyboardInterrupt:
    pass
print("Streaming ended")
