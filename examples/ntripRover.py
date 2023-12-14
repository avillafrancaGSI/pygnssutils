# pylint: disable=invalid-name

from queue import Queue
from threading import Event
from time import sleep

from pygnssutils import VERBOSITY_LOW, GNSSNTRIPClient
from gnssapp import GNSSSkeletonApp

CONNECTED = 1

def configure_ntrip_client():
    # Extracted NTRIP caster parameters into a function for clarity and reusability.
    IPPROT = "IPv4"  # or "IPv6"
    NTRIP_SERVER = "rtk2go.com"
    NTRIP_PORT = 2101
    FLOWINFO = 0  # for IPv6
    SCOPEID = 0  # for IPv6
    MOUNTPOINT = "Tw5384"  # leave blank to retrieve sourcetable
    NTRIP_USER = "avillafranca@gatekeeper-systems.com"
    NTRIP_PASSWORD = "gsi123"

    return {
        "ipprot": IPPROT,
        "server": NTRIP_SERVER,
        "port": NTRIP_PORT,
        "flowinfo": FLOWINFO,
        "scopeid": SCOPEID,
        "mountpoint": MOUNTPOINT,
        "ntripuser": NTRIP_USER,
        "ntrippassword": NTRIP_PASSWORD,
    }

def start_gnss_app(serial_port, baudrate, timeout, stop_event, send_queue):
    # Extracted GNSS app initialization into a function for clarity.
    print(f"Starting GNSS reader/writer on {serial_port} @ {baudrate}...\n")
    with GNSSSkeletonApp(
        serial_port,
        baudrate,
        timeout,
        stopevent=stop_event,
        sendqueue=send_queue,
        idonly=True,
        enableubx=True,
        showhacc=True,
    ) as gna:
        gna.run()
        sleep(2)  # wait for receiver to output at least 1 navigation solution
        return gna

def main():
    try:
        # Extracted GNSS receiver setup into a function and added comments.
        SERIAL_PORT = "COM4"
        BAUDRATE = 230400
        TIMEOUT = 10

        send_queue = Queue()
        stop_event = Event()

        gna = start_gnss_app(SERIAL_PORT, BAUDRATE, TIMEOUT, stop_event, send_queue)

        print("Connecting to NTRIP caster...\n")
        ntrip_params = configure_ntrip_client()
        with GNSSNTRIPClient(gna, verbosity=VERBOSITY_LOW) as gnc:
            streaming = gnc.run(output=send_queue, **ntrip_params)

            if ntrip_params["mountpoint"] == "Tw5384":
                print("Successfully connected to NTRIP caster!")
            else:
                print("Failed to connect to NTRIP caster. Check your configuration.")

            while streaming and not stop_event.is_set():
                sleep(1)

    except KeyboardInterrupt:
        stop_event.set()
        print("Terminated by user")

if __name__ == "__main__":
    main()
