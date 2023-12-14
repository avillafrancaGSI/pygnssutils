from queue import Queue, Empty
from threading import Event
from time import sleep

from pygnssutils import VERBOSITY_LOW, GNSSNTRIPClient
from gnssapp import GNSSSkeletonApp

CONNECTED = 1

if __name__ == "__main__":
    # GNSS receiver serial port parameters - AMEND AS REQUIRED:
    SERIAL_PORT = "COM3"
    BAUDRATE = 230400
    TIMEOUT = 10

    # NTRIP caster parameters - AMEND AS REQUIRED:
    # Ideally, mountpoint should be <30 km from location.
    IPPROT = "IPv4"  # or "IPv6"
    NTRIP_SERVER = "rtk2go.com"
    NTRIP_PORT = 2101
    FLOWINFO = 0  # for IPv6
    SCOPEID = 0  # for IPv6
    MOUNTPOINT = "Tw5384"  # leave blank to retrieve sourcetable
    NTRIP_USER = "avillafranca@gatekeeper-systems.com"
    NTRIP_PASSWORD = "gsi123"

    # NMEA GGA sentence status - AMEND AS REQUIRED:
    GGAMODE = 0  # use fixed reference position (0 = use live position)
    GGAINT = 60  # interval in seconds (-1 = do not send NMEA GGA sentences)
    # Fixed reference coordinates (only used when GGAMODE = 1) - AMEND AS REQUIRED:
    REFLAT = 51.176534
    REFLON = -2.15453
    REFALT = 40.8542
    REFSEP = 26.1743

    send_queue = Queue()
    sourcetable_queue = Queue()
    stop_event = Event()

    try:
        print(f"Starting GNSS reader/writer on {SERIAL_PORT} @ {BAUDRATE}...\n")
        with GNSSSkeletonApp(
            SERIAL_PORT,
            BAUDRATE,
            TIMEOUT,
            stopevent=stop_event,
            sendqueue=send_queue,
            idonly=True,
            enableubx=True,
            showhacc=True,
        ) as gna:
            gna.run()
            sleep(2)  # wait for the receiver to output at least 1 navigation solution

            mountpoint = ""
            print(
                f"Retrieving closest mountpoint from {NTRIP_SERVER}:{NTRIP_PORT}...\n"
            )
            with GNSSNTRIPClient(gna, verbosity=VERBOSITY_LOW) as gnc:
                streaming = gnc.run(
                    server=NTRIP_SERVER,
                    port=NTRIP_PORT,
                    mountpoint=mountpoint,
                    ntripuser=NTRIP_USER,
                    ntrippassword=NTRIP_PASSWORD,
                    output=sourcetable_queue,
                )

                try:
                    srt, (mountpoint, dist) = sourcetable_queue.get(timeout=3)
                    if dist>0.1 and mountpoint is None:
                        raise Empty
                    print(
                        f"\nClosest mountpoint is {mountpoint} which is {dist} km away\n"
                    )
                except Empty:
                    stop_event.set()
                    print("Unable to find closest mountpoint - quitting...\n")

            print(
                f"Streaming RTCM3 data from {NTRIP_SERVER}:{NTRIP_PORT}/{mountpoint}...\n"
            )
            with GNSSNTRIPClient(gna, verbosity=VERBOSITY_LOW) as gnc:
                streaming = gnc.run(
                    server=NTRIP_SERVER,
                    port=NTRIP_PORT,
                    mountpoint=mountpoint,
                    ntripuser=NTRIP_USER,
                    ntrippassword=NTRIP_PASSWORD,
                    output=send_queue,
                )

                while streaming and not stop_event.is_set():
                    try:
                        message = send_queue.get(timeout=1)
                        if message.startswith(b'\xD3\x00'):
                            rtcm_type = message[3]  # Modify this index based on the message structure
                            print(f"Received RTCM Message Type: {rtcm_type}")
                        else:
                            # If it's not an RTCM message, you can handle it accordingly
                            pass
                    except Empty:
                        pass
                    sleep(1)

    except KeyboardInterrupt:
        stop_event.set()
        print("Terminated by user")
