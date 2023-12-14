
import os

from pygnssutils import FORMAT_JSON, GNSSStreamer

# amend as required...
JSONFILE = os.path.join(os.path.expanduser("~"), "jsonfile.json")
INPORT = "/dev/tty.usbmodem141101"
LIMIT = 50  # 0 = unlimited, CRTL-C to terminate

print(f"Opening text file {JSONFILE} for write...")
with open(JSONFILE, "w", encoding="UTF-8") as jfile:
    print(f"Creating GNSSStreamer with serial port {INPORT}...")
    with GNSSStreamer(
        port=INPORT, format=FORMAT_JSON, limit=LIMIT, outputhandler=jfile
    ) as gns:
        print("Streaming GNSS data into JSON file...")
        gns.run()
print("Streaming ended")
