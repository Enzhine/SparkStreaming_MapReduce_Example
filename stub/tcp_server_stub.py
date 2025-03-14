import feedparser
import socket
import time
import requests

encoding = 'utf-8'
rss_link = 'https://www.reddit.com/r/AskReddit/new/.rss'
ip = 'localhost'
port = 9999
ttl = None
interval_sec = 2.5

last_stamp: time.struct_time | None = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((ip, port))
s.listen(1)

print(f'Awaiting connection at {ip}:{port}...')
conn, addr = s.accept()
print('Connected by ', addr)

try:
    with open('cached.txt', mode='r') as f:
        lines = f.readlines()

    while True:
        for title in lines:
            print(f'Sending: {title}')
            out = title + '\n'
            conn.send(out.encode(encoding))
            time.sleep(interval_sec)
except KeyboardInterrupt:
    print("Shutting down...")
except Exception as ex:
    print(f'Exception happened {ex}')
finally:
    s.close()
    print(f'Closed connection')

