import feedparser
import socket
import time

encoding = 'utf-8'
rss_link = 'https://www.reddit.com/r/AskReddit/new/.rss'
ip = 'localhost'
port = 9999
ttl = None
interval_sec = 30.0

last_stamp: time.struct_time | None = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((ip, port))
s.listen(1)

print(f'Awaiting connection at {ip}:{port}...')
conn, addr = s.accept()
print('Connected by ', addr)

try:
    while True:
        print(f'iteration at {time.time()}')
        if (ttl is not None) and last_stamp > ttl:
            print('Time to live passed. Exiting.')
            break

        feed = feedparser.parse(rss_link)
        if feed.status != 200:
            print(f'Bad response {feed.status} received! Exiting.')
            break

        _mx_stmp = None
        for i, entry in enumerate(feed.entries):
            title = entry.title
            time_stamp = entry.published_parsed

            if (last_stamp is None) or (last_stamp < time_stamp):
                if _mx_stmp is None: _mx_stmp = time_stamp
            else:
                break

            print(f'Retrieved: {title}')
            title += '\n'
            conn.send(title.encode(encoding))
        if _mx_stmp is not None:
            last_stamp = _mx_stmp

        time.sleep(interval_sec)
except KeyboardInterrupt:
    print("Shutting down...")
except Exception as ex:
    print(f'Exception happened {ex}')
finally:
    s.close()
    print(f'Closed connection')