import feedparser
import socket
import time
import requests

encoding = 'utf-8'
rss_link = 'https://www.reddit.com/r/AskReddit/new/.rss'
ip = 'localhost'
port = 9999
ttl = None
interval_sec = 30.0

last_stamp: time.struct_time | None = None

f = open('cached.txt', mode='w')

try:
    while True:
        print(f'iteration at {time.time()}')
        if (ttl is not None) and last_stamp > ttl:
            print('Time to live passed. Exiting.')
            break

        feed = feedparser.parse(requests.get(rss_link).content)
        # if feed.status and (feed.status != 200):
        #     print(f'Bad response {feed.status} received! Exiting.')
        #     break

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
            f.write(title)
            f.flush()
        if _mx_stmp is not None:
            last_stamp = _mx_stmp

        time.sleep(interval_sec)
except KeyboardInterrupt:
    print("Shutting down...")
except Exception as ex:
    print(f'Exception happened {ex}')
finally:
    f.close()
    print(f'Closed connection')
