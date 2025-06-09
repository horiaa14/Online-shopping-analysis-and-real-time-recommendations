import redis
import time

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

attempt = 1

while True:
    keys = r.keys('*')
    count = len(keys)
    
    print(f"[{attempt}] {count} keys found.")

    if count == 0:
        print("Redis is now empty.")
        break

    for key in keys:
        r.delete(key)

    print(f"[{attempt}] Deleted {count} keys.")
    time.sleep(0.2)
    attempt += 1
