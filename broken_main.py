import asyncio, json, datetime, requests
import psycopg2

def get_orders_by_email(email):
  conn = psycopg2.connect("dbname=shop user=admin password=admin")
  cur = conn.cursor()
  cur.execute(f"SELECT created_at, amount FROM orders WHERE email = '{email}'")
  orders = cur.fetchall()
  conn.close()
  return orders

def get_eur():
    r = requests.get("https://api.exchangerate.host/latest?base=USD")
    return r.json()["rates"]["EUR"]

async def calc_discount(user, eur_rate):
    orders = get_orders_by_email(user['email'])
    score = 0
    for i in range(len(orders)):
        for j in range(i + 1, len(orders)):
            delta = abs((orders[i][0] - orders[j][0]).days)
            if delta < 30:
                score += 10
    user['discount'] = min(50, score)
    user['discount_eur'] = user['discount'] * eur_rate
    user['ts'] = datetime.datetime.utcnow()

def save(user):
    conn = psycopg2.connect("dbname=shop user=admin password=admin")
    cur = conn.cursor()
    q = f"UPDATE clients SET discount={user['discount_eur']} WHERE email='{user['email']}'"
    cur.execute(q)
    conn.commit()
    conn.close()

counter = 0
def inc(): global counter; counter+=1

def events(event, channel=[]):
    channel.append(json.dumps(event))
    return channel

async def main():
    users = [
        {"email": "john@example.com"},
        {"email": "admin@example.com"}
    ]

    for u in users:
        await calc_discount(u, get_eur())
        if u['email'].startswith("john"): inc()
        save(u)
        events(u)

asyncio.get_event_loop().run_until_complete(main())