#!/usr/bin/env python3.10
"""Populate Lakebase Postgres with insurance demo data matching the UC tables."""
import subprocess, json, psycopg, socket

def get_connection():
    result = subprocess.run(
        ["databricks", "postgres", "generate-database-credential",
         "projects/tko-2026-demo/branches/production/endpoints/primary",
         "-o", "json"], capture_output=True, text=True)
    token = json.loads(result.stdout)["token"]
    result2 = subprocess.run(
        ["databricks", "current-user", "me", "-o", "json"], capture_output=True, text=True)
    username = json.loads(result2.stdout)["userName"]
    host = "ep-old-rice-ean8eftx.database.northeurope.azuredatabricks.net"
    try:
        ip = socket.gethostbyname(host)
    except:
        r = subprocess.run(["dig", "+short", host], capture_output=True, text=True)
        ip = r.stdout.strip().split('\n')[-1]
    return psycopg.connect(host=host, hostaddr=ip, dbname="tko_2026_demo",
                           user=username, password=token, sslmode="require")

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS lakebase_demo")
        cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.policy_types (
            policy_type_id INT PRIMARY KEY, type_name TEXT NOT NULL, description TEXT,
            has_expiry BOOLEAN, billing_frequency TEXT)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.agents (
            agent_id INT PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL,
            email TEXT, phone TEXT, license_number TEXT, hire_date DATE, region TEXT, is_active BOOLEAN)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.customers (
            customer_id INT PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL,
            date_of_birth DATE, email TEXT, phone TEXT, address TEXT, city TEXT, state TEXT,
            zip_code TEXT, created_at TIMESTAMP)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.policies (
            policy_id INT PRIMARY KEY,
            customer_id INT NOT NULL REFERENCES lakebase_demo.customers(customer_id),
            agent_id INT NOT NULL REFERENCES lakebase_demo.agents(agent_id),
            policy_type_id INT NOT NULL REFERENCES lakebase_demo.policy_types(policy_type_id),
            policy_number TEXT NOT NULL, start_date DATE NOT NULL, end_date DATE,
            premium_amount NUMERIC(12,2), status TEXT, created_at TIMESTAMP)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.vehicles (
            vehicle_id INT PRIMARY KEY, policy_id INT NOT NULL, vin TEXT, make TEXT,
            model TEXT, year INT, color TEXT, license_plate TEXT)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.beneficiaries (
            beneficiary_id INT PRIMARY KEY, policy_id INT NOT NULL, first_name TEXT NOT NULL,
            last_name TEXT NOT NULL, relationship TEXT, percentage NUMERIC(5,2), date_of_birth DATE)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.coverages (
            coverage_id INT PRIMARY KEY, policy_id INT NOT NULL, coverage_type TEXT NOT NULL,
            coverage_limit NUMERIC(12,2), deductible NUMERIC(10,2), effective_date DATE)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.premiums (
            premium_id INT PRIMARY KEY, policy_id INT NOT NULL, amount NUMERIC(12,2) NOT NULL,
            due_date DATE, paid_date DATE, payment_method TEXT, status TEXT)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.claims (
            claim_id INT PRIMARY KEY, policy_id INT NOT NULL, claim_number TEXT NOT NULL,
            claim_type TEXT, incident_date DATE, report_date DATE, claim_amount NUMERIC(12,2),
            status TEXT, description TEXT)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.claim_payments (
            payment_id INT PRIMARY KEY, claim_id INT NOT NULL, amount NUMERIC(12,2) NOT NULL,
            payment_date DATE, payment_type TEXT, status TEXT, notes TEXT)""")
        conn.commit()
        print("All tables created.")

def populate_batch(conn, table, columns, generator, total, batch_size=5000):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM lakebase_demo.{table}")
        if cur.fetchone()[0] > 0:
            print(f"  {table} already populated, skipping")
            return
    placeholders = ",".join(["%s"] * columns)
    insert_sql = f"INSERT INTO lakebase_demo.{table} VALUES ({placeholders})"
    batch, inserted = [], 0
    for row in generator:
        batch.append(row)
        if len(batch) >= batch_size:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, batch)
            conn.commit()
            inserted += len(batch)
            print(f"  {table}: {inserted}/{total}")
            batch = []
    if batch:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, batch)
        conn.commit()
        inserted += len(batch)
    print(f"  {table}: {inserted}/{total} DONE")

def gen_agents():
    fn = ['James','Mary','Robert','Patricia','John','Jennifer','Michael','Linda',
          'David','Elizabeth','William','Barbara','Richard','Susan','Joseph','Jessica',
          'Thomas','Sarah','Charles','Karen']
    ln = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
          'Rodriguez','Martinez','Anderson','Taylor','Thomas','Moore','Jackson','Martin',
          'Lee','Thompson','White','Harris','Clark','Lewis','Robinson','Walker','Young']
    regions = ['Northeast','Southeast','Midwest','Southwest','West','Northwest','Central','South']
    from datetime import date, timedelta
    for i in range(1, 501):
        yield (i, fn[i%20], ln[(i*7+3)%25], f'agent{i}@insurance.com',
               f'555-{str((i*37)%10000).zfill(4)}', f'LIC-{str(i).zfill(6)}',
               date(2010,1,1)+timedelta(days=(i*13)%4000), regions[i%8], i%10!=0)

def gen_customers():
    fn = ['James','Mary','Robert','Patricia','John','Jennifer','Michael','Linda',
          'David','Elizabeth','William','Barbara','Richard','Susan','Joseph','Jessica',
          'Thomas','Sarah','Charles','Karen','Daniel','Nancy','Matthew','Lisa',
          'Anthony','Betty','Mark','Margaret','Donald','Sandra']
    ln = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
          'Rodriguez','Martinez','Anderson','Taylor','Thomas','Moore','Jackson','Martin',
          'Lee','Thompson','White','Harris','Clark','Lewis','Robinson','Walker',
          'Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill',
          'Flores','Green','Adams','Nelson','Baker','Hall','Rivera','Campbell']
    streets = ['Main St','Oak Ave','Elm St','Pine Rd','Maple Dr','Cedar Ln','Park Blvd','Lake Ave','Hill St','River Rd']
    cities = ['New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego',
              'Dallas','Austin','Denver','Seattle','Boston','Nashville','Portland']
    states = ['NY','CA','IL','TX','AZ','PA','TX','CA','TX','TX','CO','WA','MA','TN','OR']
    from datetime import date, timedelta, datetime
    for i in range(1, 50001):
        yield (i, fn[i%30], ln[(i*7+3)%40], date(1950,1,1)+timedelta(days=(i*31)%20000),
               f'customer{i}@email.com', f'555-{str((i*41)%10000).zfill(4)}',
               f'{(i*17)%9999+1} {streets[i%10]}', cities[i%15], states[i%15],
               str((i*53)%90000+10000).zfill(5), datetime(2015,1,1)+timedelta(days=i%3650))

def gen_policies():
    from datetime import date, timedelta, datetime
    st = ['ACTIVE','ACTIVE','ACTIVE','EXPIRED','CANCELLED']
    for i in range(1, 100001):
        yield (i, (i%50000)+1, (i%500)+1, (i%10)+1, f'POL-{str(i).zfill(8)}',
               date(2018,1,1)+timedelta(days=i%2500), date(2018,1,1)+timedelta(days=(i%2500)+365),
               round(200+(i*37)%4800,2), st[i%5], datetime(2018,1,1)+timedelta(days=i%2500))

def gen_vehicles():
    makes = ['Toyota','Honda','Ford','Chevrolet','BMW','Mercedes','Nissan','Hyundai','Kia','Tesla']
    models = ['Camry','Civic','F-150','Malibu','3 Series','C-Class','Altima','Elantra','Forte','Model 3']
    colors = ['White','Black','Silver','Blue','Red','Gray','Green','Brown']
    for i in range(1, 60001):
        yield (i, i, f'1HG{i:010X}', makes[i%10], models[i%10], 2015+(i%10), colors[i%8],
               f'{chr(65+i%26)}{chr(65+(i*3)%26)}{chr(65+(i*7)%26)}-{str(i%10000).zfill(4)}')

def gen_beneficiaries():
    fn = ['Emma','Liam','Olivia','Noah','Ava','Ethan','Sophia','Mason',
          'Isabella','Logan','Mia','Lucas','Charlotte','Aiden','Amelia']
    ln = ['Smith','Johnson','Brown','Davis','Wilson','Moore','Taylor','Anderson',
          'Thomas','Jackson','White','Harris','Martin','Garcia','Martinez','Clark',
          'Lewis','Lee','Walker','Hall']
    rels = ['Spouse','Child','Parent','Sibling','Other']
    from datetime import date, timedelta
    for i in range(1, 80001):
        yield (i, ((i-1)%100000)+1, fn[i%15], ln[(i*11)%20], rels[i%5],
               round(100.0/(1+(i%3)),2), date(1960,1,1)+timedelta(days=(i*29)%18000))

def gen_coverages():
    types = ['Liability','Collision','Comprehensive','Medical',
             'Uninsured Motorist','Property Damage','Personal Injury','Rental']
    from datetime import date, timedelta
    for i in range(1, 100001):
        yield (i, ((i-1)%100000)+1, types[i%8], round(10000+(i*43)%490000,2),
               round(250+(i*17)%4750,2), date(2018,1,1)+timedelta(days=i%2500))

def gen_premiums():
    methods = ['CREDIT_CARD','BANK_TRANSFER','CHECK','AUTO_PAY']
    from datetime import date, timedelta
    for i in range(1, 100001):
        due = date(2018,1,1)+timedelta(days=(i*3)%2500)
        paid = None if i%8==0 else due+timedelta(days=i%15)
        status = 'OVERDUE' if i%8==0 else ('PENDING' if i%12==0 else 'PAID')
        yield (i, ((i-1)%100000)+1, round(50+(i*23)%950,2), due, paid, methods[i%4], status)

def gen_claims():
    types = ['Accident','Theft','Fire','Water Damage','Natural Disaster','Vandalism']
    st = ['OPEN','UNDER_REVIEW','APPROVED','DENIED','CLOSED']
    descs = ['Vehicle collision at intersection','Property stolen from premises',
             'Fire damage to property','Water damage from pipe burst',
             'Storm damage to roof','Vandalism to vehicle']
    from datetime import date, timedelta
    for i in range(1, 80001):
        yield (i, ((i-1)%100000)+1, f'CLM-{str(i).zfill(8)}', types[i%6],
               date(2019,1,1)+timedelta(days=i%2200), date(2019,1,1)+timedelta(days=(i%2200)+(i%30)),
               round(500+(i*67)%49500,2), st[i%5], descs[i%6])

def gen_claim_payments():
    types = ['SETTLEMENT','PARTIAL','FINAL']
    st = ['PROCESSED','PENDING','COMPLETED','VOID']
    notes = ['Initial payment','Supplemental payment','Final settlement','Adjusted amount','Expedited processing']
    from datetime import date, timedelta
    for i in range(1, 100001):
        yield (i, ((i-1)%80000)+1, round(100+(i*31)%9900,2),
               date(2019,3,1)+timedelta(days=i%2100), types[i%3], st[i%4], notes[i%5])

if __name__ == "__main__":
    print("Connecting to Lakebase (tko_2026_demo)...")
    conn = get_connection()
    print("Creating tables...")
    create_tables(conn)
    print("Populating data...")
    populate_batch(conn, "policy_types", 5, [
        (1,'Auto','Automobile insurance coverage',True,'MONTHLY'),
        (2,'Home','Homeowners insurance coverage',True,'MONTHLY'),
        (3,'Life','Life insurance coverage',False,'MONTHLY'),
        (4,'Health','Health insurance coverage',True,'MONTHLY'),
        (5,'Renters','Renters insurance coverage',True,'MONTHLY'),
        (6,'Umbrella','Umbrella liability coverage',True,'ANNUALLY'),
        (7,'Commercial','Commercial property insurance',True,'QUARTERLY'),
        (8,'Travel','Travel insurance coverage',True,'ONE_TIME'),
        (9,'Pet','Pet insurance coverage',True,'MONTHLY'),
        (10,'Disability','Disability insurance coverage',True,'MONTHLY'),
    ], 10, batch_size=10)
    populate_batch(conn, "agents", 9, gen_agents(), 500)
    populate_batch(conn, "customers", 11, gen_customers(), 50000)
    populate_batch(conn, "policies", 10, gen_policies(), 100000)
    populate_batch(conn, "vehicles", 8, gen_vehicles(), 60000)
    populate_batch(conn, "beneficiaries", 7, gen_beneficiaries(), 80000)
    populate_batch(conn, "coverages", 6, gen_coverages(), 100000)
    populate_batch(conn, "premiums", 7, gen_premiums(), 100000)
    populate_batch(conn, "claims", 9, gen_claims(), 80000)
    populate_batch(conn, "claim_payments", 7, gen_claim_payments(), 100000)

    print("\nVerification:")
    with conn.cursor() as cur:
        for t in ['policy_types','agents','customers','policies','vehicles',
                   'beneficiaries','coverages','premiums','claims','claim_payments']:
            cur.execute(f"SELECT COUNT(*) FROM lakebase_demo.{t}")
            print(f"  {t}: {cur.fetchone()[0]}")
    conn.close()
    print("Done!")
