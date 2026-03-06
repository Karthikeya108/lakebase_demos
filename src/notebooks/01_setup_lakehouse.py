# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Setup Lakehouse (Unity Catalog Delta Tables)
# MAGIC Creates a SQL warehouse, UC catalog/schema, and 10 insurance tables with ~570K total records.
# MAGIC All resources are created under the identity of the executor.

# COMMAND ----------

dbutils.widgets.text("catalog", "tko_2026")
dbutils.widgets.text("schema", "lakebase_demo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Setting up {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Find SQL Warehouse

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me()
print(f"Running as: {me.user_name}")

warehouse_name = f"tko-2026-warehouse"
warehouse_id = None

# Check for existing warehouse
for wh in w.warehouses.list():
    if wh.name == warehouse_name:
        warehouse_id = wh.id
        print(f"Found existing warehouse: {warehouse_name} ({warehouse_id})")
        break

if not warehouse_id:
    print(f"Creating SQL warehouse: {warehouse_name}")
    wh = w.warehouses.create(
        name=warehouse_name,
        cluster_size="Small",
        max_num_clusters=1,
        auto_stop_mins=10,
        enable_photon=True,
        warehouse_type="PRO",
        spot_instance_policy="COST_OPTIMIZED",
    ).result()
    warehouse_id = wh.id
    print(f"Created warehouse: {warehouse_name} ({warehouse_id})")

# Pass warehouse_id to downstream tasks
dbutils.jobs.taskValues.set(key="warehouse_id", value=warehouse_id)
print(f"Warehouse ID: {warehouse_id}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print(f"Catalog and schema ready: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Tables

# COMMAND ----------

tables_sql = {
    "policy_types": """
        CREATE TABLE IF NOT EXISTS policy_types (
            policy_type_id INT,
            type_name STRING,
            description STRING,
            has_expiry BOOLEAN,
            billing_frequency STRING
        ) USING DELTA
    """,
    "agents": """
        CREATE TABLE IF NOT EXISTS agents (
            agent_id INT,
            first_name STRING,
            last_name STRING,
            email STRING,
            phone STRING,
            license_number STRING,
            hire_date DATE,
            region STRING,
            is_active BOOLEAN
        ) USING DELTA
    """,
    "customers": """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT,
            first_name STRING,
            last_name STRING,
            date_of_birth DATE,
            email STRING,
            phone STRING,
            address STRING,
            city STRING,
            state STRING,
            zip_code STRING,
            created_at TIMESTAMP
        ) USING DELTA
    """,
    "policies": """
        CREATE TABLE IF NOT EXISTS policies (
            policy_id INT,
            customer_id INT,
            agent_id INT,
            policy_type_id INT,
            policy_number STRING,
            start_date DATE,
            end_date DATE,
            premium_amount DECIMAL(12,2),
            status STRING,
            created_at TIMESTAMP
        ) USING DELTA
    """,
    "vehicles": """
        CREATE TABLE IF NOT EXISTS vehicles (
            vehicle_id INT,
            policy_id INT,
            vin STRING,
            make STRING,
            model STRING,
            year INT,
            color STRING,
            license_plate STRING
        ) USING DELTA
    """,
    "beneficiaries": """
        CREATE TABLE IF NOT EXISTS beneficiaries (
            beneficiary_id INT,
            policy_id INT,
            first_name STRING,
            last_name STRING,
            relationship STRING,
            percentage DECIMAL(5,2),
            date_of_birth DATE
        ) USING DELTA
    """,
    "coverages": """
        CREATE TABLE IF NOT EXISTS coverages (
            coverage_id INT,
            policy_id INT,
            coverage_type STRING,
            coverage_limit DECIMAL(12,2),
            deductible DECIMAL(10,2),
            effective_date DATE
        ) USING DELTA
    """,
    "premiums": """
        CREATE TABLE IF NOT EXISTS premiums (
            premium_id INT,
            policy_id INT,
            amount DECIMAL(12,2),
            due_date DATE,
            paid_date DATE,
            payment_method STRING,
            status STRING
        ) USING DELTA
    """,
    "claims": """
        CREATE TABLE IF NOT EXISTS claims (
            claim_id INT,
            policy_id INT,
            claim_number STRING,
            claim_type STRING,
            incident_date DATE,
            report_date DATE,
            claim_amount DECIMAL(12,2),
            status STRING,
            description STRING
        ) USING DELTA
    """,
    "claim_payments": """
        CREATE TABLE IF NOT EXISTS claim_payments (
            payment_id INT,
            claim_id INT,
            amount DECIMAL(12,2),
            payment_date DATE,
            payment_type STRING,
            status STRING,
            notes STRING
        ) USING DELTA
    """,
}

for table_name, ddl in tables_sql.items():
    spark.sql(ddl)
    print(f"Created table: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Populate Data

# COMMAND ----------

from pyspark.sql import Row
from datetime import date, datetime, timedelta

# COMMAND ----------

# Check if already populated
row_count = spark.sql("SELECT COUNT(*) AS cnt FROM policy_types").collect()[0]["cnt"]
if row_count > 0:
    print("Tables already populated, skipping data generation.")
    dbutils.notebook.exit("ALREADY_POPULATED")

# COMMAND ----------

# Policy Types (10 rows)
policy_types_data = [
    (1, "Auto", "Automobile insurance coverage", True, "MONTHLY"),
    (2, "Home", "Homeowners insurance coverage", True, "MONTHLY"),
    (3, "Life", "Life insurance coverage", False, "MONTHLY"),
    (4, "Health", "Health insurance coverage", True, "MONTHLY"),
    (5, "Renters", "Renters insurance coverage", True, "MONTHLY"),
    (6, "Umbrella", "Umbrella liability coverage", True, "ANNUALLY"),
    (7, "Commercial", "Commercial property insurance", True, "QUARTERLY"),
    (8, "Travel", "Travel insurance coverage", True, "ONE_TIME"),
    (9, "Pet", "Pet insurance coverage", True, "MONTHLY"),
    (10, "Disability", "Disability insurance coverage", True, "MONTHLY"),
]

df = spark.createDataFrame(policy_types_data, ["policy_type_id", "type_name", "description", "has_expiry", "billing_frequency"])
df.write.mode("append").saveAsTable("policy_types")
print(f"policy_types: {df.count()} rows")

# COMMAND ----------

# Agents (500 rows)
fn = ['James','Mary','Robert','Patricia','John','Jennifer','Michael','Linda',
      'David','Elizabeth','William','Barbara','Richard','Susan','Joseph','Jessica',
      'Thomas','Sarah','Charles','Karen']
ln = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
      'Rodriguez','Martinez','Anderson','Taylor','Thomas','Moore','Jackson','Martin',
      'Lee','Thompson','White','Harris','Clark','Lewis','Robinson','Walker','Young']
regions = ['Northeast','Southeast','Midwest','Southwest','West','Northwest','Central','South']

agents_data = []
for i in range(1, 501):
    agents_data.append(Row(
        agent_id=i, first_name=fn[i%20], last_name=ln[(i*7+3)%25],
        email=f"agent{i}@insurance.com", phone=f"555-{str((i*37)%10000).zfill(4)}",
        license_number=f"LIC-{str(i).zfill(6)}",
        hire_date=date(2010,1,1)+timedelta(days=(i*13)%4000),
        region=regions[i%8], is_active=(i%10!=0)
    ))

spark.createDataFrame(agents_data).write.mode("append").saveAsTable("agents")
print(f"agents: {len(agents_data)} rows")

# COMMAND ----------

# Customers (50,000 rows)
fn2 = ['James','Mary','Robert','Patricia','John','Jennifer','Michael','Linda',
       'David','Elizabeth','William','Barbara','Richard','Susan','Joseph','Jessica',
       'Thomas','Sarah','Charles','Karen','Daniel','Nancy','Matthew','Lisa',
       'Anthony','Betty','Mark','Margaret','Donald','Sandra']
ln2 = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
       'Rodriguez','Martinez','Anderson','Taylor','Thomas','Moore','Jackson','Martin',
       'Lee','Thompson','White','Harris','Clark','Lewis','Robinson','Walker',
       'Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill',
       'Flores','Green','Adams','Nelson','Baker','Hall','Rivera','Campbell']
streets = ['Main St','Oak Ave','Elm St','Pine Rd','Maple Dr','Cedar Ln','Park Blvd','Lake Ave','Hill St','River Rd']
cities = ['New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego',
          'Dallas','Austin','Denver','Seattle','Boston','Nashville','Portland']
states = ['NY','CA','IL','TX','AZ','PA','TX','CA','TX','TX','CO','WA','MA','TN','OR']

customers_data = []
for i in range(1, 50001):
    customers_data.append(Row(
        customer_id=i, first_name=fn2[i%30], last_name=ln2[(i*7+3)%40],
        date_of_birth=date(1950,1,1)+timedelta(days=(i*31)%20000),
        email=f"customer{i}@email.com", phone=f"555-{str((i*41)%10000).zfill(4)}",
        address=f"{(i*17)%9999+1} {streets[i%10]}", city=cities[i%15], state=states[i%15],
        zip_code=str((i*53)%90000+10000).zfill(5),
        created_at=datetime(2015,1,1)+timedelta(days=i%3650)
    ))

spark.createDataFrame(customers_data).write.mode("append").saveAsTable("customers")
print(f"customers: {len(customers_data)} rows")

# COMMAND ----------

# Policies (100,000 rows)
status_vals = ['ACTIVE','ACTIVE','ACTIVE','EXPIRED','CANCELLED']

policies_data = []
for i in range(1, 100001):
    policies_data.append(Row(
        policy_id=i, customer_id=(i%50000)+1, agent_id=(i%500)+1,
        policy_type_id=(i%10)+1, policy_number=f"POL-{str(i).zfill(8)}",
        start_date=date(2018,1,1)+timedelta(days=i%2500),
        end_date=date(2018,1,1)+timedelta(days=(i%2500)+365),
        premium_amount=float(round(200+(i*37)%4800, 2)),
        status=status_vals[i%5],
        created_at=datetime(2018,1,1)+timedelta(days=i%2500)
    ))

spark.createDataFrame(policies_data).write.mode("append").saveAsTable("policies")
print(f"policies: {len(policies_data)} rows")

# COMMAND ----------

# Vehicles (60,000 rows)
makes = ['Toyota','Honda','Ford','Chevrolet','BMW','Mercedes','Nissan','Hyundai','Kia','Tesla']
models = ['Camry','Civic','F-150','Malibu','3 Series','C-Class','Altima','Elantra','Forte','Model 3']
colors = ['White','Black','Silver','Blue','Red','Gray','Green','Brown']

vehicles_data = []
for i in range(1, 60001):
    vehicles_data.append(Row(
        vehicle_id=i, policy_id=i, vin=f"1HG{i:010X}",
        make=makes[i%10], model=models[i%10], year=2015+(i%10),
        color=colors[i%8],
        license_plate=f"{chr(65+i%26)}{chr(65+(i*3)%26)}{chr(65+(i*7)%26)}-{str(i%10000).zfill(4)}"
    ))

spark.createDataFrame(vehicles_data).write.mode("append").saveAsTable("vehicles")
print(f"vehicles: {len(vehicles_data)} rows")

# COMMAND ----------

# Beneficiaries (80,000 rows)
bfn = ['Emma','Liam','Olivia','Noah','Ava','Ethan','Sophia','Mason',
       'Isabella','Logan','Mia','Lucas','Charlotte','Aiden','Amelia']
bln = ['Smith','Johnson','Brown','Davis','Wilson','Moore','Taylor','Anderson',
       'Thomas','Jackson','White','Harris','Martin','Garcia','Martinez','Clark',
       'Lewis','Lee','Walker','Hall']
rels = ['Spouse','Child','Parent','Sibling','Other']

beneficiaries_data = []
for i in range(1, 80001):
    beneficiaries_data.append(Row(
        beneficiary_id=i, policy_id=((i-1)%100000)+1,
        first_name=bfn[i%15], last_name=bln[(i*11)%20],
        relationship=rels[i%5], percentage=float(round(100.0/(1+(i%3)), 2)),
        date_of_birth=date(1960,1,1)+timedelta(days=(i*29)%18000)
    ))

spark.createDataFrame(beneficiaries_data).write.mode("append").saveAsTable("beneficiaries")
print(f"beneficiaries: {len(beneficiaries_data)} rows")

# COMMAND ----------

# Coverages (100,000 rows)
cov_types = ['Liability','Collision','Comprehensive','Medical',
             'Uninsured Motorist','Property Damage','Personal Injury','Rental']

coverages_data = []
for i in range(1, 100001):
    coverages_data.append(Row(
        coverage_id=i, policy_id=((i-1)%100000)+1,
        coverage_type=cov_types[i%8],
        coverage_limit=float(round(10000+(i*43)%490000, 2)),
        deductible=float(round(250+(i*17)%4750, 2)),
        effective_date=date(2018,1,1)+timedelta(days=i%2500)
    ))

spark.createDataFrame(coverages_data).write.mode("append").saveAsTable("coverages")
print(f"coverages: {len(coverages_data)} rows")

# COMMAND ----------

# Premiums (100,000 rows)
methods = ['CREDIT_CARD','BANK_TRANSFER','CHECK','AUTO_PAY']

premiums_data = []
for i in range(1, 100001):
    due = date(2018,1,1)+timedelta(days=(i*3)%2500)
    paid = None if i%8==0 else due+timedelta(days=i%15)
    status = 'OVERDUE' if i%8==0 else ('PENDING' if i%12==0 else 'PAID')
    premiums_data.append(Row(
        premium_id=i, policy_id=((i-1)%100000)+1,
        amount=float(round(50+(i*23)%950, 2)),
        due_date=due, paid_date=paid,
        payment_method=methods[i%4], status=status
    ))

spark.createDataFrame(premiums_data).write.mode("append").saveAsTable("premiums")
print(f"premiums: {len(premiums_data)} rows")

# COMMAND ----------

# Claims (80,000 rows)
claim_types = ['Accident','Theft','Fire','Water Damage','Natural Disaster','Vandalism']
claim_statuses = ['OPEN','UNDER_REVIEW','APPROVED','DENIED','CLOSED']
descs = ['Vehicle collision at intersection','Property stolen from premises',
         'Fire damage to property','Water damage from pipe burst',
         'Storm damage to roof','Vandalism to vehicle']

claims_data = []
for i in range(1, 80001):
    claims_data.append(Row(
        claim_id=i, policy_id=((i-1)%100000)+1,
        claim_number=f"CLM-{str(i).zfill(8)}",
        claim_type=claim_types[i%6],
        incident_date=date(2019,1,1)+timedelta(days=i%2200),
        report_date=date(2019,1,1)+timedelta(days=(i%2200)+(i%30)),
        claim_amount=float(round(500+(i*67)%49500, 2)),
        status=claim_statuses[i%5], description=descs[i%6]
    ))

spark.createDataFrame(claims_data).write.mode("append").saveAsTable("claims")
print(f"claims: {len(claims_data)} rows")

# COMMAND ----------

# Claim Payments (100,000 rows)
pay_types = ['SETTLEMENT','PARTIAL','FINAL']
pay_statuses = ['PROCESSED','PENDING','COMPLETED','VOID']
notes = ['Initial payment','Supplemental payment','Final settlement','Adjusted amount','Expedited processing']

payments_data = []
for i in range(1, 100001):
    payments_data.append(Row(
        payment_id=i, claim_id=((i-1)%80000)+1,
        amount=float(round(100+(i*31)%9900, 2)),
        payment_date=date(2019,3,1)+timedelta(days=i%2100),
        payment_type=pay_types[i%3], status=pay_statuses[i%4],
        notes=notes[i%5]
    ))

spark.createDataFrame(payments_data).write.mode("append").saveAsTable("claim_payments")
print(f"claim_payments: {len(payments_data)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

print(f"\n{'='*50}")
print(f"Lakehouse Setup Complete: {catalog}.{schema}")
print(f"{'='*50}")
for t in ["policy_types", "agents", "customers", "policies", "vehicles",
          "beneficiaries", "coverages", "premiums", "claims", "claim_payments"]:
    cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {t}").collect()[0]["cnt"]
    print(f"  {t}: {cnt:,} rows")
