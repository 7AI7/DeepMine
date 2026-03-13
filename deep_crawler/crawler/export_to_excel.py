
"""
Export PostgreSQL data to Excel with multiple sheets
Company IDs 1-200
"""

import pandas as pd
import psycopg2
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# Database connection
conn = psycopg2.connect(
    host="localhost",
    database="Scraped",
    user="postgres",
    password=os.environ.get("DB_PASS", "your_db_password")
)

# Create Excel writer
excel_file = "companies_1_200_export.xlsx"
writer = pd.ExcelWriter(excel_file, engine='openpyxl')

print("Exporting data...")

# 1. Export Companies
print("  → Companies...")
query_companies = """
    SELECT * FROM companies 
    WHERE id >= 1 AND id <= 200
    ORDER BY id
"""
df_companies = pd.read_sql(query_companies, conn)
df_companies.to_excel(writer, sheet_name='Companies', index=False)
print(f"     Exported {len(df_companies)} companies")

# 2. Export Products
print("  → Products...")
query_products = """
    SELECT * FROM products 
    WHERE company_id >= 1 AND company_id <= 200
    ORDER BY company_id, id
"""
df_products = pd.read_sql(query_products, conn)
df_products.to_excel(writer, sheet_name='Products', index=False)
print(f"     Exported {len(df_products)} products")

# 3. Export Addresses
print("  → Addresses...")
query_addresses = """
    SELECT * FROM addresses 
    WHERE company_id >= 1 AND company_id <= 200
    ORDER BY company_id, id
"""
df_addresses = pd.read_sql(query_addresses, conn)
df_addresses.to_excel(writer, sheet_name='Addresses', index=False)
print(f"     Exported {len(df_addresses)} addresses")

# 4. Export Clients
print("  → Clients...")
query_clients = """
    SELECT * FROM clients 
    WHERE company_id >= 1 AND company_id <= 200
    ORDER BY company_id, id
"""
df_clients = pd.read_sql(query_clients, conn)
df_clients.to_excel(writer, sheet_name='Clients', index=False)
print(f"     Exported {len(df_clients)} clients")

# 5. Export Management
print("  → Management...")
query_management = """
    SELECT * FROM management 
    WHERE company_id >= 1 AND company_id <= 200
    ORDER BY company_id, id
"""
df_management = pd.read_sql(query_management, conn)
df_management.to_excel(writer, sheet_name='Management', index=False)
print(f"     Exported {len(df_management)} management")

# 6. Export Infrastructure Blocks
print("  → Infrastructure Blocks...")
query_blocks = """
    SELECT * FROM company_infra_blocks 
    WHERE company_id >= 1 AND company_id <= 200
    ORDER BY company_id, id
"""
df_blocks = pd.read_sql(query_blocks, conn)
df_blocks.to_excel(writer, sheet_name='Infrastructure', index=False)
print(f"     Exported {len(df_blocks)} infrastructure blocks")

# 7. Export Machines
print("  → Machines...")
query_machines = """
    SELECT * FROM company_machines 
    WHERE company_id >= 1 AND company_id <= 200
    ORDER BY company_id, id
"""
df_machines = pd.read_sql(query_machines, conn)
df_machines.to_excel(writer, sheet_name='Machines', index=False)
print(f"     Exported {len(df_machines)} machines")

# Save and close
writer.close()
conn.close()

print(f"\n✅ Export complete: {excel_file}")
print(f"   Total sheets: 7")
