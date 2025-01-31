import sqlite3
import logging
import pandas as pd

# Configuración de logging
logging.basicConfig(filename='etl.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Función para conectar a la base de datos
def connect_to_db():
    return sqlite3.connect('crm.db')

# Función para crear las tablas
def create_tables(conn):
    cursor = conn.cursor()
    
    # Tabla de empresas
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS companies (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        domain TEXT,
        industry TEXT,
        size TEXT,
        country TEXT,
        created_date DATE,
        is_customer BOOLEAN,
        annual_revenue REAL
    )
    ''')
    
    # Tabla de contactos
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS contacts (
        id INTEGER PRIMARY KEY,
        email TEXT,
        first_name TEXT,
        last_name TEXT,
        title TEXT,
        company_id INTEGER,
        phone TEXT,
        status TEXT,
        created_date DATE,
        last_modified DATE,
        FOREIGN KEY (company_id) REFERENCES companies(id)
    )
    ''')
    
    # Tabla de oportunidades
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS opportunities (
        id INTEGER PRIMARY KEY,
        name TEXT,
        contact_id INTEGER,
        company_id INTEGER,
        amount REAL,
        stage TEXT,
        product TEXT,
        probability REAL CHECK (probability >= 0 AND probability <= 100),
        created_date DATE,
        close_date DATE,
        is_closed BOOLEAN,
        forecast_category TEXT,
        FOREIGN KEY (company_id) REFERENCES companies(id),
        FOREIGN KEY (contact_id) REFERENCES contacts(id)
    )
    ''')
    
    # Tabla de actividades
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS activities (
        id INTEGER PRIMARY KEY,
        contact_id INTEGER,
        opportunity_id INTEGER,
        type TEXT,
        subject TEXT,
        timestamp DATETIME,
        duration_minutes INTEGER,
        outcome TEXT,
        notes TEXT,
        FOREIGN KEY (contact_id) REFERENCES contacts(id),
        FOREIGN KEY (opportunity_id) REFERENCES opportunities(id)
    )
    ''')
    
    # Crear índices
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_name ON companies(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_industry ON companies(industry)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_contact_email ON contacts(email)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_contact_company ON contacts(company_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_opportunity_company ON opportunities(company_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_opportunity_contact ON opportunities(contact_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_activity_contact ON activities(contact_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_activity_opportunity ON activities(opportunity_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_activity_timestamp ON activities(timestamp)')
    
    conn.commit()

# Funciones de carga de datos
def load_companies(conn, companies_df):
    companies_df.to_sql('companies', conn, if_exists='replace', index=False)
    logging.info(f"Loaded {len(companies_df)} companies")

def load_contacts(conn, contacts_df):
    contacts_df.to_sql('contacts', conn, if_exists='replace', index=False)
    logging.info(f"Loaded {len(contacts_df)} contacts")

def load_opportunities(conn, opportunities_df):
    opportunities_df.to_sql('opportunities', conn, if_exists='replace', index=False)
    logging.info(f"Loaded {len(opportunities_df)} opportunities")

def load_activities(conn, activities_df):
    activities_df['timestamp'] = pd.to_datetime(activities_df['timestamp'])
    activities_df.to_sql('activities', conn, if_exists='replace', index=False)
    logging.info(f"Loaded {len(activities_df)} activities")

# Función principal de ETL
def etl_process(companies_df, contacts_df, opportunities_df, activities_df):
    conn = connect_to_db()
    try:
        create_tables(conn)
        load_companies(conn, companies_df)
        load_contacts(conn, contacts_df)
        load_opportunities(conn, opportunities_df)
        load_activities(conn, activities_df)
        logging.info("ETL process completed successfully")
    except Exception as e:
        logging.error(f"Error during ETL process: {str(e)}")
    finally:
        conn.close()

# Función para ejecutar consultas de muestra
def run_sample_queries():
    conn = connect_to_db()
    
    print("Companies with their contacts:")
    df = pd.read_sql_query('''
        SELECT c.name, co.first_name, co.last_name, co.email
        FROM companies c
        LEFT JOIN contacts co ON c.id = co.company_id
        LIMIT 5
    ''', conn)
    print(df)
    
    print("\nOpportunities by stage:")
    df = pd.read_sql_query('''
        SELECT stage, COUNT(*) as count, SUM(amount) as total_amount
        FROM opportunities
        GROUP BY stage
    ''', conn)
    print(df)
    
    print("\nRecent activities for a specific contact:")
    df = pd.read_sql_query('''
        SELECT a.type, a.subject, a.timestamp, a.outcome
        FROM activities a
        JOIN contacts c ON a.contact_id = c.id
        WHERE c.email = ?
        ORDER BY a.timestamp DESC
        LIMIT 5
    ''', conn, params=('ejemplo@email.com',))
    print(df)
    
    conn.close()

# Ejecución principal
if __name__ == "__main__":
    # Asumiendo que los DataFrames ya están cargados en el entorno de Spyder
    etl_process(companies_df, contacts_df, opportunities_df, activities_df)
    run_sample_queries()