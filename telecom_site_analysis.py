from datetime import datetime, timedelta
import io
import requests
import pandas as pd
from sqlalchemy import create_engine

# Define GitHub Base URL
GITHUB_BASE_URL = "https://raw.githubusercontent.com/GayathriBS25/TelecomSiteAnalysis/main/data/"

def fetch_available_csv(technology):
    """Fetches and merges only available CSV files for a given technology."""
    df_list = []
    available_days = []
    
    for day in range(1, 32):
        url = f"{GITHUB_BASE_URL}{technology}day{day}.csv"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                available_days.append(day)
                #print(f"Fetching: {url}")
                
                csv_data = io.StringIO(response.text)
                df = pd.read_csv(csv_data, sep=';')
                df.columns = df.columns.str.strip()
                
                df_list.append(df)
        except requests.RequestException as e:
            print(f"Error fetching {url}: {e}")
    
    #print(f"Available days for {technology}: {available_days}")
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

def process_and_store_data():
    """Processes the telecom data and stores it in PostgreSQL."""
    # Fetch data from GitHub
    gsm_df = fetch_available_csv("gsm")
    umts_df = fetch_available_csv("umts")
    lte_df = fetch_available_csv("lte")
    site_df = fetch_available_csv("site")

    # Assign Technology Labels
    gsm_df["technology"] = "2g" if not gsm_df.empty else None
    umts_df["technology"] = "3g" if not umts_df.empty else None
    lte_df["technology"] = "4g" if not lte_df.empty else None

    # Merge All Cells
    total_cells_df = pd.concat([gsm_df, umts_df, lte_df], ignore_index=True)

    # Join with Site Data
    merged_df = total_cells_df.merge(site_df, on="site_id", how="inner")

    # Count Cells Per Technology Per Site
    cell_count = merged_df.groupby("site_id")["technology"].value_counts().unstack(fill_value=0)
    cell_count.columns = [f"site_{col}_cnt" for col in cell_count.columns]

    # Calculate Frequency Bands Per Site
    band_df = merged_df.groupby(["site_id", "technology", "frequency_band"]).size().unstack(fill_value=0)
    band_df.columns = [f"frequency_band_{col}_by_site" for col in band_df.columns]

    # Combine all results
    final_df = cell_count.join(band_df, how="left")

    # Saving Results to PostgreSQL
    #PostgreSQL connection details
    db_username = 'postgres'
    db_password = 'gayathri98!'
    db_host = 'localhost'  
    db_port = '5432'        
    db_name = 'TELECOM'

    # Creating the connection string
    connection_string = f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

    # Creating a SQLAlchemy engine
    engine = create_engine(connection_string)

    # Loading data into a DataFrame
    df = final_df

    # Load data into PostgreSQL
    table_name = 'site_status'
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    
    print("Processing complete! Data stored in PostgreSQL.")


process_and_store_data()