import requests
import sqlalchemy as sa
import pandas as pd
# Import Currency Exchange
cur_ex_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/rates_of_exchange"
cur_ex_params = {
    "fields": "record_date,country_currency_desc,exchange_rate",
    "filter": "record_date:gte:2015-01-01",
    "page[size]": 1000,
    "page[number]": 1
}
all_data = []

while True:
    response = requests.get(cur_ex_url, params=cur_ex_params)
    data = response.json()

    # Add current page's data to the list
    all_data.extend(data.get("data", []))

    # Check if there's a next page
    if "next" in data.get("links", {}):
        cur_ex_params["page[number]"] += 1
    else:
        break

# Now all_data contains everything from 2015 onward
print(f"Retrieved {len(all_data)} records.")

# convert list to Dataframe
cur_ex_url_df = pd.DataFrame(all_data)

# Connection string (with escaped password symbol)
connection_string = (
    "mssql+pyodbc://sa:MyPass%40word@localhost,1433/tempdb"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

engine = sa.create_engine(connection_string)

# Upload DataFrame to SQL Server
cur_ex_url_df.to_sql (
    name = "CurrencyEx",
    con=engine,
    if_exists="replace",
    index=False
)

print("Data Successfully Imported!")

