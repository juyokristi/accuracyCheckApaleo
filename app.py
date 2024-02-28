# Import necessary libraries
import streamlit as st
import requests
import pandas as pd
import base64
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Authentication function
def get_access_token(client_id, client_secret):
    url = 'https://identity.apaleo.com/connect/token'
    creds = f"{client_id}:{client_secret}"
    encoded_creds = base64.b64encode(creds.encode()).decode()
    headers = {
        'Authorization': f'Basic {encoded_creds}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = 'grant_type=client_credentials'
    response = requests.post(url, headers=headers, data=data, timeout=10)
    response_data = response.json()
    return response_data['access_token']

# Function to fetch property performance
def get_property_performance(access_token, property_id, from_date, to_date):
    url = f'https://api.apaleo.com/reports/v1/reports/property-performance?propertyId={property_id}&from={from_date}&to={to_date}&expand=businessDays&timeSliceDefinitionIds={property_id}-NIGHT'
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(url, headers=headers, timeout=10)
    return response.json()

# Function to fetch revenue data
def get_revenue_data(access_token, property_id, date):
    if date > datetime.now().date():
        return 0, 0  # Future dates do not have data
    
    from_date = date.strftime('%Y-%m-%d')
    to_date = (date + timedelta(days=1)).strftime('%Y-%m-%d')
    url = f'https://api.apaleo.com/reports/v1/reports/revenues?from={from_date}&to={to_date}&propertyId={property_id}'
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {access_token}'}
    response = requests.get(url, headers=headers, timeout=10)
    data = response.json()

    net_amount = 0
    gross_amount = 0
    for entry in data.get("children", []):
        if entry["account"]["name"] == "Revenues Accommodation":
            net_amount = entry["netAmount"]["amount"]
            gross_amount = entry["grossAmount"]["amount"]
            break

    return net_amount, gross_amount

# User input for property performance report
st.title('Property Performance Report')
client_id = st.sidebar.text_input('Client ID')
client_secret = st.sidebar.text_input('Client Secret', type="password")
property_id = st.text_input('Property ID')
from_date = st.date_input('From Date', datetime.now())
to_date = st.date_input('To Date', datetime.now())

# Generate report and process data
if st.button('Generate Report', key='generate_report_button'):
    access_token = get_access_token(client_id, client_secret)
    if access_token:
        report_data = get_property_performance(access_token, property_id, from_date, to_date)
        business_days = report_data['businessDays']
        
        progress_bar = st.progress(0)
        total_days = len(business_days)
        data = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_date = {executor.submit(get_revenue_data, access_token, property_id, pd.to_datetime(day['businessDay']).date()): day for day in business_days}
            for i, future in enumerate(as_completed(future_to_date), 1):
                day = future_to_date[future]
                try:
                    net_revenue, gross_revenue = future.result()
                    data.append({
                        'businessDay': pd.to_datetime(day['businessDay']).date(),  # Ensure this is a date for proper sorting
                        'soldCount': day['soldCount'],
                        'noShowsCount': day['noShowsCount'],
                        'netAccommodationRevenue': day['netAccommodationRevenue']['amount'],
                        'netRevenue': net_revenue,
                        'grossRevenue': gross_revenue
                    })
                except Exception as exc:
                    st.error(f"Generated an exception: {exc}")
                
                progress_bar.progress(i / total_days)
        
        # Convert the list to a DataFrame
        df = pd.DataFrame(data)

        # Sort the DataFrame by the 'businessDay' column to ensure data is ordered by date
        df = df.sort_values(by='businessDay')

        # Convert the sorted DataFrame to CSV
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download report as CSV",
            data=csv,
            file_name='property_performance_report.csv',
            mime='text/csv',
        )
