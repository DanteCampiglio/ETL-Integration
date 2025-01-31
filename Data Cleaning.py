#Standardize company names and industries

import pandas as pd
import re

def standardize_company_name(name):
    # Remove any non-alphanumeric characters except spaces
    name = re.sub(r'[^\w\s]', '', name)
    # Convert to title case
    return name.strip().title()

def standardize_industry(industry):
    # Create a mapping of common variations to standard names
    industry_map = {
        'tech': 'Technology',
        'it': 'Information Technology',
        'finance': 'Financial Services',
        # Add more mappings as needed
    }
    
    industry = industry.lower().strip()
    return industry_map.get(industry, industry.title())

# Apply standardization to companies DataFrame
companies_df['name'] = companies_df['name'].apply(standardize_company_name)
companies_df['industry'] = companies_df['industry'].apply(standardize_industry)



#Remove duplicates 

# Identify duplicate contacts based on email
duplicate_contacts = contacts_df[contacts_df.duplicated(subset=['email'], keep=False)]

## There are no duplicates 

### Normalize phone number 


import phonenumbers

def normalize_phone(phone):
    try:
        parsed_number = phonenumbers.parse(phone, "US")  # I am assuming that are US numbers 
        return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
    except:
        return None

def normalize_email(email):
    return email.lower().strip()

contacts_df['phone'] = contacts_df['phone'].apply(normalize_phone)
contacts_df['email'] = contacts_df['email'].apply(normalize_email)



##Handle missing values appropriately


# Fill missing values in companies DataFrame.If null category goes unknown , if int goes 0-> to confirm with stakeholder
companies_df['size'].fillna('Unknown', inplace=True)
companies_df['annual_revenue'].fillna(0, inplace=True)

# Fill missing values in contacts DataFrame
contacts_df['title'].fillna('Unknown', inplace=True)

# Fill missing values in opportunities DataFrame
opportunities_df['probability'].fillna(0, inplace=True)
opportunities_df['forecast_category'].fillna('Unknown', inplace=True)

# If important fields are null , remove row 
companies_df.dropna(subset=['name', 'created_date', 'is_customer'], inplace=True)
contacts_df.dropna(subset=['email', 'first_name', 'last_name', 'status', 'created_date', 'last_modified'], inplace=True)
opportunities_df.dropna(subset=['name', 'company_id', 'amount', 'stage', 'created_date', 'is_closed'], inplace=True)
activities_df.dropna(subset=['contact_id', 'type', 'subject', 'timestamp'], inplace=True)


###Validate date formats and ranges:


from datetime import datetime, timedelta

def validate_date(date_str):
    try:
        date = pd.to_datetime(date_str)
        if date > datetime.now() + timedelta(days=365):  # Future dates more than a year ahead are likely errors
            return None
        return date
    except:
        return None

# Apply date validation to all date columns
date_columns = {
    'companies_df': ['created_date'],
    'contacts_df': ['created_date', 'last_modified'],
    'opportunities_df': ['created_date', 'close_date'],
    'activities_df': ['timestamp']
}

for df_name, columns in date_columns.items():
    df = globals()[df_name]
    for col in columns:
        df[col] = df[col].apply(validate_date)
    df.dropna(subset=columns, inplace=True)