import git
import os
import pandas as pd
import json

def clone_or_pull_repository(repo_url, local_path):
    if os.path.exists(local_path):
        repo = git.Repo(local_path)
        origin = repo.remotes.origin
        origin.pull()
        print(f"Repository updated in {local_path}")
    else:
        git.Repo.clone_from(repo_url, local_path)
        print(f"Repository cloned to {local_path}")

def extract_data_from_git(repo_url, local_path):
    clone_or_pull_repository(repo_url, local_path)
    
    # Read CSV files
    companies_df = pd.read_csv(os.path.join(local_path, 'data', 'salesforce', 'companies.csv'))
    opportunities_df = pd.read_csv(os.path.join(local_path, 'data', 'salesforce', 'opportunities.csv'))
    
    # Read JSON files and convert to DataFrames
    with open(os.path.join(local_path, 'data', 'salesforce', 'contacts.json'), 'r') as f:
        contacts_data = json.load(f)
    contacts_df = pd.json_normalize(contacts_data)
    
    with open(os.path.join(local_path, 'data', 'salesforce', 'activities.json'), 'r') as f:
        activities_data = json.load(f)
    activities_df = pd.json_normalize(activities_data)
    
    return companies_df, opportunities_df, contacts_df, activities_df

# Usage
repo_url = 'https://github.com/Alysio-io/alysio-data-engineer-challenge.git'
local_path = './alysio_challenge'
companies_df, opportunities_df, contacts_df, activities_df = extract_data_from_git(repo_url, local_path)



