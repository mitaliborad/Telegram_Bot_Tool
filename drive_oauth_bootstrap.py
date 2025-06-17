from __future__ import print_function
import os, pickle, pathlib, json
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/drive']
CLIENT_JSON = 'client_secret.json'   # the file you just downloaded

flow = InstalledAppFlow.from_client_secrets_file(CLIENT_JSON, SCOPES)
creds = flow.run_local_server(port=0)

print("REFRESH_TOKEN =", creds.refresh_token)
print("ACCESS_TOKEN  =", creds.token)

# Optional: quick sanity check (list first 5 files)
service = build('drive', 'v3', credentials=creds)
results = service.files().list(pageSize=5, fields="files(id, name)").execute()
print("Sample files:", results.get('files', []))
