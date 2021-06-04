from dotenv import load_dotenv
import os

load_dotenv('Path to you .env file')
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')

API_KEY = os.environ.get('API_KEY')
API_SECRET = os.environ.get('API_SECRET')
API_URL = os.environ.get('API_URL')
