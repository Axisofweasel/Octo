import requests
import os
from dotenv import load_dotenv

load_dotenv()

api = os.getenv("APIKEY")

class OctoApi:
    
    def __init__(self):
        self.r = requests.get('https://api.octopus.energy')
        self.status = None
        
    def call(self,apikey):
        self.r
        self.status = self.r.status_code
        
        print(self.status)