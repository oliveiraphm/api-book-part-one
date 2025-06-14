import os 
from dotenv import load_dotenv

load_dotenv()

class SWCConfig:

    swc_base_url: str
    swc_backoff: bool
    swc_backoff_max_time: int
    swc_bulk_file_format: str

    def __init__(
        self,
        swc_base_url: str = None,
        backoff: bool = True,
        backoff_max_time: int = 30,
        bulk_file_format: str = "csv",      
    ):
        self.swc_base_url = swc_base_url or os.getenv("SWC_API_BASE_URL")
        print(f"SWC_API_BASE_URL in SWCConfig init: {self.swc_base_url}")

        if not self.swc_base_url:
            raise ValueError("Base URL is required. Set SWC_API_BASE_URL environment variable.")
        
        self.swc_backoff = backoff
        self.swc_backoff_max_time = backoff_max_time
        self.swc_bulk_file_format = bulk_file_format

    def __str__(self):
        return f"{self.swc_base_url} {self.swc_backoff} {self.swc_backoff_max_time} {self.swc_bulk_file_format}"