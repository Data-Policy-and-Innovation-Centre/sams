from sams.etl.orchestrate import SAMSDataOrchestrator
from sams.config import API_AUTH
from pathlib import Path

if not Path(API_AUTH).exists():
    raise FileNotFoundError(f"API_AUTH file not found at {API_AUTH}")

orchestrator = SAMSDataOrchestrator()    
orchestrator.process_data("students")
