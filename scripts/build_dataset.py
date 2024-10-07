from sams.etl.orchestrate import SAMSDataOrchestrator
from sams.config import PROJ_ROOT
from pathlib import Path

if not Path(PROJ_ROOT / ".env").exists():
    raise FileNotFoundError(f".env file not found at {PROJ_ROOT}/.env")

orchestrator = SAMSDataOrchestrator()    
orchestrator.process_data("students")
