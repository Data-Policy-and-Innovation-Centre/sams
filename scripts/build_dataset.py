from sams.etl.orchestrate import SAMSDataOrchestrator
from sams.config import PROJ_ROOT
from pathlib import Path

if not Path(PROJ_ROOT / ".env").exists():
    raise FileNotFoundError(f".env file not found at {PROJ_ROOT}/.env")

orchestrator = SAMSDataOrchestrator()    
orchestrator.process_data("students",exclude=[("PDIS",2020),("PDIS",2021),("PDIS",2022),("PDIS",2023),("PDIS",2024),
                                              ("ITI",2017),("ITI",2018),("ITI",2019),
                                              ("ITI",2020),("ITI",2021),("ITI",2022),
                                              ("ITI",2023),("ITI",2024),("Diploma",2018),("Diploma",2019),("Diploma",2020),("Diploma",2021),("Diploma",2022),("Diploma",2023)])
