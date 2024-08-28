from sams.etl.orchestrate import SAMSDataOrchestrator

orchestrator = SAMSDataOrchestrator()
    
try:
    orchestrator.process_data()
finally:
    orchestrator.close()
