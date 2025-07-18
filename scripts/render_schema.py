import os
from pathlib import Path
from eralchemy import render_er
from sqlalchemy import create_engine
from sams.config import datasets

# 1. Build the DB URL (points at the local sqlite file)
db_file = datasets["sams"]["path"]  # e.g. "./data/sams.sqlite"
db_url  = f"sqlite:///{Path(db_file).resolve()}"

# 2. Compute project root (two levels up from this script)
project_root = Path(__file__).resolve().parent.parent

# 3. Prepare the schema output folder under project_root/output/figures
schema_dir = project_root / "output" / "figures"
schema_dir.mkdir(parents=True, exist_ok=True)

output_png = schema_dir / "schema_graph.png"

# 4. Render the ER diagram
render_er(db_url, str(output_png))

print(f"✓Schema diagram written to {output_png}")

