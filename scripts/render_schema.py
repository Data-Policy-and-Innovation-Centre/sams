from pathlib import Path
from eralchemy import render_er
from sams.config import datasets

# Build the DB URL (points at the local sqlite file)
db_file = datasets["sams"]["path"]
db_url = f"sqlite:///{Path(db_file).resolve()}"

# Set output path to notebooks folder
project_root = Path(__file__).resolve().parent.parent
output_png = project_root / "notebooks" / "schema_graph.png"

# Ensure the notebooks folder exists
output_png.parent.mkdir(parents=True, exist_ok=True)

# Render the ER diagram
render_er(db_url, str(output_png))
print(f"Schema diagram written to {output_png}")
