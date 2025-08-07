import shutil
from datetime import date

# Paths
ORIGINAL_DB = "crexi_tx_industrial.duckdb"
BACKUP_DB   = f"crexi_tx_industrial_backup_{date.today().isoformat()}.duckdb"

# Copy the file
shutil.copy(ORIGINAL_DB, BACKUP_DB)
print(f"Backed up {ORIGINAL_DB} → {BACKUP_DB}")
