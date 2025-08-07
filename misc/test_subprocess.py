import subprocess

if __name__ == "__main__":
    print("testing...")
    
    try:
        result = subprocess.run(['python', 'update_stop_ids.py'], 
                                capture_output=True, text=True, check=True)
        print("Update stop_ids script completed successfully")
        if result.stdout:
            print("Script output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Update stop_ids script failed with exit code {e.returncode}")
        print(f"Error output: {e.stderr}")
        if e.stdout:
            print(f"Standard output: {e.stdout}")
