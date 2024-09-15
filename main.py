import subprocess

def run_script(script_name):
    print(f"Running {script_name}...")
    try:
        subprocess.run(["python", script_name], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e}")
        exit(1)


# Execute the scripts in the specified order
# run_script("price_target.py")
# run_script("analysts.py")
# run_script("stock_price.py")
# run_script("analysis.py")
# run_script("portfolio.py")



# run_script("price_history.py")
# run_script("price_target_history.py")

run_script("analysis_simulation.py")
# run_script("portfolio_simulation.py")
run_script("portfolio.py")

