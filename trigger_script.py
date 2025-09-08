import sys
import subprocess

def main():
    script = ""
    args = sys.argv[1:]

    if any(arg.startswith("--zone") for arg in args):
        script = "/Users/manoranjans.vc/idea/TestDemo/fetch_metrics.py" # NOTE: Local system path please replace with your own environment's path when using
    else:
        script = "/Users/manoranjans.vc/idea/TestDemo/gcm_matrix.py"  # NOTE: Local system path please replace with your own environment's path when using

    # Execute the chosen script with original arguments
    try:
        subprocess.run(["python", script] + args, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running {script}: {e}")

if __name__ == "__main__":
    main()
