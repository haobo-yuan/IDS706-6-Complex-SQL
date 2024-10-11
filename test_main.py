import subprocess

def test_main():
    # Run main.py and capture the output
    result = subprocess.run(['python', 'main.py'], capture_output=True, text=True)
    # Check if the script ran successfully
    assert result.returncode == 0
    print("main.py ran successfully.")
    # Optionally, you can print or check the output
    # print(result.stdout)

if __name__ == "__main__":
    test_main()
    print("test_main.py passed.")
