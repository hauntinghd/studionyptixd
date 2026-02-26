import sys
with open("import_test.txt", "w") as f:
    f.write(f"Python: {sys.version}\n")
    try:
        import dotenv
        f.write("dotenv: OK\n")
    except Exception as e:
        f.write(f"dotenv: FAIL ({e})\n")
    
    try:
        import py_clob_client
        f.write("py_clob_client: OK\n")
    except Exception as e:
        f.write(f"py_clob_client: FAIL ({e})\n")
    
    try:
        import requests
        f.write("requests: OK\n")
    except Exception as e:
        f.write(f"requests: FAIL ({e})\n")
