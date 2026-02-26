try:
    import py_clob_client
    print(f"py-clob-client version: {py_clob_client.__version__}")
except ImportError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"Other Error: {e}")
