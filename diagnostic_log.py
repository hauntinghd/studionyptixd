import logging
logging.basicConfig(filename="shadow_v4.log", level=logging.INFO)
try:
    import dotenv
    logging.info("dotenv OK")
except Exception as e:
    logging.error(f"dotenv FAIL: {e}")

try:
    import py_clob_client
    logging.info("py_clob_client OK")
except Exception as e:
    logging.error(f"py_clob_client FAIL: {e}")

print("Diagnostic complete. Check shadow_v4.log")
