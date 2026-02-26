import traceback
try:
    import backend
    with open('python_error.log', 'w') as f:
        f.write('SUCCESS')
except Exception as e:
    with open('python_error.log', 'w') as f:
        traceback.print_exc(file=f)
