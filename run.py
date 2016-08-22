#!flask/bin/python
from app import app
#app.run(debug=True, port=5555)
app.run(host='0.0.0.0', port=5555)
