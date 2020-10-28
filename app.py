from flask import Flask
app = Flask(__name__)

@app.route("//store")
def hello():
    return "Hello, World!"