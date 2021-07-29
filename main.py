from fastapi import FastAPI

app = FastAPI()


@app.route("/")
def ping():
    return "Hello World"
