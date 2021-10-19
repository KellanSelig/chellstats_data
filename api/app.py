from fastapi import FastAPI

app = FastAPI()


@app.route("/")
def ping() -> str:
    return "Hello World"
