from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Calendar integration will be ebic"}
