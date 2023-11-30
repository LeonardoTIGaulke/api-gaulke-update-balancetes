from stable import get_all_data_to_google_sheets

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/update-balancete")
def read_item():
    try:
        process = get_all_data_to_google_sheets()
        return {"code": process}
    except:
        return {"code": 500}