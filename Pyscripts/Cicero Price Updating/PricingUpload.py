# %%
import pandas as pd
import numpy as np
import requests
import json

#TODO: To change following variables.
source_csv = "Data/3M_pricing_clean.csv"
output_csv = "Data/products_present_sku_17feb.csv"
jsonOutputFileLocation = "Output/JSON/missingProds.csv"
noPriceCsvFile = "Output/JSON/NoPriceProducts.csv"
jsonRequestFolder = "Output/JSON/PriceJson/"
username = ""
password = ""

# %%
db = pd.read_csv(source_csv)

# %%
db2 = pd.read_csv(output_csv)

# %%
price3m = pd.DataFrame(db)
price3m.head()

# %%
prods = pd.DataFrame(db2,columns=["sku"])
prods.head()

# %%
def product(sku,price):
    indiProd = {
                    "price": str(price),
                    "store_id": str(0),
                    "sku": str(sku),
                }
    return indiProd
def query(payload):
    query = {
                "prices": payload
            }
    return query

# %%
prod_list = list(prods["sku"])
payload = []
req = []
reqNum = 1
with open(jsonOutputFileLocation,"a") as file:
            file.write("sku"+"\n")
file.close()
for i in range(len(price3m)):
    sku = price3m.iloc[i,0]
    if str(sku) not in prod_list:
        with open(jsonOutputFileLocation,"a") as file:
            file.write(str(sku)+"\n")
        continue
    else:
        price = price3m.iloc[i,1]
        if not pd.isna(price):
            payload.append(product(sku,price))
        else:
            payload.append(product(sku,10000))
            with open(noPriceCsvFile,"a") as nop:
                nop.write(str(sku)+"\n")
                nop.close()
        if len(payload)>=10000:
            singleLoad = query(payload)
            req.append(json.dumps(singleLoad))
            print(len(payload))
            with open(jsonRequestFolder+"request"+str(reqNum)+".json","a") as j:
                j.write(json.dumps(singleLoad))
                j.close()
            reqNum+=1
            payload = []
with open(jsonRequestFolder+"requestLast.json", "a") as j:
    singleLoad = query(payload)
    req.append(json.dumps(singleLoad))
    print(len(payload))
    j.write(json.dumps(singleLoad))
    j.close()
file.close()


# %%
def authToken(url_key,username,password):
    r1 = requests.post(url=url_key,data=json.dumps({"username":username,"password":password}),headers={"Content-Type":"application/json"})
    return r1.json()
base_cicero_url = "https://stage-cicerosupply.humdash.com"
ext = "/rest/V1/integration/admin/token"
cicero_token = authToken(base_cicero_url+ext,username,password)
cicero_token

# %%
cicero_url = "https://stage-cicerosupply.humdash.com/rest/V1/products/base-prices"

headers = {"Authorization":"Bearer {}".format(cicero_token),\
            "Content-Type": "application/json"}

responses = []
for i in req:
    responses.append(requests.post(url = cicero_url, headers=headers,data=i))

# %%
responses


