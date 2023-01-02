from fastapi import FastAPI, HTTPException
import os
import json
import requests
from bs4 import BeautifulSoup as bs 
import time
from web3 import Web3, EthereumTesterProvider
from eth_account.messages import encode_defunct
from dotenv import load_dotenv
import pymongo
from pymongo.server_api import ServerApi
from lxml import html

load_dotenv()
data = requests.get("https://raw.githubusercontent.com/moonlend/moonlend-nft-list/master/nft-list.json").json()

def moonsama_marketplace_price(address, link):

	query = f"""{{ 
		latestOrders: 
			orders( where: {{
				active: true, buyAsset: \"0x0000000000000000000000000000000000000000-0\", sellAsset_starts_with: \"{address}\"
				}} 
			orderBy: pricePerUnit orderDirection: asc skip: 0 first: 1 ) {{ 
				id orderType createdAt active pricePerUnit 
				}}
			}}"""
	resp = requests.post(link, json={"query": query}).json()
	floor = int(float(resp["data"]["latestOrders"][0]["pricePerUnit"]) // 10e17)
	return floor


def moonbeans_price(address, link):
	query = f"""{{ 
		allAsks(condition: {{collectionId: \"{address}\"}}, 
		orderBy: VALUE_ASC, first: 1) {{ 
			nodes {{ 
				id timestamp value __typename }} 
				__typename 
				}}
			}}"""
	
	resp = (requests.post(link, json={"query": query})).json()
	floor = int(resp["data"]["allAsks"]["nodes"][0]["value"])
	return floor


def raregems_price(link):
	resp = requests.get(link)
	tree = html.fromstring(resp.content)
	floor = int(float(tree.xpath("//html/body/main/div/div/div[1]/ul[2]/li[4]/div[2]/text()")[1].strip()) *10**18)
	return floor

def database_price(address):
	MONGODBPASSWORD = os.getenv('MONGODBPASSWORD')
	client = pymongo.MongoClient(f"mongodb+srv://ninja:{MONGODBPASSWORD}@oracle-atlas.2mwhyc5.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))
	table = client["nft_collections_moonriver"][address.lower()]
	results = table.find({"timestamp": {"$gte": int(time.time()) - 7*24*3600 }}).sort("price", pymongo.ASCENDING).limit(1)  #fetching prices in the last week
	record = results.next()
	floor = int(record["price"] * 10e18)
	return floor


def return_floor(chainId, address):
	print(chainId, address)

	collection = {}
	for nft in data["tokens"]:
		if nft["address"].lower() == address.lower() and chainId == 1285:
			collection = nft
			break
	
	if not bool(collection):
		return "500Error"

	prices = []

	for marketplace in collection["marketplaces"]:
		if marketplace["name"] == "Moonsama Marketplace":
			try:
				price = moonsama_marketplace_price(collection["address"], marketplace["link"])
				prices.append(price)
			except:
				pass

		elif marketplace["name"] == "Moonbeans":
			try:
				price = moonbeans_price(collection["address"], marketplace["link"])
				prices.append(price)
			except:
				pass
		elif marketplace["name"] == "Raregems":
			try:
				price = raregems_price(marketplace["link"])
				prices.append(price)
			except:
				pass
	
	try:
		price = database_price(collection["address"])
		assert(type(price) == int)
		prices.append(price)
	except:
		pass

	final_floor = 0

	if len(prices) == 0:
		return final_floor
	
	final_floor = min(prices)
	return final_floor


def signature(price, deadline, chainId, address):
	
	w3 = Web3(EthereumTesterProvider())

	price = w3.toHex(w3.toBytes(price).rjust(27, b'\0'))[2:]
	deadline = w3.toHex(w3.toBytes(deadline).rjust(32, b'\0'))[2:]
	chainId = w3.toHex(w3.toBytes(chainId).rjust(32, b'\0'))[2:]
	address = address[2:]

	message = (price+deadline+chainId+address)
	signable_message = encode_defunct(hexstr = message)
	key = os.getenv('KEY')
	assert key is not None

	signed_message = w3.eth.account.sign_message(signable_message, private_key=key)
	v = signed_message.v
	r = w3.toHex(w3.toBytes(signed_message.r).rjust(32, b'\0'))
	s = w3.toHex(w3.toBytes(signed_message.s).rjust(32, b'\0'))

	return(v,r,s)


app = FastAPI()

@app.get('/quote/{chainId}/{address}')
def returnValue(chainId: int, address: str):

	price = return_floor(chainId, address)

	if price == "500Error":
		raise HTTPException(status_code=500, detail="Internal Server Error")
	
	deadline = int(time.time()) + 1200  #20 minutes in the future

	assert(type(price) == int)

	try:
		v,r,s = signature(price, deadline, chainId, address)
	except:
		raise HTTPException(status_code=500, detail="Internal Server Error")

	obj = {
		"price": price,
		"deadline": deadline,
		"normalizedNftContract": address,
		"signature":{
			"v": v,
			"r": r,
			"s": s
		}
	}
	return obj