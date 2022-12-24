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

load_dotenv()
data = json.load(open('data.json'))


# Add successful sales data and the floor one week ago data
def moonsama_marketplace_price(contract, link):

	query = f"""{{ 
		latestOrders: 
			orders( where: {{
				active: true, buyAsset: \"0x0000000000000000000000000000000000000000-0\", sellAsset_starts_with: \"{contract.lower()}\"
				}} 
			orderBy: pricePerUnit orderDirection: asc skip: 0 first: 1 ) {{ 
				id orderType createdAt active pricePerUnit 
				}}
			}}"""
	
	resp = (requests.post(link, json={"query": query})).json()
	floor = int(resp["data"]["latestOrders"][0]["pricePerUnit"])
	return floor


def moonbeans_price(contract, link):
	query = f"""{{ 
		allAsks(condition: {{collectionId: \"{contract}\"}}, 
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
	soup = bs(resp.content, features="html.parser")
	parent_element = soup.find("div", string="Min Price").parent
	floor = int(parent_element.find("img").next_sibling.strip()) * 10**18
	return floor

def database_price(contract):
	PASSWORD = os.getenv('MONGODBPASSWORD')
	client = pymongo.MongoClient(f"mongodb+srv://ninja:{PASSWORD}@oracle-atlas.2mwhyc5.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))
	table = client["nft_collections_moonriver"][contract]
	results = table.find({"timestamp": {"$gte": int(time.time()) - 7*24*3600 }}) #fetching prices in the last week

	prices = []
	for result in results:
		prices.append(int(result["price"]))
	
	return min(prices)


def return_floor(chainId, contract):
	print(chainId, contract)

	collection = {}
	for nft in data["collections_supported"]:
		if nft["contract"] == contract and nft["chainId"] == chainId:
			collection = nft

	if not bool(collection):
		return "500Error"

	prices = []

	for marketplace in collection["marketplaces"]:
		if marketplace["name"] == "Moonsama Marketplace":
			try:
				price = moonsama_marketplace_price(collection["contract"], marketplace["link"])
				prices.append(price)
			except:
				pass

		elif marketplace["name"] == "Moonbeans":
			try:
				price = moonbeans_price(collection["contract"], marketplace["link"])
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
		price = database_price(collection["contract"])
		assert(type(price) == int)
		prices.append(price)
	except:
		pass

	final_floor = 0

	if len(prices) == 0:
		return final_floor
	
	final_floor = min(prices)
	return final_floor


def signature(price, deadline, chainId, contract):
		
	w3 = Web3(EthereumTesterProvider())

	price = w3.toHex(w3.toBytes(price).rjust(27, b'\0'))[2:]
	deadline = w3.toHex(w3.toBytes(deadline).rjust(32, b'\0'))[2:]
	chainId = w3.toHex(w3.toBytes(chainId).rjust(32, b'\0'))[2:]
	contract = contract[2:]

	message = (price+deadline+chainId+contract)
	signable_message = encode_defunct(hexstr = message)
	key = os.getenv('KEY')
	assert key is not None

	signed_message = w3.eth.account.sign_message(signable_message, private_key=key)
	v = signed_message.v
	r = w3.toHex(w3.toBytes(signed_message.r).rjust(32, b'\0'))
	s = w3.toHex(w3.toBytes(signed_message.s).rjust(32, b'\0'))

	return(v,r,s)


app = FastAPI()

@app.get('/quote/{chainId}/{contract}')
def returnValue(chainId: int, contract: str):
	# return(chainId, contract)

	price = return_floor(chainId, contract)

	if price == "500Error":
		raise HTTPException(status_code=500, detail="Internal Server Error")
	
	deadline = int(time.time()) + 1200  #20 minutes in the future

	assert(type(price) == int)

	try:
		v,r,s = signature(price, deadline, chainId, contract)
	except:
		raise HTTPException(status_code=500, detail="Internal Server Error")

	obj = {
		"price": price,
		"deadline": deadline,
		"normalizedNftContract": contract,
		"signature":{
			"v": v,
			"r": r,
			"s": s
		}
	}
	return obj