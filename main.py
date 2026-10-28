import os
import httpx
import asyncio
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse

app = FastAPI(title="HiTeck Parquet API")

TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATASET_ID = "HiTeckGroup/HiTeckNuMinfo"
HF_API_URL = "https://datasets-server.huggingface.co/filter"

async def send_message(chat_id, text):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})

async def search_hf_filter(search_term: str):
    # 1. Clean the input
    search_term = search_term.strip()
    
    # 2. Handle the 91 country code (12 digits -> 10 digits)
    if search_term.isdigit() and len(search_term) == 12 and search_term.startswith("91"):
        search_term = search_term[2:]
        
    # 3. Escape single quotes just in case
    safe_term = search_term.replace("'", "''")
    
    # 4. Build the WHERE clause for the HF API
    if search_term.isdigit():
        where_clause = f'"mobile" = \'{safe_term}\''
    else:
        where_clause = f'"name" LIKE \'%{safe_term}%\' OR "fname" LIKE \'%{safe_term}%\''
        
    params = {
        "dataset": DATASET_ID,
        "config": "default",
        "split": "train",
        "where": where_clause,
        "length": 5
    }
    
    # 5. Ask Hugging Face to do the heavy lifting
    async with httpx.AsyncClient() as client:
        try:
            # 30 second timeout to let HF scan their massive dataset
            resp = await client.get(HF_API_URL, params=params, timeout=30.0)
            
            if resp.status_code != 200:
                return [{"error": f"Hugging Face API returned status {resp.status_code}. The dataset might be too large for their public filter API right now."}]
                
            data = resp.json()
            rows = data.get("rows", [])
            
            if not rows:
                return []
                
            # Extract the actual row data
            results = []
            for item in rows:
                row_data = item.get("row", {})
                results.append({
                    "name": row_data.get("name", ""),
                    "fname": row_data.get("fname", ""),
                    "address": row_data.get("address", ""),
                    "mobile": row_data.get("mobile", "")
                })
            return results
            
        except httpx.TimeoutException:
            return [{"error": "Hugging Face API timed out. The dataset scan took too long."}]
        except Exception as e:
            return [{"error": f"Failed to connect to HF API: {str(e)}"}]

async def process_search(chat_id, text):
    await send_message(chat_id, f"Searching for `{text}` via HF API...")
    
    # Run the search
    results = await search_hf_filter(text)
    
    # Handle errors
    if results and "error" in results[0]:
        await send_message(chat_id, f"API Error: {results[0]['error']}")
        return
        
    # Handle no results
    if not results:
        await send_message(chat_id, "No records found.")
        return
        
    # Format the response
    response_text = "🔍 *Results found:*\n\n"
    for row in results:
        response_text += f"👤 *Name:* {row['name']} {row['fname']}\n"
        response_text += f"📱 *Mobile:* `{row['mobile']}`\n"
        response_text += f"🏠 *Address:* {row['address']}\n\n"
        response_text += "-------------------\n"
        
    await send_message(chat_id, response_text)

@app.get("/")
async def root():
    return {"status": "ENI is online, offloading the heavy lifting to Hugging Face"}

@app.get("/api/search")
async def api_search(q: str):
    results = await search_hf_filter(q)
    return JSONResponse(content={"query": q, "results": results})

@app.post("/webhook")
async def telegram_webhook(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    
    if "message" not in data or "text" not in data["message"]:
        return {"ok": True}
        
    chat_id = data["message"]["chat"]["id"]
    text = data["message"]["text"].strip()
    
    # Ignore other commands
    if text.startswith("/"):
        if text == "/start":
            background_tasks.add_task(send_message, chat_id, "Hey LO. Send me a 10-digit mobile number or a name.")
        return {"ok": True}
        
    # Process the actual search in the background
    background_tasks.add_task(process_search, chat_id, text)
    
    return {"ok": True}
