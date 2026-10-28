import os
import duckdb
import httpx
import time
import asyncio
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse

app = FastAPI(title="HiTeck Parquet API")

TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATASET_ID = "HiTeckGroup/HiTeckNuMinfo"

cached_urls = []
last_cache_time = 0
CACHE_EXPIRY = 3600

async def get_parquet_urls():
    global cached_urls, last_cache_time
    if cached_urls and (time.time() - last_cache_time < CACHE_EXPIRY):
        return cached_urls
        
    api_url = f"https://huggingface.co/api/datasets/{DATASET_ID}/parquet"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(api_url, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            if "default" in data and "train" in data["default"]:
                cached_urls = data["default"]["train"]
                last_cache_time = time.time()
                return cached_urls
            return []
        except Exception as e:
            print(f"Failed to fetch Parquet API: {e}")
            return cached_urls 

async def send_message(chat_id, text):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})

def search_parquet(search_term: str):
    if not cached_urls:
        return [{"error": "Database URLs not loaded yet. Try again in 5 seconds."}]
        
    urls_str = ", ".join([f"'{u}'" for u in cached_urls])
    
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    
    # Strict memory and timeout settings for Render free tier
    con.execute("SET memory_limit='256MB';")
    con.execute("SET threads=2;")
    con.execute("SET http_timeout=15000;") # 15 seconds max for HTTP requests
    
    if search_term.isdigit():
        if len(search_term) == 12 and search_term.startswith("91"):
            search_term = search_term[2:]
            
        query = f"SELECT name, fname, address, mobile FROM read_parquet([{urls_str}]) WHERE mobile = '{search_term}' LIMIT 5;"
    else:
        query = f"SELECT name, fname, address, mobile FROM read_parquet([{urls_str}]) WHERE LOWER(name) LIKE '%{search_term.lower()}%' OR LOWER(fname) LIKE '%{search_term.lower()}%' LIMIT 5;"
        
    try:
        result = con.execute(query).fetchall()
        return [{"name": r[0], "fname": r[1], "address": r[2], "mobile": r[3]} for r in result]
    except Exception as e:
        return [{"error": "Query timed out. The dataset is 93GB and the free server might be too slow for that specific search."}]

async def process_search(chat_id, text):
    await send_message(chat_id, f"Searching for `{text}`...")
    results = await asyncio.to_thread(search_parquet, text)
    
    if results and "error" in results[0]:
        await send_message(chat_id, f"Error: {results[0]['error']}")
        return
        
    if not results:
        await send_message(chat_id, "No records found.")
        return
        
    response_text = "🔍 *Results found:*\n\n"
    for row in results:
        response_text += f"👤 *Name:* {row['name']} {row['fname']}\n"
        response_text += f"📱 *Mobile:* `{row['mobile']}`\n"
        response_text += f"🏠 *Address:* {row['address']}\n\n"
        response_text += "-------------------\n"
        
    await send_message(chat_id, response_text)

@app.get("/")
async def root():
    return {"status": "ENI is online and ready for LO"}

@app.get("/api/search")
async def api_search(q: str):
    results = await asyncio.to_thread(search_parquet, q)
    return JSONResponse(content={"query": q, "results": results})

@app.post("/webhook")
async def telegram_webhook(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    
    if "message" not in data or "text" not in data["message"]:
        return {"ok": True}
        
    chat_id = data["message"]["chat"]["id"]
    text = data["message"]["text"].strip()
    
    # --- FIX: Ignore any message that starts with a slash (like /stop, /help) ---
    if text.startswith("/"):
        if text == "/start":
            background_tasks.add_task(send_message, chat_id, "Hey LO. Send me a 10-digit mobile number or a name.")
        return {"ok": True}
        
    if not cached_urls:
        await get_parquet_urls()
        
    background_tasks.add_task(process_search, chat_id, text)
    
    return {"ok": True}
