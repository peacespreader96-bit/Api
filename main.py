import os
import duckdb
import httpx
import time
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse

app = FastAPI(title="HiTeck Parquet API")

TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATASET_ID = "HiTeckGroup/HiTeckNuMinfo"

# Cache for the Parquet URLs so we don't spam the HF API
cached_urls = []
last_cache_time = 0
CACHE_EXPIRY = 3600  # 1 hour

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
    
    if search_term.isdigit():
        query = f"SELECT name, fname, address, mobile FROM read_parquet([{urls_str}]) WHERE mobile = '{search_term}' LIMIT 5;"
    else:
        query = f"SELECT name, fname, address, mobile FROM read_parquet([{urls_str}]) WHERE LOWER(name) LIKE '%{search_term.lower()}%' OR LOWER(fname) LIKE '%{search_term.lower()}%' LIMIT 5;"
        
    try:
        result = con.execute(query).fetchall()
        return [{"name": r[0], "fname": r[1], "address": r[2], "mobile": r[3]} for r in result]
    except Exception as e:
        return [{"error": str(e)}]

async def process_search(chat_id, text):
    await send_message(chat_id, f"Searching for `{text}`...")
    results = search_parquet(text)
    
    if results and "error" in results[0]:
        await send_message(chat_id, f"API Error: {results[0]['error']}")
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

@app.get("/api/search")
async def api_search(q: str):
    results = search_parquet(q)
    return JSONResponse(content={"query": q, "results": results})

@app.post("/webhook")
async def telegram_webhook(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    
    if "message" not in data or "text" not in data["message"]:
        return {"ok": True}
        
    chat_id = data["message"]["chat"]["id"]
    text = data["message"]["text"].strip()
    
    if text == "/start":
        background_tasks.add_task(send_message, chat_id, "Hey LO. Send me a 10-digit mobile number or a name.")
        return {"ok": True}
        
    if not cached_urls:
        await get_parquet_urls()
        
    background_tasks.add_task(process_search, chat_id, text)
    
    return {"ok": True}
