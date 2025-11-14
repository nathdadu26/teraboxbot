import asyncio
import aiohttp
import logging
import os
import urllib.parse
import time
import re
import mimetypes
import sqlite3
from typing import Optional, Dict, Any, List, Tuple
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes, CommandHandler, Defaults
from telegram.constants import ParseMode
from dotenv import load_dotenv
from aiohttp import web

# Load environment variables from .env file
load_dotenv()

# Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Disable httpx and telegram library verbose logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)

# Config from environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
TERABOX_API = os.getenv("TERABOX_API", "")
HYDRAX_UPLOAD_API = os.getenv("HYDRAX_UPLOAD_API", "")

# Channels
TELEGRAM_CHANNEL_ID = int(os.getenv("TELEGRAM_CHANNEL_ID", ""))
RESULT_CHANNEL_ID = int(os.getenv("RESULT_CHANNEL_ID", ""))

# Webhook Configuration for Koyeb
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")  # Your Koyeb app URL
PORT = int(os.getenv("PORT", "8000"))  # Koyeb automatically sets PORT

# Aria2 Configuration
ARIA2_RPC_URL = os.getenv("ARIA2_RPC_URL", "http://localhost:6800/jsonrpc")
ARIA2_SECRET = os.getenv("ARIA2_SECRET", "mysecret")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/aria2_downloads")

# Terabox domains
TERABOX_DOMAINS = [
    "terabox.com", "1024terabox.com", "teraboxapp.com", "teraboxlink.com",
    "terasharelink.com", "terafileshare.com", "1024tera.com", "1024tera.cn",
    "teraboxdrive.com", "dubox.com"
]

DB_PATH = os.getenv("DB_PATH", "files.db")

# API timeout
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "30"))

# Validate required environment variables
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is required!")

# --- Database setup ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)")
    conn.commit()
    conn.close()

def is_file_processed(file_name: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM files WHERE name = ?", (file_name,))
    result = c.fetchone()
    conn.close()
    return result is not None

def save_file_name(file_name: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO files (name) VALUES (?)", (file_name,))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()

# ---------------- Aria2Client ----------------
class Aria2Client:
    def __init__(self, rpc_url: str, secret: Optional[str] = None):
        self.rpc_url = rpc_url
        self.secret = secret
        self.session: Optional[aiohttp.ClientSession] = None

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600))

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def _call_rpc(self, method: str, params: list = None):
        if params is None:
            params = []
        if self.secret:
            params.insert(0, f"token:{self.secret}")
        payload = {"jsonrpc": "2.0", "id": f"aria2_{int(time.time())}", "method": method, "params": params}
        try:
            await self.init_session()
            async with self.session.post(self.rpc_url, json=payload) as r:
                result = await r.json()
                if "error" in result:
                    return {"success": False, "error": result["error"]}
                return {"success": True, "result": result.get("result")}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def add_download(self, url: str, options: Dict[str, Any] = None):
        if options is None:
            options = {}
        opts = {"dir": DOWNLOAD_DIR, "continue": "true"}
        opts.update(options)
        return await self._call_rpc("aria2.addUri", [[url], opts])

    async def wait_for_download(self, gid: str):
        while True:
            status = await self._call_rpc("aria2.tellStatus", [gid])
            if not status["success"]:
                return status
            info = status["result"]
            if info["status"] == "complete":
                return {"success": True, "files": info["files"]}
            elif info["status"] in ["error", "removed"]:
                return {"success": False, "error": info.get("errorMessage", "Download failed")}
            await asyncio.sleep(2)

# ---------------- Bot Logic ----------------
class TeraboxHydraxBot:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.aria2 = Aria2Client(ARIA2_RPC_URL, ARIA2_SECRET)
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=API_TIMEOUT))

    def is_terabox_url(self, url: str) -> bool:
        try:
            domain = urllib.parse.urlparse(url).netloc.lower().removeprefix("www.")
            return domain in TERABOX_DOMAINS or any(d in domain for d in TERABOX_DOMAINS)
        except:
            return False

    async def download_from_terabox(self, url: str, max_retries: int = 3):
        """Download from Terabox with retry logic and proper timeout handling"""
        await self.init_session()
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Terabox API call attempt {attempt + 1}/{max_retries} for URL: {url}")
                async with self.session.get(TERABOX_API, params={"url": url}) as r:
                    data = await r.json()
                    
                    # Log full API response
                    logger.info(f"=== TERABOX API RESPONSE (Attempt {attempt + 1}) ===")
                    logger.info(f"Status Code: {r.status}")
                    logger.info(f"Full Response: {data}")
                    logger.info("=" * 50)
                    
                    if data.get("status") == "✅ Successfully":
                        logger.info(f"✅ Terabox API success for URL: {url}")
                        logger.info(f"File Name: {data.get('file_name', 'N/A')}")
                        logger.info(f"File Size: {data.get('file_size', 'N/A')}")
                        logger.info(f"Download Link: {data.get('download_link', 'N/A')[:50]}...")
                        logger.info(f"Streaming URL: {data.get('streaming_url', 'N/A')[:50]}...")
                        return {"success": True, "data": data}
                    else:
                        logger.warning(f"⚠️ Terabox API returned unsuccessful status")
                        logger.warning(f"Status: {data.get('status')}")
                        logger.warning(f"Full Response: {data}")
                        return {"success": False, "error": data.get("status")}
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ Timeout on attempt {attempt + 1}/{max_retries}")
                if attempt == max_retries - 1:
                    return {"success": False, "error": "API timeout after multiple retries"}
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"❌ Error on attempt {attempt + 1}/{max_retries}: {str(e)}")
                if attempt == max_retries - 1:
                    return {"success": False, "error": str(e)}
                await asyncio.sleep(2)
        
        return {"success": False, "error": "Unknown error after retries"}

    async def upload_file_to_hydrax(self, file_path: str):
        await self.init_session()
        logger.info(f"📤 Starting Hydrax upload for: {os.path.basename(file_path)}")
        
        with open(file_path, "rb") as f:
            form = aiohttp.FormData()
            form.add_field("file", f, filename=os.path.basename(file_path))
            async with self.session.post(HYDRAX_UPLOAD_API, data=form) as r:
                data = await r.json()
                
                # Log full Hydrax response
                logger.info(f"=== HYDRAX API RESPONSE ===")
                logger.info(f"Status Code: {r.status}")
                logger.info(f"Full Response: {data}")
                logger.info("=" * 50)
                
                if data.get("status"):
                    logger.info(f"✅ Hydrax upload success")
                    logger.info(f"URL Iframe: {data.get('urlIframe', 'N/A')}")
                    logger.info(f"Slug: {data.get('slug', 'N/A')}")
                    return {"success": True, "data": data}
                else:
                    logger.error(f"❌ Hydrax upload failed")
                    logger.error(f"Message: {data.get('msg', 'Unknown error')}")
                    return {"success": False, "error": data.get("msg")}

# ---------------- Handlers ----------------
bot_instance = TeraboxHydraxBot()

async def schedule_delete(msg, user_msg, delay=1200):
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except:
        pass
    try:
        await user_msg.delete()
    except:
        pass

async def process_single_link(url: str, link_number: int, total_links: int, context: ContextTypes.DEFAULT_TYPE, status_msg) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Process a single link in the pipeline
    Returns: (success, result_caption, error_message)
    """
    try:
        # Update status
        await status_msg.edit_text(
            f"📄 Processing link {link_number}/{total_links}...\n"
            f"⏳ Waiting for Terabox API response...",
            parse_mode=ParseMode.HTML
        )
        
        # Step 1: Download from Terabox with proper timeout handling
        logger.info(f"[Link {link_number}/{total_links}] Starting Terabox download for: {url}")
        tb = await bot_instance.download_from_terabox(url)
        
        if not tb["success"]:
            error_msg = f"❌ Link {link_number}/{total_links}: Terabox API failed\n{tb['error']}"
            logger.error(error_msg)
            return False, None, error_msg

        data = tb["data"]
        file_name = data.get("file_name", "unknown")
        
        logger.info(f"[Link {link_number}/{total_links}] Got response for file: {file_name}")

        # Check if already processed
        if is_file_processed(file_name):
            error_msg = f"⚠️ Link {link_number}/{total_links}: File <b>{file_name}</b> already processed"
            logger.info(error_msg)
            return False, None, error_msg

        # Update status
        await status_msg.edit_text(
            f"📄 Processing link {link_number}/{total_links}...\n"
            f"📦 File: {file_name}\n"
            f"⏳ Checking file size...",
            parse_mode=ParseMode.HTML
        )

        # Check file size
        file_size_str = data.get("file_size", "0")
        try:
            size_val, size_unit = file_size_str.split()
            size_val = float(size_val)
            if size_unit.lower().startswith("kb"):
                size_mb = size_val / 1024
            elif size_unit.lower().startswith("mb"):
                size_mb = size_val
            elif size_unit.lower().startswith("gb"):
                size_mb = size_val * 1024
            else:
                size_mb = 0
        except Exception:
            size_mb = 0

        if size_mb > 50:
            error_msg = f"❌ Link {link_number}/{total_links}: File <b>{file_name}</b> size {file_size_str} exceeds 50MB limit"
            logger.info(error_msg)
            return False, None, error_msg

        # Update status
        await status_msg.edit_text(
            f"📄 Processing link {link_number}/{total_links}...\n"
            f"📦 File: {file_name}\n"
            f"⬇️ Starting download...",
            parse_mode=ParseMode.HTML
        )

        # Step 2: Download file
        dl_url = data.get("streaming_url") or data.get("download_link")
        if not dl_url:
            error_msg = f"❌ Link {link_number}/{total_links}: No download link for <b>{file_name}</b>"
            logger.error(error_msg)
            return False, None, error_msg

        logger.info(f"📥 Starting Aria2 download")
        logger.info(f"Download URL: {dl_url[:100]}...")
        
        dl = await bot_instance.aria2.add_download(dl_url, {"out": file_name})
        
        if not dl["success"]:
            error_msg = f"❌ Link {link_number}/{total_links}: Download failed for <b>{file_name}</b>\n{dl['error']}"
            logger.error(error_msg)
            logger.error(f"Aria2 Error Details: {dl.get('error')}")
            return False, None, error_msg

        gid = dl["result"]
        logger.info(f"✅ Aria2 download started with GID: {gid}")
        
        # Update status
        await status_msg.edit_text(
            f"📄 Processing link {link_number}/{total_links}...\n"
            f"📦 File: {file_name}\n"
            f"⬇️ Downloading...",
            parse_mode=ParseMode.HTML
        )
        
        done = await bot_instance.aria2.wait_for_download(gid)
        if not done["success"]:
            error_msg = f"❌ Link {link_number}/{total_links}: Download error for <b>{file_name}</b>\n{done['error']}"
            logger.error(error_msg)
            logger.error(f"Aria2 Download Error: {done.get('error')}")
            return False, None, error_msg

        fpath = done["files"][0]["path"]
        logger.info(f"✅ File downloaded successfully: {fpath}")
        logger.info(f"File size on disk: {os.path.getsize(fpath) / (1024*1024):.2f} MB")

        # Update status
        await status_msg.edit_text(
            f"📄 Processing link {link_number}/{total_links}...\n"
            f"📦 File: {file_name}\n"
            f"⬆️ Uploading to Hydrax...",
            parse_mode=ParseMode.HTML
        )

        # Step 3: Upload to Hydrax
        up = await bot_instance.upload_file_to_hydrax(fpath)
        if not up["success"]:
            error_msg = f"❌ Link {link_number}/{total_links}: Upload failed for <b>{file_name}</b>\n{up['error']}"
            logger.error(error_msg)
            try:
                os.remove(fpath)
            except:
                pass
            return False, None, error_msg

        urlIframe = up["data"].get("urlIframe")
        slug = up["data"].get("slug")

        # Update status
        await status_msg.edit_text(
            f"📄 Processing link {link_number}/{total_links}...\n"
            f"📦 File: {file_name}\n"
            f"📤 Sending to channel...",
            parse_mode=ParseMode.HTML
        )

        # Step 4: Send to channel
        caption_file = (
            f"File Name : {file_name}\n"
            f"File Size : {file_size_str}\n"
            f"URLIframe : {urlIframe}\n"
            f"Slug : {slug}"
        )
        try:
            mime_type, _ = mimetypes.guess_type(fpath)
            with open(fpath, "rb") as f:
                if mime_type and mime_type.startswith("video"):
                    await context.bot.send_video(chat_id=TELEGRAM_CHANNEL_ID, video=f, caption=caption_file)
                elif mime_type and mime_type.startswith("image"):
                    await context.bot.send_photo(chat_id=TELEGRAM_CHANNEL_ID, photo=f, caption=caption_file)
                else:
                    await context.bot.send_document(chat_id=TELEGRAM_CHANNEL_ID, document=f, caption=caption_file)
        except Exception as e:
            logger.warning(f"Failed to send file to TELEGRAM_CHANNEL_ID: {e}")

        # Prepare result caption
        result_caption = (
            f"✅ {file_name} ({file_size_str})\n"
            f"🔗 {urlIframe}\n"
            f"🏷️ {slug}"
        )

        # Cleanup
        try:
            os.remove(fpath)
        except:
            pass

        save_file_name(file_name)
        
        logger.info(f"[Link {link_number}/{total_links}] Successfully processed: {file_name}")
        return True, result_caption, None

    except Exception as e:
        error_msg = f"❌ Link {link_number}/{total_links}: Unexpected error\n{str(e)}"
        logger.error(f"Unexpected error processing link {link_number}: {str(e)}")
        return False, None, error_msg

async def process_links_pipeline(urls: List[str], context: ContextTypes.DEFAULT_TYPE, update: Update):
    """
    Process multiple links in a pipeline - one after another
    Each link waits for proper API response before proceeding
    """
    m = update.effective_message
    if not m:
        return

    total_links = len(urls)
    successful_results = []
    failed_links = []

    # Create initial status message
    status_msg = await m.reply_text(
        f"📄 Starting pipeline processing...\n"
        f"📊 Total links: {total_links}",
        parse_mode=ParseMode.HTML
    )

    # Process each link sequentially
    for idx, url in enumerate(urls, 1):
        logger.info(f"Pipeline: Processing link {idx}/{total_links}")
        
        success, result_caption, error_msg = await process_single_link(
            url, idx, total_links, context, status_msg
        )

        if success and result_caption:
            successful_results.append(result_caption)
        elif error_msg:
            failed_links.append(error_msg)
        
        # Small delay between links
        if idx < total_links:
            await asyncio.sleep(1)

    # Final summary
    summary = "📦 <b>Processing Complete!</b>\n\n"
    
    if successful_results:
        summary += f"✅ <b>Successful ({len(successful_results)}):</b>\n\n"
        summary += "\n\n".join(successful_results)
    
    if failed_links:
        summary += f"\n\n❌ <b>Failed ({len(failed_links)}):</b>\n\n"
        summary += "\n\n".join(failed_links)

    # Send results to result channel if any successful
    if successful_results:
        try:
            await context.bot.copy_message(
                chat_id=RESULT_CHANNEL_ID,
                from_chat_id=m.chat.id,
                message_id=m.message_id,
                caption=summary,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to copy media to result channel: {e}")

    # Update status message with final summary
    try:
        await status_msg.edit_text(summary, parse_mode=ParseMode.HTML)
    except:
        pass

    # Delete user message
    try:
        await m.delete()
    except:
        pass

    # Schedule deletion of status message
    asyncio.create_task(schedule_delete(status_msg, None, delay=1200))

async def handle_media_with_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m = update.effective_message
    if not m:
        return

    try:
        caption = m.caption or ""
        urls = re.findall(r"https?://[^\s]+", caption)
        urls = list(dict.fromkeys(urls))  # Remove duplicates while preserving order

        if not urls:
            err_msg = await m.reply_text("❌ No links found in caption.", parse_mode=ParseMode.HTML)
            asyncio.create_task(schedule_delete(err_msg, m))
            return

        terabox_links = [u for u in urls if bot_instance.is_terabox_url(u)]

        if not terabox_links:
            err_msg = await m.reply_text(
                "❌ Not supported domain. Please send valid Terabox links.",
                parse_mode=ParseMode.HTML
            )
            asyncio.create_task(schedule_delete(err_msg, m))
            return

        logger.info(f"Starting pipeline for {len(terabox_links)} Terabox links")
        
        # Process all links in pipeline
        await process_links_pipeline(terabox_links, context, update)

    except Exception as e:
        logger.error(f"Error in handle_media_with_links: {e}")
        try:
            err_msg = await m.reply_text(f"❌ Error: {str(e)}", parse_mode=ParseMode.HTML)
            asyncio.create_task(schedule_delete(err_msg, m))
        except:
            pass

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m = update.effective_message
    if not m:
        return
    await m.reply_text("✅ Bot is running with pipeline processing!\n\n"
                      "Send media with Terabox links in caption.\n"
                      "Multiple links will be processed one by one.")

# Health check endpoint for Koyeb
async def health_check(request):
    return web.Response(text="OK", status=200)

async def webhook_handler(request):
    """Handle incoming webhook updates from Telegram"""
    try:
        data = await request.json()
        update = Update.de_json(data, application.bot)
        await application.process_update(update)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return web.Response(status=500)

# Global application instance
application = None

async def start_webhook_server():
    """Start the webhook server for Koyeb"""
    global application
    
    # Initialize database
    init_db()
    
    # Create application
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .defaults(Defaults(parse_mode=ParseMode.HTML))
        .build()
    )

    async def handle_media_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        context.application.create_task(handle_media_with_links(update, context))

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(
        filters.PHOTO | filters.VIDEO | filters.Document.ALL,
        handle_media_wrapper
    ))

    # Initialize the application
    await application.initialize()
    await application.start()

    # Set webhook
    if WEBHOOK_URL:
        webhook_path = f"/webhook/{BOT_TOKEN}"
        full_webhook_url = f"{WEBHOOK_URL}{webhook_path}"
        await application.bot.set_webhook(url=full_webhook_url)
        logger.info(f"Webhook set to: {full_webhook_url}")
    else:
        logger.warning("WEBHOOK_URL not set! Bot will not receive updates.")

    # Create web application
    app = web.Application()
    app.router.add_get("/health", health_check)
    app.router.add_get("/", health_check)  # Root path health check
    app.router.add_post(f"/webhook/{BOT_TOKEN}", webhook_handler)

    # Start web server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    logger.info(f"Bot started in webhook mode on port {PORT}")
    logger.info(f"Telegram Channel ID: {TELEGRAM_CHANNEL_ID}")
    logger.info(f"Result Channel ID: {RESULT_CHANNEL_ID}")
    logger.info(f"Health check endpoint: http://0.0.0.0:{PORT}/health")

    # Keep the server running
    await asyncio.Event().wait()

def main():
    """Main entry point"""
    # Check if running in Koyeb (has WEBHOOK_URL or PORT env var)
    if WEBHOOK_URL or os.getenv("PORT"):
        logger.info("Starting in WEBHOOK mode for Koyeb")
        asyncio.run(start_webhook_server())
    else:
        # Fallback to polling mode for local development
        logger.info("Starting in POLLING mode (local development)")
        init_db()
        
        app = (
            Application.builder()
            .token(BOT_TOKEN)
            .defaults(Defaults(parse_mode=ParseMode.HTML))
            .build()
        )

        async def handle_media_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
            context.application.create_task(handle_media_with_links(update, context))

        app.add_handler(CommandHandler("start", start))
        app.add_handler(MessageHandler(
            filters.PHOTO | filters.VIDEO | filters.Document.ALL,
            handle_media_wrapper
        ))

        logger.info("Bot started with pipeline processing enabled")
        logger.info(f"Telegram Channel ID: {TELEGRAM_CHANNEL_ID}")
        logger.info(f"Result Channel ID: {RESULT_CHANNEL_ID}")
        app.run_polling()

if __name__ == "__main__":
    main()
