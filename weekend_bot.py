#!/usr/bin/env python3
# ─── Monkey-patch tzlocal & APScheduler ─────────────────────────────────────────
import pytz, tzlocal, apscheduler.util

# Force tzlocal to return a pytz timezone
tzlocal.get_localzone = lambda: pytz.timezone("Europe/Stockholm")

# Force APScheduler's astimezone to accept pytz tzinfos only
apscheduler.util.astimezone = (
    lambda tz: tz if isinstance(tz, pytz.BaseTzInfo)
               else pytz.timezone("Europe/Stockholm")
)

# ─── Debug startup prints ───────────────────────────────────────────────────────
import sys
print(f"[DEBUG] Starting weekend_bot.py under: {sys.executable}")

# ─── Imports ─────────────────────────────────────────────────────────────────────
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
import sqlite3, os

# ─── Configuration ───────────────────────────────────────────────────────────────
# Set TELEGRAM_BOT_TOKEN in environment or .env (never commit real tokens)
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
DB_PATH = os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards/sas_awards.sqlite"))

# ─── Handler for any “/CityName” command ─────────────────────────────────────────
async def city_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    city = update.message.text.lstrip("/").strip()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT
          inb.date     AS inbound_date,
          outb.date    AS outbound_date,
          CASE WHEN inb.ag>0 THEN inb.ag ELSE inb.ap END   AS seats_in,
          CASE WHEN outb.ag>0 THEN outb.ag ELSE outb.ap END AS seats_out
        FROM flights AS inb
        JOIN flights AS outb USING (airport_code)
        WHERE
          inb.city_name    = ? COLLATE NOCASE
          AND inb.direction = 'inbound'
          AND outb.direction= 'outbound'
          AND (inb.ag>0 OR inb.ap>0)
          AND (outb.ag>0 OR outb.ap>0)
          AND strftime('%w', inb.date)  IN ('6','0','1')   -- Sat/Sun/Mon
          AND strftime('%w', outb.date) IN ('3','4','5')   -- Wed/Thu/Fri
          AND date(outb.date)
              BETWEEN date(inb.date,'-7 days') AND date(inb.date,'-1 days')
          AND date(inb.date)
              BETWEEN date('now') AND date('now','+1 year')
        ORDER BY inb.date, outb.date;
    """, (city,))

    rows = cur.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text(
            f"No weekend-pairings found for *{city}*.",
            parse_mode="Markdown"
        )
        return

    # Build a Markdown table
    msg = [
        f"*Weekend pairs for* _{city.capitalize()}_:",
        "`Inbound     Outbound    Seats(in/out)`"
    ]
    for in_date, out_date, seats_in, seats_out in rows:
        msg.append(f"`{in_date}   {out_date}    {seats_in}/{seats_out}`")

    await update.message.reply_text("\n".join(msg), parse_mode="Markdown")

# ─── Main ───────────────────────────────────────────────────────────────────────
def main():
    if not TOKEN:
        print("ERROR: Set TELEGRAM_BOT_TOKEN in environment. See README.md")
        sys.exit(1)
    app = ApplicationBuilder().token(TOKEN).build()
    # Accept anything of form `/CityName`
    app.add_handler(
        MessageHandler(
            filters.Regex(r"^/[A-Za-zÅÄÖåäö\-]+$"),
            city_handler
        )
    )

    print("[DEBUG] Entering polling loop with drop_pending_updates=True")
    print("Bot running; awaiting /CityName …")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
