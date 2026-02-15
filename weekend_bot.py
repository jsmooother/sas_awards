#!/usr/bin/env python3
import sys
print(f"[DEBUG] Starting weekend_bot.py under: {sys.executable}")

# ─── Imports ─────────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, CommandHandler, filters
import sqlite3, os

# ─── Configuration ───────────────────────────────────────────────────────────────
# Set TELEGRAM_BOT_TOKEN in environment or .env (never commit real tokens)
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
DB_PATH = os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards/sas_awards.sqlite"))

from report_config import MIN_SEATS, TRIP_DAYS_MIN, TRIP_DAYS_MAX

# ─── Handler for any “/CityName” command ─────────────────────────────────────────
async def city_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    city = update.message.text.lstrip("/").strip()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT
          inb.origin   AS origin,
          inb.date     AS inbound_date,
          outb.date    AS outbound_date,
          CASE WHEN inb.ag>0 THEN inb.ag ELSE inb.ap END   AS seats_in,
          CASE WHEN outb.ag>0 THEN outb.ag ELSE outb.ap END AS seats_out
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code
          AND inb.origin = outb.origin
        WHERE
          inb.city_name    = ? COLLATE NOCASE
          AND inb.direction = 'inbound'
          AND outb.direction= 'outbound'
          AND (inb.ag>=? OR inb.ap>=?)
          AND (outb.ag>=? OR outb.ap>=?)
          AND strftime('%w', inb.date)  IN ('6','0','1')   -- Sat/Sun/Mon
          AND strftime('%w', outb.date) IN ('3','4','5')   -- Wed/Thu/Fri
          AND (julianday(inb.date) - julianday(outb.date)) BETWEEN ? AND ?
          AND date(outb.date)
              BETWEEN date(inb.date,'-7 days') AND date(inb.date,'-1 days')
          AND date(inb.date)
              BETWEEN date('now') AND date('now','+1 year')
        ORDER BY inb.origin, inb.date, outb.date;
    """, (city, MIN_SEATS, MIN_SEATS, MIN_SEATS, MIN_SEATS, TRIP_DAYS_MIN, TRIP_DAYS_MAX))

    rows = cur.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text(
            f"No weekend-pairings found for *{city}*.",
            parse_mode="Markdown"
        )
        return

    # Build a Markdown table (origin, inbound, outbound, seats in/out)
    msg = [f"*Weekend pairs for* _{city.capitalize()}_ (min {MIN_SEATS} seats, {TRIP_DAYS_MIN}-{TRIP_DAYS_MAX} days):"]
    for origin, in_date, out_date, seats_in, seats_out in rows:
        msg.append(f"`{origin} {in_date} {out_date} {seats_in}/{seats_out}`")

    MAX_ROWS = 15
    if len(rows) > MAX_ROWS:
        msg = msg[: MAX_ROWS + 1]
        msg.append(f"_…first {MAX_ROWS} of {len(rows)}, use web for full list_")
    await update.message.reply_text("\n".join(msg), parse_mode="Markdown")


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = """*SAS Awards Bot*

*Commands:*
/help – this message
/business [City] – business seats (≥{m}) for city or top 10
/CityName – weekend pairs (e.g. /Barcelona, /Oslo)

Min {m} seats, 3–4 day trip for weekend pairs.
""".format(m=MIN_SEATS)
    await update.message.reply_text(text, parse_mode="Markdown")


async def business_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    city = " ".join(context.args).strip() if context.args else ""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if city:
        cur.execute("""
            SELECT origin, date, direction, ab
            FROM flights
            WHERE ab >= ? AND (city_name LIKE ? OR airport_code LIKE ?)
            ORDER BY date, origin
            LIMIT 20
        """, (MIN_SEATS, f"%{city}%", f"%{city}%"))
    else:
        cur.execute("""
            SELECT origin, city_name, date, direction, ab
            FROM flights
            WHERE ab >= ?
            ORDER BY ab DESC, date
            LIMIT 10
        """, (MIN_SEATS,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text(
            f"No business seats (≥{MIN_SEATS}) found" + (f" for *{city}*" if city else "."),
            parse_mode="Markdown"
        )
        return

    if city:
        msg = [f"*Business {city}* (≥{MIN_SEATS} seats):"]
        for origin, date, direction, ab in rows:
            msg.append(f"`{origin} {date} {direction[:3]} {ab}B`")
    else:
        msg = [f"*Top business* (≥{MIN_SEATS} seats):"]
        for origin, city_name, date, direction, ab in rows:
            msg.append(f"`{origin} {city_name} {date} {direction[:3]} {ab}B`")

    text = "\n".join(msg)
    if len(text) > 4000:
        text = "\n".join(msg[:12]) + "\n_…(truncated, use web for full list)_"
    await update.message.reply_text(text, parse_mode="Markdown")


# ─── Main ───────────────────────────────────────────────────────────────────────
def main():
    if not TOKEN:
        print("ERROR: Set TELEGRAM_BOT_TOKEN in environment. See README.md")
        sys.exit(1)
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("help", help_handler))
    app.add_handler(CommandHandler("business", business_handler))
    app.add_handler(
        MessageHandler(
            filters.Regex(r"^/[A-Za-zÅÄÖåäö\-]+$"),
            city_handler
        )
    )

    print("[DEBUG] Entering polling loop with drop_pending_updates=True")
    print("Bot running; /help, /business, /CityName")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
