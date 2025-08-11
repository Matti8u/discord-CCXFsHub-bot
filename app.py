import pandas as pd
import matplotlib.pyplot as plt
import os
import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timezone
from dotenv import load_dotenv
from keep_alive import keep_alive
import discord
import asyncio
from matplotlib import rcParams
import json

keep_alive()
load_dotenv()

scheduler_started = False

def days_to_ymd(days):
    if days is None or days < 0:
        return "N/A"

    years = int(days // 365)
    days %= 365
    months = int(days // 30)
    days %= 30
    days = int(round(days, 0))

    parts = []
    if years > 0:
        parts.append(f"{years} year{'s' if years != 1 else ''}")
    if months > 0:
        parts.append(f"{months} month{'s' if months != 1 else ''}")
    if days > 0 or not parts:
        parts.append(f"{days} day{'s' if days != 1 else ''}")

    return ", ".join(parts)


def fetch_airline_data():

    fshub_token = os.getenv("FSHUB_TOKEN")

    all_airline_data = []

    headers = {
        "Content-Type": "application/json",
        "X-Pilot-Token": fshub_token
    }

    airline_ids = [2145, 2216, 3822, 4817, 3427, 1876, 1986, 1850, 2691, 1341, 3681, 2713, 2599, 2090, 2639, 1918, 3972, 2397, 6076]
    

    for airline_id in airline_ids:
        try:
            # 1. Fetch general airline info
            airline_resp = requests.get(f"https://fshub.io/api/v3/airline/{airline_id}", headers=headers)
            airline_resp.raise_for_status()
            airline = airline_resp.json()["data"]

            # 2. Fetch stats for this airline
            stats_resp = requests.get(f"https://fshub.io/api/v3/airline/{airline_id}/stats", headers=headers)
            stats_resp.raise_for_status()
            stats = stats_resp.json()["data"]



            # 3. Combine relevant data
            info = {
                "id": airline["id"],
                "name": airline["name"],
                "abbr": airline["abbr"],
                "owner": airline["owner"]["name"],
                "total_pilots": stats["total_pilots"],
                "total_flights": stats.get("all_time", {}).get("total_flights"),
                "flights_last_30_days": stats.get("month", {}).get("total_flights"),
                "days_to_pass": None,
                "days_to_pass_str": None
            }
            all_airline_data.append(info)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for airline {airline_id}: {e}", flush=True)
        except KeyError as e:
            print(f"Missing expected field for airline {airline_id}: {e}", flush=True)


    CCXTotalFlights = None
    CCXMonthFlights = None

    for airline in all_airline_data:
        if airline["id"] == 6076:
            CCXTotalFlights = airline["total_flights"]
            CCXMonthFlights = airline["flights_last_30_days"]
            airline["days_to_pass_str"] = "N/A"
            break

    if CCXTotalFlights is not None and CCXMonthFlights is not None and CCXMonthFlights > 0:
        CCXRate = CCXMonthFlights / 30
    else: 
        print("Error: Missing or invalid CCX stats", flush=True)
        CCXRate = None

    if CCXRate:
        for airline in all_airline_data:
            if airline["id"] != 6076:
                airline_rate = airline["flights_last_30_days"] / 30 if airline["flights_last_30_days"] else 0
                if airline_rate < CCXRate and airline["total_flights"] > CCXTotalFlights:
                    days = (airline["total_flights"] - CCXTotalFlights) / (CCXRate - airline_rate)
                    airline["days_to_pass"] = round(days, 0)
                    airline["days_to_pass_str"] = days_to_ymd(days)
                else:
                    airline["days_to_pass_str"] = "N/A"



    all_airline_data.sort(key=lambda a: a["total_flights"], reverse=True)

    print (json.dumps(all_airline_data, indent=2), flush=True)
    return all_airline_data





def save_airline_table_image(airline_data, filename="airline_table.png"):
    df = pd.DataFrame(airline_data)
    df = df[["name", "abbr", "owner", "total_pilots", "total_flights", "flights_last_30_days", "days_to_pass_str"]]
    df.columns = ["Name", "ID", "CEO", "Pilots", "Total Flights", "Flights Last 30 Days", "Estimated Time Until CCX Passes"]

    df["Total Flights"] = df["Total Flights"].apply(lambda x: f"{x:,}" if pd.notnull(x) else "N/A")
    df["Flights Last 30 Days"] = df["Flights Last 30 Days"].apply(lambda x: f"{x:,}" if pd.notnull(x) else "N/A")
    df["Pilots"] = df["Pilots"].apply(lambda x: f"{x:,}" if pd.notnull(x) else "N/A")


    
    # Settings
    font_size = 10
    row_height = 0.6  # inches
    padding = 0.4     # extra space per column (inches)
    rcParams['font.family'] = 'DejaVu Sans'  # clean, modern font

    # Measure column widths based on text
    import matplotlib.pyplot as plt
    from matplotlib.font_manager import FontProperties

    font_props = FontProperties(size=font_size)
    renderer = plt.figure().canvas.get_renderer()

    def text_width(text):
        text_obj = plt.text(0, 0, str(text), fontproperties=font_props)
        bbox = text_obj.get_window_extent(renderer=renderer)
        text_obj.remove()
        return bbox.width

    col_widths = []
    for col in df.columns:
        max_width = max(
            [text_width(col)] + [text_width(val) for val in df[col]]
        )
        # Convert from pixels to inches (approx 96 dpi)
        col_widths.append(max_width / 96 + padding)

    total_width = sum(col_widths) + 0.5
    total_height = 7

    fig, ax = plt.subplots(figsize=(total_width, total_height))
    ax.axis("off")

    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        colWidths=[w / total_width for w in col_widths],  # relative widths
        cellLoc='center',
        loc='center'
    )

    # Style
    table.auto_set_font_size(False)
    table.set_fontsize(font_size)
    table.scale(1, 1.5)

    # Style header, alternating rows, and highlight CCX
    for (row, col), cell in table.get_celld().items():
        if row == 0:
            cell.set_facecolor("#4a7ebb")  # header color
            cell.set_text_props(weight='bold', color='white')
        else:
            airline_name = df.iloc[row-1]["ID"]  
            if airline_name.lower().startswith("ccx"): 
                cell.set_facecolor("#ffe599")  
            else:
                cell.set_facecolor("#f8f8f8" if row % 2 == 0 else "white")
        cell.set_edgecolor("#cccccc")

    plt.tight_layout()
    plt.savefig(filename, bbox_inches="tight", pad_inches=0.05, dpi=150)
    plt.close()








discord_token = os.getenv("DISCORD_TOKEN")
channel_id = int(os.getenv("CHANNEL_ID"))
USER_ID = int(os.getenv("USER_ID"))

if discord_token is None:
    raise ValueError("DISCORD_TOKEN environment variable not set.")


intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

async def send_image():
    await client.wait_until_ready()
    channel = client.get_channel(channel_id)

    if channel:
        await channel.send(file=discord.File("airline_table.png"))


async def generate_and_send():
    try:
        print("Running update...", flush=True)
        data = fetch_airline_data()
        save_airline_table_image(data)
        await send_image()
        print("Update complete.", flush=True)
    except Exception as e:
        print(f"Error in generate_and_send: {e}", flush=True)



def schedule_updates():
    global scheduler_started
    if scheduler_started:
        return
    scheduler_started = True

    scheduler = AsyncIOScheduler(timezone=timezone.utc)
    scheduler.add_job(generate_and_send, "cron", hour=0, minute=0, max_instances=1)
    scheduler.start()


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.strip().lower() == "!ccxbottest":
        if message.author.id == USER_ID:
            await message.channel.send("Test started!")
            print("Manual Test started!", flush=True)
            await generate_and_send()


@client.event
async def on_ready():
    print("Discord bot ready.", flush=True)
    schedule_updates()



client.run(discord_token)
