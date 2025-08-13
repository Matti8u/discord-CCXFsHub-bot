import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os
import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import timezone
from dotenv import load_dotenv
from keep_alive import keep_alive
import discord
import asyncio
from matplotlib import rcParams
from matplotlib.font_manager import FontProperties
import json
from upstash_redis import Redis


keep_alive()
load_dotenv()

scheduler_started = False
redis = Redis.from_env()

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

def find_dict_index(lst, target):
    target_id = target.get("id")
    for i, d in enumerate(lst):
        if d.get("id") == target_id: 
            return i
    return None


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
                "days_to_pass_str": None,
                "change": None
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

    for airline in all_airline_data:
        current_index = find_dict_index(all_airline_data, {"id": airline["id"]})
        if current_index is not None:
            current_index = int(current_index)
            
        old_index = redis.get(airline["id"])

        if old_index is not None and current_index is not None:
            if isinstance(old_index, bytes):
                old_index = old_index.decode()
            old_index = int(old_index) 
            if current_index < old_index:
                airline["change"] = 1   # moved up
            elif current_index > old_index:
                airline["change"] = -1  # moved down
            else:
                airline["change"] = 0   # same

        if current_index is not None:
            redis.set(airline["id"], current_index)



    print (json.dumps(all_airline_data, indent=2), flush=True)
    return all_airline_data





def save_airline_table_image(airline_data, filename="airline_table.png"):
    df = pd.DataFrame(airline_data)

    # Select and rename columns
    df = df[["name", "abbr", "owner", "total_pilots", "total_flights", "flights_last_30_days", "days_to_pass_str", "change"]].copy()
    df.columns = ["Name", "ID", "CEO", "Pilots", "Total Flights", "Flights Last 30 Days", "Estimated Time Until CCX Passes", "Change"]

    df["Total Flights"] = df["Total Flights"].apply(lambda x: f"{x:,}" if pd.notnull(x) else "N/A")
    df["Flights Last 30 Days"] = df["Flights Last 30 Days"].apply(lambda x: f"{x:,}" if pd.notnull(x) else "N/A")
    df["Pilots"] = df["Pilots"].apply(lambda x: f"{x:,}" if pd.notnull(x) else "N/A")

    # Map 'change' numeric values to arrows
    def change_to_arrow(c):
        if c == 1:
            return "↑"
        elif c == -1:
            return "↓"
        else:
            return ""
    df["Change"] = df["Change"].apply(change_to_arrow)

    # Settings
    font_size = 10
    padding = 0.4  # inches
    rcParams['font.family'] = 'DejaVu Sans'
    font_props = FontProperties(size=font_size)


    fig, ax = plt.subplots(figsize=(12, 2)) 
    renderer = fig.canvas.get_renderer()

    def text_width(text):
        text_obj = ax.text(0, 0, str(text), fontproperties=font_props)
        bbox = text_obj.get_window_extent(renderer=renderer)
        text_obj.remove()
        return bbox.width

    # Calculate column widths (pixels to inches)
    col_widths = []
    for i, col in enumerate(df.columns):
        max_width = max([text_width(col)] + [text_width(val) for val in df[col]])
        col_padding = padding * 0.3 if col == "Change" else padding
        col_widths.append(max_width / 96 + col_padding)  # 96 dpi = 1 inch

    plt.close(fig)  

    total_width = sum(col_widths) + 0.5
    total_height = 7

    relative_widths = [w / total_width for w in col_widths]


    fig, ax = plt.subplots(figsize=(total_width, total_height))
    ax.axis("off")

    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        colWidths=relative_widths,
        cellLoc='center',
        loc='center'
    )

    table.auto_set_font_size(False)
    table.set_fontsize(font_size)
    table.scale(1, 1.5)

    # Color header and rows
    for (row, col), cell in table.get_celld().items():
        if row == 0:
            cell.set_facecolor("#4a7ebb")
            cell.set_text_props(weight='bold', color='white')
        else:
            airline_id = df.iloc[row - 1]["ID"]
            if airline_id.lower().startswith("ccx"):
                cell.set_facecolor("#ffe599")
            else:
                cell.set_facecolor("#f8f8f8" if row % 2 == 0 else "white")

        cell.set_edgecolor("#cccccc")

        # Set arrow color and size in the "Change" column
        if row > 0 and col == len(df.columns) - 1:
            arrow = df.iloc[row - 1]["Change"]
            if arrow == "↑":
                cell.get_text().set_color("green")
                cell.get_text().set_fontsize(font_size * 2)
            elif arrow == "↓":
                cell.get_text().set_color("red")
                cell.get_text().set_fontsize(font_size * 2)
            else:
                cell.get_text().set_color("black")
                cell.get_text().set_fontsize(font_size)

    plt.tight_layout()
    plt.savefig(filename, bbox_inches="tight", pad_inches=0.05, dpi=150)
    plt.close()



discord_token = os.getenv("DISCORD_TOKEN")
try:
    channel_id = int(os.getenv("CHANNEL_ID"))
    USER_ID = int(os.getenv("USER_ID"))
except (TypeError, ValueError):
    raise ValueError("CHANNEL_ID or USER_ID environment variable not set or invalid")


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
    scheduler.add_job(generate_and_send, "cron", hour=4, minute=6, max_instances=1)
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
