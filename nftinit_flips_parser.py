"""
Script for parsing NFTinit flip stats
"""

import json
from time import sleep
from math import floor
from datetime import datetime

import requests
import pandas as pd


def get_nftinit_flip_stats(period_days: int, max_tries: int):
    """
    Parses all NFTinit flip stats
    :param period_days: period of data, days
    :param max_tries: number of tries
    :return:
    """
    data = []
    tries_count = 0

    i = 1
    while True:
        try:
            url = f'https://api.nftinit.io/api/get_profit_hot_map/?format=json&period={period_days}&page={i}' \
                  f'&order_by=None&pay_type=None'

            response = requests.get(url)
            json_response = json.loads(response.text)

            for flip in json_response['data']:
                data.append(flip)

            if i == json_response['page_count']:
                return {
                    "success": True,
                    "data": data
                }

            print(f'Parsed {i}/{json_response["page_count"]} pages')
            i += 1
            tries_count = 0

        except Exception as e:
            print(e)
            tries_count += 1

            if tries_count == max_tries:
                return {
                    "success": False,
                    "error": e
                }

        finally:
            sleep(0.25)


def process_flips_data(flips_data: list):
    """
    Process flips data for further dataframe building
    :param flips_data: list of nftinit flips dictionaries
    :return: processed data
    """
    processed_data = []

    for flip in flips_data:

        rank_percent = None
        if "rank" in flip and 'supply' in flip:
            if isinstance(flip["rank"], int) and isinstance(flip["supply"], int) and flip["supply"] > 0:
                rank_percent = floor((flip["rank"] / flip["supply"] * 100))

        processed_data.append({
            "collection": flip["collection"],
            "slug": flip["slug"],
            "address": flip["address"],
            "floor_price": flip["floor_price"],
            "supply": flip["supply"],
            "token_id": flip["token_id"],
            "token_name": flip["token_name"],
            "link": flip["permalink"],
            "image": flip["image"],
            "rank": flip["rank"],
            "rank_percent": rank_percent,
            "date": flip["event_date"],
            "buy_price_eth": flip["buy"]["price_eth"],
            "buy_price_usd": flip["buy"]["price_usd"],
            "buy_date": flip["buy"]["date"],
            "buy_pay_type": flip["buy"]["pay_type"],
            "sell_price_eth": flip["sell"]["price_eth"],
            "sell_price_usd": flip["sell"]["price_usd"],
            "sell_date": flip["sell"]["date"],
            "sell_pay_type": flip["sell"]["pay_type"],
            "profit_eth": flip["profit_eth"],
            "profit_usd": flip["profit_usd"],
            "hold_duration": flip["hold_duration"],
            "percentage": flip["percentage"]
        })

    return processed_data


if __name__ == "__main__":
    flips_period = 1  # 1, 7, 15, 30
    now = datetime.now()

    all_flips = get_nftinit_flip_stats(flips_period, 10)
    if all_flips['success']:
        print('Finished parsing')
        processed_flips_data = process_flips_data(all_flips['data'])
        flips_df = pd.DataFrame(processed_flips_data)
        flips_df.to_excel(f"data/"
                          f"{now.strftime('%Y')}.{now.strftime('%m')}.{now.strftime('%d')}-{flips_period}.xlsx")
        print('All data saved to excel file')
    else:
        print(f'Failed to parse data, reason: {all_flips["error"]}')
