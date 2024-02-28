import json
import gspread
import requests
from datetime import datetime
import threading
import csv


token = "VU81UXhzNVpCVDNIWEdjOG93NVhNNGlGOUhqR2VVazNDTE5SS0R4WGdJTTp1bmRlZmluZWQ="
headers = {'authorization': 'Basic %s' % token}
plans = ["Plano Fluxia", "Plano Custom", "Plano Ilimitado", "Documentos Assinados - Plano Custom", "Documentos Assinados - Plano Fluxia", "Documentos Assinados - Plano Ilimitado"]
gc = gspread.service_account()
sheet = gc.open_by_key('1GpQm7DofGPS0fMiaZbmVL7ww2RrLoaFU7TMFpkrT3SM')
worksheet = sheet.worksheet("Teste")
csv_file_path = '/Users/odairbonin/Documents/clicksign/vindi-export-list-subsctriptions.csv'
num_threads = 1
batch_size = 100


def open_spreadsheet():
    rows = worksheet.row_count
    data = []
    start_time = datetime.now()
    for row_index in range(2, rows + 1):
        subscription_id = worksheet.cell(row_index, 1).value
        vindi_response = get_vindi_info(subscription_id)
        json_response = map_vindi_json(subscription_id, vindi_response)
        data.append([json_response['customer_name'], json_response['plan_name'], json_response['price']])
        if row_index % batch_size == 0:
            print(row_index)
            sheet_range = "B" + str((row_index - batch_size) + 1) + ":D" + str(row_index + 1)
            worksheet.update(sheet_range, data)
            data.clear()
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))


def map_vindi_json(subscription_id, vindi_response):
    subscription = vindi_response['subscription']
    status = subscription['status']
    next_billing = convert_date(subscription['next_billing_at'])
    current_period = subscription['current_period']
    period_start = convert_date(current_period['start_at'])
    period_end = convert_date(current_period['end_at'])
    product_items = subscription['product_items']
    payment_method = subscription['payment_method']['name']
    email = subscription['customer']['email']
    customer_name = subscription['customer']['name']
    for item in product_items:
        item_id = item['id']
        product_name = item['product']['name']
        product_id = item['product']['id']
        if product_name in plans:
            price_schema = item['pricing_schema']
            pricing_schema_id = price_schema['id']
            plan_name = item['product']['name']
            short_format = str(price_schema['short_format'])
            price_amount = price_schema['price']
            price = str(price_schema['price'])
            schema_type = price_schema['schema_type']
            pricing_ranges = price_schema['pricing_ranges']
            price_range = ""
            overage_price = ""
            for pr in pricing_ranges:
                price_range = str(pr['price'])
                overage_price = str(pr['overage_price'])

            json_response = {
                "subscription_id": subscription_id,
                "status": status,
                "customer_name": customer_name,
                "next_billing": next_billing,
                "email": email,
                "item_id": item_id,
                "product_id": product_id,
                "plan_name": plan_name,
                "product_name": product_name,
                "payment_method": payment_method,
                "period_start": period_start,
                "period_end": period_end,
                "short_format": short_format,
                "price": price,
                "pricing_schema_id": pricing_schema_id,
                "schema_type": schema_type,
                "price_range": price_range,
                "overage_price": overage_price
            }
    return json_response


def get_vindi_info(subscription_id):
    private_url = "https://app.vindi.com.br/api/v1/subscriptions/" + str(subscription_id)
    response = requests.get(private_url, headers=headers)
    if response.status_code != 200:
        print(f"Error: {response.status_code} para sub_id {subscription_id}")
        return None
    data = response.content
    return json.loads(data)


def convert_date(date_str):
    date_obj = ""
    if date_str is not None:
        index = date_str.index('T')
        if index > 0:
            date_obj = date_str[:index]
    return date_obj


if __name__ == '__main__':
    open_spreadsheet()
