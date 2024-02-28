import decimal
import json
import gspread
import requests
import threading
import csv
import os
import sys
from decimal import Decimal
from datetime import datetime

plans = ["Documentos Assinados - Plano Custom", "Documentos Assinados - Plano Custom", "Plano Ilimitado"]
gc = gspread.service_account()
vindi_token = sys.argv[4]

print("Starting process ...")
print("sheet_name " + sys.argv[2])
print("sheet_id " + sys.argv[1])
print("file_name " + sys.argv[0])

sheet = gc.open_by_key(sys.argv[1])
worksheet = sheet.worksheet(sys.argv[2])
csv_file_path = sys.argv[0]
num_threads = 1


def process_all():
    start_time = datetime.now()

    list_of_lists = worksheet.get_all_values()
    writer = csv.writer(open(csv_file_path, 'w'), delimiter='|')
    process_in_parallel(list_of_lists, writer)
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))


def process_row(row_index, row, writer):
    subscription_id = row[0]
    vindi_response = get_vindi_info(subscription_id)
    subscription = vindi_response['subscription']
    customer_name = subscription['customer']['name']
    product_items = subscription['product_items']
    for product_item in product_items:
        plan = product_item['product']['name']
        if plan in plans:
            payload = get_new_price_payload(product_item)
            response = put_vindi_product_item(payload)



def get_new_price_payload(product_item):
    result = {}
    plan = product_item['product']['name']
    if plan in plans:
        payload = {'id': product_item['id'], 'status': product_item['status'], 'cycles': product_item['cycles'],
                   'quantity': product_item['quantity']}

        pricing_schema = product_item['pricing_schema']
        price = pricing_schema['price'] * 2
        pricing_ranges = []

        for pricing_range in pricing_schema['pricing_ranges']:
            new_price = pricing_range['price'] * 2
            pricing_range['price'] = new_price
            pricing_ranges.append(pricing_range)

        payload['pricing_schema'] = {
            "id": pricing_schema['id'],
            "short_format": pricing_schema['short_format'],
            "price": price,
            "pricing_ranges": pricing_ranges
        }

    return result


def process_batch(list_of_lists, start_index, end_index, writer):
    for row_index, row in enumerate(list_of_lists[start_index:end_index], start=start_index):
        process_row(row_index, row, writer)


def process_in_parallel(list_of_lists, writer):
    batch_size = len(list_of_lists) // num_threads

    threads = []
    for i in range(num_threads):
        start_index = i * batch_size + 1
        end_index = start_index + batch_size if i < num_threads - 1 else len(list_of_lists)
        thread = threading.Thread(target=process_batch, args=(list_of_lists, start_index, end_index, writer))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def map_vindi_json(subscription_id, vindi_response):
    data_row = []
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
        price_schema = item['pricing_schema']
        price_amount = decimal.Decimal(price_schema['price'])
        pricing_schema_id = price_schema['id']
        short_format = str(price_schema['short_format'])
        price = str(price_schema['price'])
        schema_type = price_schema['schema_type']
        pricing_ranges = price_schema['pricing_ranges']
        price_range = ""
        overage_price = ""
        for pr in pricing_ranges:
            price_range = str(pr['price'])
            overage_price = str(pr['overage_price'])

        data_row.append(
            [subscription_id, status, customer_name, next_billing, email, item_id, product_id, product_name,
             payment_method, period_start, period_end, short_format, price, pricing_schema_id, schema_type,
             price_range, overage_price])
    return data_row


def get_vindi_info(subscription_id):
    private_url = "https://app.vindi.com.br/api/v1/subscriptions/" + str(subscription_id)
    response = requests.get(private_url, headers={'authorization': 'Basic %s' % vindi_token})
    if response.status_code != 200:
        print(f"Error: {response.status_code} para sub_id {subscription_id}")
        return None
    data = response.content
    return json.loads(data)


def put_vindi_product_item(product_item):
    url = "https://app.vindi.com.br/api/v1/product_items/" + str(product_item.id)
    response = requests.put(url, payload=product_item, headers={'authorization': 'Basic %s' % vindi_token})
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


def get_miro_shape_template(subscription_id, from_amount, to_amount, x, y):
    template = {
          "type": "shape",
          "data": {
            "content": "<p><strong>" + str(subscription_id) + "</strong></p><p>de: " + str(from_amount) + "</p><p>para: " + str(to_amount) + "</p><p><br /></p><p><br /></p>",
            "shape": "rectangle"
          },
          "style": {
            "fillColor": "#ffffff",
            "fillOpacity": "0.0",
            "fontFamily": "open_sans",
            "fontSize": "10",
            "borderColor": "#1a1a1a",
            "borderWidth": "1.0",
            "borderOpacity": "1.0",
            "borderStyle": "normal",
            "textAlign": "left",
            "textAlignVertical": "middle",
            "color": "#1a1a1a"
          },
          "geometry": {
            "width": 132.35814562182742,
            "height": 51.64189403553296
          },
          "parent": {
            "id": "3458764580605879383"
          },
          "position": {
            "x": x,
            "y": y
          }
        }
    return template


if __name__ == '__main__':
    process_all()
