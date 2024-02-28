import json
import gspread
import requests
import threading
import csv

token = "VU81UXhzNVpCVDNIWEdjOG93NVhNNGlGOUhqR2VVazNDTE5SS0R4WGdJTTp1bmRlZmluZWQ="
headers = {'authorization': 'Basic %s' % token}
plans = ["Plano Fluxia", "Plano Custom", "Plano Ilimitado"]
gc = gspread.service_account()
sheet = gc.open_by_key('1GpQm7DofGPS0fMiaZbmVL7ww2RrLoaFU7TMFpkrT3SM')
worksheet = sheet.worksheet("Painel de controle")
csv_bills_file_path = '/Users/odairbonin/Documents/clicksign/vindi-export-bills.csv'
csv_charges_file_path = '/Users/odairbonin/Documents/clicksign/vindi-export-charges.csv'
num_threads = 3
last_index = 1


def open_spreadsheet():
    # worksheet.batch_clear(["E2:K"])
    list_of_lists = worksheet.get_all_values()[last_index:]
    writer_bills = csv.writer(open(csv_bills_file_path, 'w'), delimiter=';')
    writer_charges = csv.writer(open(csv_charges_file_path, 'w'), delimiter=';')
#    for row in list_of_lists:
#        subscription_id = row[0]
#        vindi_response = get_vindi_info(subscription_id)
#        data_row = map_vindi_json(subscription_id, vindi_response)
#        writer.writerow(data_row)
    process_in_parallel(list_of_lists, writer_bills, writer_charges)


def process_row(row, writer_bills, writer_charges):
    subscription_id = row[0]
    vindi_response = get_vindi_info(subscription_id)
    json_return = map_vindi_json(subscription_id, vindi_response)
    rows = json_return.get('bills')
    writer_bills.writerows(rows)
    rows = json_return.get('charges')
    writer_charges.writerows(rows)

    print(str(subscription_id) + " - OK")


def process_batch(list_of_lists, start_index, end_index, writer_bills, writer_charges):
    for row_index, row in enumerate(list_of_lists[start_index:end_index], start=start_index):
        process_row(row, writer_bills, writer_charges)


def process_in_parallel(list_of_lists, writer_bills, writer_charges):
    batch_size = len(list_of_lists) // num_threads

    threads = []
    for i in range(num_threads):
        start_index = i * batch_size + 2
        end_index = start_index + batch_size if i < num_threads - 1 else len(list_of_lists)
        thread = threading.Thread(target=process_batch, args=(list_of_lists, start_index, end_index, writer_bills, writer_charges))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def map_vindi_json(subscription_id, vindi_response):
    datarow_bills = []
    datarow_charges = []
    bills = vindi_response['bills']
    for bill in bills:
        bill_id = bill['id']
        bill_amount = bill['amount']
        bill_status = bill['status']
        bill_due_at = convert_date(bill['due_at'])
        bill_created_at = convert_date(bill['created_at'])
        bill_url = bill['url']
        datarow_bills.append([subscription_id, bill_id, bill_amount, bill_status, bill_due_at, bill_created_at, bill_url])
        charges = bill['charges']
        for charge in charges:
            charge_id = charge['id']
            charge_amount = charge['amount']
            charge_status = charge['status']
            charge_paid_at = convert_date(charge['paid_at'])
            charge_url = charge['print_url']
            charge_created_at = convert_date(charge['created_at'])
            charge_attempt = charge['attempt_count']
            last_barcode = ""
            last_charge_id = ""
            last_gateway_message = ""
            last_created_at = ""
            last = charge['last_transaction']
            if last is not None:
                last_charge_id = last['id']
                last_gateway_message = last['gateway_message']
                last_created_at = convert_date(last['created_at'])
                gateway_response_fields = last['gateway_response_fields']

                if gateway_response_fields is not None:
                    last_barcode = gateway_response_fields.get('typeable_barcode')

            datarow_charges.append([subscription_id, charge_id, charge_amount, charge_status, charge_paid_at, charge_url, charge_created_at, charge_attempt, last_charge_id, last_gateway_message, last_created_at, last_barcode])

    return {"bills": datarow_bills, "charges": datarow_charges}


def get_vindi_info(subscription_id):
    #https://app.vindi.com.br/api/v1/bills?page=1&per_page=25&query=subscription_id%3D18126813%20and%20created_at%3E%3D2024-01-01&sort_by=created_at&sort_order=desc
    private_url = "https://app.vindi.com.br/api/v1/bills?per_page=25&query=subscription_id=" + str(subscription_id) + " and created_at>=2024-01-01&sort_by=created_at&sort_order=desc"
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
