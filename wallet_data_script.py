import requests
import csv
import time


# Function to get transaction data for an address
def get_transaction_data(bitcoin_address):
    url = f"https://blockchain.info/rawaddr/{bitcoin_address}?limit=1"
    response = requests.get(url)
    return response.json()


def process_csv(input_file, output_file, log_file):
    with open(input_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)

        with open(output_file, 'a', newline='') as output_csv, open(log_file, 'w') as log:
            fieldnames = ['address', 'hash160', 'n_tx', 'n_unredeemed', 'total_received', 'total_sent', 'final_balance']


            writer = csv.DictWriter(output_csv, fieldnames=fieldnames)

            for row in reader:
                bitcoin_address = row[4]  # Assuming the address is in the 5th column
                log.write(f"Processing address: {bitcoin_address}\n")

                try:
                    transaction_data = get_transaction_data(bitcoin_address)
                    address_data = {
                        'address': transaction_data['address'],
                        'hash160': transaction_data['hash160'],
                        'n_tx': transaction_data['n_tx'],
                        'n_unredeemed': transaction_data['n_unredeemed'],
                        'total_received': transaction_data['total_received'],
                        'total_sent': transaction_data['total_sent'],
                        'final_balance': transaction_data['final_balance']

                    }

                    writer.writerow(address_data)
                    log.write(f"Successfully processed address: {bitcoin_address}\n")
                except Exception as e:
                    log.write(f"Error processing address {bitcoin_address}: {str(e)}\n")

                print(f"Processed address: {bitcoin_address}")
                time.sleep(10)  # Add a small delay to avoid API limits


if __name__ == "__main__":
    input_file = "chainabuse_cleaned.csv"
    output_file = "wallet_data.csv"
    log_file = "processing_log.txt"

    process_csv(input_file, output_file, log_file)