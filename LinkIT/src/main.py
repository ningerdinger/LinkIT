from modules.modules import generate_customer_data, generate_transaction, save_to_csv

if __name__ == "__main__":
    CUSTOMER_NUM = 200
    TRANSACTION_NUM = 2000

    customer_data = generate_customer_data(CUSTOMER_NUM)
    creditcard_transaction, product_transaction = generate_transaction(
        TRANSACTION_NUM, customer_data=customer_data
    )
    data_files = {
        "customer_data.csv": customer_data,
        "creditcard_transaction.csv": creditcard_transaction,
        "product_transaction.csv": product_transaction,
    }

    for filename, dataset in data_files.items():
        save_to_csv(dataset, filename)
