"""_summary_

Returns:
    _type_: _description_
"""

import csv
import datetime
import logging
import uuid
from random import randint, choices, choice, uniform
from faker import Faker


fake = Faker()


def random_date_2022():
    """Generates a random date in 2022"""
    start_date = datetime.datetime(2022, 1, 1)
    end_date = datetime.datetime(2022, 12, 31)
    delta = end_date - start_date
    random_days = randint(0, delta.days)
    random_date = start_date + datetime.timedelta(days=random_days)

    # Generate a random time
    random_seconds = randint(0, 86399)  # 86399 seconds in a day
    random_time = datetime.timedelta(seconds=random_seconds)

    random_date_time = random_date + random_time
    return random_date_time.strftime("%Y-%m-%d %H:%M:%S")


def save_to_csv(data: list, filename: str):
    """
    saving data as a csv file

    Args:
        data (list): The data in a list with nested dictionary
        filename (str): the filename that needs to be given in a string with .csv affix
    """
    headers = data[0].keys()
    with open(filename, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)


def generate_birth_date() -> datetime.date:
    """Generates a random birth date for individuals aged between 18 and 100."""
    today = datetime.date.today()
    start_date = today.replace(year=today.year - 18)
    end_date = today.replace(year=today.year - 100)

    random_date = start_date - datetime.timedelta(
        days=randint(0, (start_date - end_date).days)
    )
    return random_date


def generate_expiry_date() -> datetime:
    """
    Generate a credit card expiry date.

    Returns:
        datetime: The expiry date in MM-YYYY format.
    """
    today = datetime.date.today()
    year = str(today.year + randint(1, 10))
    month = str(randint(1, 12))
    expiry_date = f"{month}-{year}"
    logging.info("expiry date is set to %s", expiry_date)
    return expiry_date


def generate_CUSTOMER_NUMber():
    """_summary_

    Returns:
        _type_: _description_
    """
    letters = "".join(choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=4))
    numbers = "".join(choices("0123456789", k=4))
    return letters + numbers


def generate_customer_data(num_records):
    """_summary_

    Args:
        num_records (_type_): _description_

    Returns:
        _type_: _description_
    """
    data = []
    for _ in range(num_records):
        customer_data = {
            "CUSTOMER_NUMBER": generate_CUSTOMER_NUMber(),
            "FIRST_NAME": fake.first_name(),
            "LAST_NAME": fake.last_name(),
            "BIRTH_DATE": generate_birth_date(),
            "SSN": fake.ssn(),
            "CUSTOMER_ADDRESS.STREET": fake.street_name(),
            "CUSTOMER_ADDRESS.HOUSE_NUMBER": fake.building_number(),
            "CUSTOMER_ADDRESS.CITY": fake.city(),
            "CUSTOMER_ADDRESS.STATE": fake.state(),
            "CUSTOMER_ADDRESS.COUNTRY": fake.country(),
            "CUSTOMER_ADDRESS.ZIP_CODE": fake.zipcode(),
            "CREDITCARD.NUMBER": fake.credit_card_number(),
            "CREDITCARD.EXPIRATION_DATE": generate_expiry_date(),
            "CREDITCARD.VERIFICATION_CODE": fake.credit_card_security_code(),
            "CREDITCARD.PROVIDER": fake.credit_card_provider(),
        }

        data.append(customer_data)
    return data


def generate_transaction(num: int, customer_data: dict):
    """_summary_

    Args:
        num (int): _description_
        customer_data (dict): _description_

    Returns:
        _type_: _description_
    """
    creditcard_transaction = []
    product_transaction = []

    for i in range(num):
        transaction_id = str(uuid.uuid4())
        item_value = round(uniform(1, 100000), 2)
        item_quantity = randint(1, 200)
        transaction_value = item_quantity * item_value
        word_num = 50
        departments = [fake.word() for i in range(word_num)]

        creditcard_transaction_dict = {
            "TRANSACTION_ID": transaction_id,
            "CUSTOMER_NUMBER": choice(customer_data)["CUSTOMER_NUMBER"],
            "TRANSACTION_VALUE": transaction_value,
            "TRANSACTION_DATE_TIME": random_date_2022(),
            "NUMBER_OF_ITEMS": randint(1, 30),
        }

        product_transaction_dict = {
            "TRANSACTION_ID": transaction_id,
            "ITEM_EAN": fake.ean13(),
            "ITEM_DEPARTMENT": choice(departments),
            "ITEM_VALUE": item_value,
            "ITEM_QUANTITY": item_quantity,
        }

        creditcard_transaction.append(creditcard_transaction_dict)

        product_transaction.append(product_transaction_dict)
    return creditcard_transaction, product_transaction
