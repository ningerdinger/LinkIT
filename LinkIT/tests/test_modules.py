import pytest
from unittest.mock import mock_open, patch
import datetime

from src.modules.modules import (
    random_date_2022,
    save_to_csv,
    generate_birth_date,
    generate_expiry_date,
    generate_CUSTOMER_NUMber,
    generate_customer_data,
    generate_transaction,
)


def test_random_date_2022():
    date_str = random_date_2022()
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    assert datetime.datetime(2022, 1, 1) <= date <= datetime.datetime(2022, 12, 31)


def test_save_to_csv():
    data = [
        {"header1": "value1", "header2": "value2"},
        {"header1": "value3", "header2": "value4"},
    ]
    filename = "test.csv"

    # Mock the open function and csv.DictWriter
    m = mock_open()
    with patch("builtins.open", m), patch("csv.DictWriter") as mock_writer:
        mock_instance = mock_writer.return_value
        save_to_csv(data, filename)

        # Check if the file was opened correctly
        m.assert_called_once_with(filename, mode="w", newline="")

        # Check if the writer was initialized with the correct headers
        mock_writer.assert_called_once_with(m(), fieldnames=data[0].keys())

        # Check if writeheader and writerows were called
        mock_instance.writeheader.assert_called_once()
        mock_instance.writerows.assert_called_once_with(data)


def test_generate_birth_date():
    birth_date = generate_birth_date()
    today = datetime.date.today()
    assert (
        today.replace(year=today.year - 100)
        <= birth_date
        <= today.replace(year=today.year - 18)
    )


def test_generate_expiry_date():
    expiry_date = generate_expiry_date()
    month, year = map(int, expiry_date.split("-"))
    today = datetime.date.today()
    assert today.year + 1 <= year <= today.year + 10
    assert 1 <= month <= 12


def test_generate_CUSTOMER_NUMber():
    customer_number = generate_CUSTOMER_NUMber()
    assert len(customer_number) == 8
    assert customer_number[:4].isalpha()
    assert customer_number[4:].isdigit()


def test_generate_customer_data():
    num_records = 5
    data = generate_customer_data(num_records)
    assert len(data) == num_records
    for record in data:
        assert "CUSTOMER_NUMBER" in record
        assert "FIRST_NAME" in record
        assert "LAST_NAME" in record


def test_generate_transaction():
    num = 5
    customer_data = generate_customer_data(5)
    creditcard_transaction, product_transaction = generate_transaction(
        num, customer_data
    )
    assert len(creditcard_transaction) == num
    assert len(product_transaction) == num
    for transaction in creditcard_transaction:
        assert "TRANSACTION_ID" in transaction
        assert "CUSTOMER_NUMBER" in transaction
    for transaction in product_transaction:
        assert "TRANSACTION_ID" in transaction
        assert "ITEM_EAN" in transaction


if __name__ == "__main__":
    pytest.main()
