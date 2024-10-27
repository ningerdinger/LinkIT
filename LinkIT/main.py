"""
File that generates part 1 data
"""

import datetime
import logging
from random import randint, choices
from faker import Faker


class CustomerData:
    """_summary_"""

    def __init__(self, num: int):
        fake = Faker()
        self.data = []
        for _ in range(num):
            self.customer_number = self.generate_customer_number()
            self.birthdate = self.generate_birth_date()
            self.first_name = fake.first_name()
            self.last_name = fake.last_name()
            self.ssn = fake.ssn()
            self.street_address = fake.street_name()
            self.house_number = fake.building_number()
            self.city = fake.city()
            self.state = fake.state()
            self.country = fake.country()
            self.zip_code = fake.zipcode()
            self.credit_card_number = fake.credit_card_number()
            self.credit_card_expiry = self.generate_expiry_date()
            self.credit_card_cvv = fake.credit_card_security_code()
            self.credit_card_provider = fake.credit_card_provider()

    def generate_birth_date(self) -> datetime:
        """generates a random birthdate
        User is between 18 and 100 years old

        Returns:
            datetime: return a datetime datatype
        """
        today = datetime.date.today()
        year_start = today.year - 18
        year_end = today.year - 100

        month = randint(1, 12)
        year = randint(year_end, year_start)

        checker = [1, 3, 5, 7, 8, 10, 12]

        if month in checker:
            day = randint(1, 31)
        if month == 2:
            day = randint(1, 28)
        else:
            day = randint(1, 30)

        date = datetime.date(year, month, day)
        return date

    def generate_expiry_date(self) -> datetime:
        """
        the credit card expiry date

        Returns:
            datetime:
        """
        self.today = datetime.date.today()
        self.year = str(self.today.year + randint(1, 10))
        self.month = str(randint(1, 12))
        self.expiry_date = f"{self.month}-{self.year}"
        logging.info("expiry date is set to %s", self.expiry_date)
        return self.expiry_date

    def generate_customer_number(self) -> str:
        """
        Returns:
            _type_: _description_
        """
        letters = "".join(choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=4))
        numbers = "".join(choices("0123456789", k=4))
        return letters + numbers


# def generate_customer_data(num_records: int) -> list:
#     """_summary_

#     Args:
#         num_records (_type_): _description_

#     Returns:
#         _type_: _description_
#     """
#     data = []
#     for _ in range(num_records):
#         customer_number = generate_customer_number()
#         birthdate = generate_birth_date()
#         first_name = fake.first_name()
#         last_name = fake.last_name()
#         ssn = fake.ssn()
#         street_address = fake.street_name()
#         house_number = fake.building_number()
#         city = fake.city()
#         state = fake.state()
#         country = fake.country()
#         zip_code = fake.zipcode()
#         credit_card_number = fake.credit_card_number()
#         credit_card_expiry = generate_expiry_date()
#         credit_card_cvv = fake.credit_card_security_code()
#         credit_card_provider = fake.credit_card_provider()

#         data.append(
#             [
#                 customer_number,
#                 birthdate,
#                 first_name,
#                 last_name,
#                 ssn,
#                 street_address,
#                 house_number,
#                 city,
#                 state,
#                 country,
#                 zip_code,
#                 credit_card_number,
#                 credit_card_expiry,
#                 credit_card_cvv,
#                 credit_card_provider,
#             ]
#         )

#     return data
