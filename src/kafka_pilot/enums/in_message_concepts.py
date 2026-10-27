from enum import Enum


class PriceConcept(Enum):
    REFERENCE_PRICE = 38  # price even after increase or decrease fund
    YESTERDAY_PRICE = 34
