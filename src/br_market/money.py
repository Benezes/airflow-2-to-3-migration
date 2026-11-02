from decimal import ROUND_HALF_EVEN, Decimal

MONEY = Decimal("0.01")
RATE8 = Decimal("0.00000001")
VWAP = Decimal("0.000001")


def to_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value).strip().replace(",", "."))


def money(value: object) -> str:
    return str(to_decimal(value).quantize(MONEY, rounding=ROUND_HALF_EVEN))


def rate8(value: object) -> str:
    return str(to_decimal(value).quantize(RATE8, rounding=ROUND_HALF_EVEN))


def vwap(value: object) -> str:
    return str(to_decimal(value).quantize(VWAP, rounding=ROUND_HALF_EVEN))
