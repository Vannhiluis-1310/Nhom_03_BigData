from __future__ import annotations


def format_currency_brl(value: float | int | None) -> str:
    if value is None:
        return "R$ 0,00"
    return (
        f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )


def format_number(value: float | int | None) -> str:
    if value is None:
        return "0"
    return f"{float(value):,.0f}".replace(",", ".")


def format_percent(value: float | int | None, digits: int = 2) -> str:
    if value is None:
        return "0%"
    ratio = float(value)
    if ratio <= 1:
        ratio *= 100
    return f"{ratio:.{digits}f}%"


def format_review_score(value: float | int | None) -> str:
    if value is None:
        return "0.00 / 5"
    return f"{float(value):.2f} / 5"
