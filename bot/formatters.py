import re
from typing import List, Tuple

from backend.utils.format import format_rub

def clean_dish_name(name: str) -> str:
    """Removes leading numeric codes from dish names."""
    return re.sub(r'^\d+', '', str(name)).strip()

def format_percent_change(value: float) -> str:
    """Formats percentage change with appropriate sign."""
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.1f}%"

def format_ranked_list(items: List[Tuple[str, float]], is_ruble: bool = True) -> str:
    """Formats a ranked list of items."""
    lines = []
    for i, (name, value) in enumerate(items, 1):
        clean_name = clean_dish_name(name)
        if is_ruble:
            formatted_value = format_rub(float(value))
        else:
            formatted_value = f"{int(value)} шт"
        lines.append(f"{i}. {clean_name} — {formatted_value}")
    return "\n".join(lines)
