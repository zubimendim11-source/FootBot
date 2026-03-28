import random
from database import get_db

MAX_STAMINA = 50 # Обязательно добавь эту строку сюда!

INJURY_TYPES = [
    ("Ушиб голени", 1),
    ("Растяжение мышц", 3),
    ("Надрыв связок", 5),
    ("Разрыв крестообразной связки", 10)
]

def check_injury_chance(stamina):
    """
    Чем больше усталость, тем выше шанс.
    При 50 усталости шанс ~10% на проверку травмы.
    """
    chance = (stamina / MAX_STAMINA) * 0.1
    return random.random() < chance

def get_random_injury():
    """Случайная травма из списка"""
    return random.choice(INJURY_TYPES)

def can_get_injured(current_injured_count):
    """Проверка лимита: не более 4 травм на команду"""
    return current_injured_count < 4
