TEAM_BALANCES = {
    # --- ЕВРОПЕЙСКИЕ ГИГАНТЫ ---
    "Real Madrid": 200_000_000,
    "Barcelona": 110_000_000,
    "Bayern": 160_000_000,
    "PSG": 220_000_000,
    "Inter": 120_000_000,
    "Milan": 100_000_000,
    "Borussia Dortmund": 100_000_000,
    "Atletico Madrid": 120_000_000,
    "Bayer Leverkusen": 95_000_000,
    "Juventus": 105_000_000,

    # --- TOP CLUBS / АПЛ ---
    "Arsenal": 150_000_000,
    "Man City": 180_000_000,
    "Manchester United": 140_000_000,
    "Liverpool": 130_000_000,
    "Chelsea": 160_000_000,
    "Tottenham": 130_000_000,
    "Newcastle": 120_000_000,
    "Napoli": 100_000_000,
    "RB Leipzig": 100_000_000, # Добавлено

    # --- STRONG EUROPEAN (Benfica, Porto, Roma etc.) ---
    "Ajax": 90_000_000,
    "SL Benfica": 85_000_000,    # Добавлено
    "FC Porto": 80_000_000,      # Добавлено
    "Sporting CP": 80_000_000,   # Добавлено
    "AS Roma": 85_000_000,       # Добавлено
    "Atalanta": 80_000_000,      # Добавлено
    "Olympique Lyonnais": 75_000_000, # Добавлено
    "Olympique de Marseille": 80_000_000, # Добавлено
    "PSV": 70_000_000,           # Добавлено
    "Eintracht Frankfurt": 70_000_000, # Добавлено
    "Aston Villa": 95_000_000,
    "Monaco": 80_000_000,

    # --- MID-TABLE / STABLE ---
    "Stuttgart": 65_000_000,     # Добавлено
    "Real Betis": 60_000_000,    # Добавлено
    "Athletic Bilbao": 75_000_000,
    "Brighton": 75_000_000,
    "West Ham": 80_000_000,
    "Fulham": 65_000_000,
    "Villarreal": 65_000_000,    # Добавлено
    "Real Sociedad": 70_000_000,  # Добавлено
    "Club Brugge": 50_000_000,

    # --- LOWER MID-TABLE ---
    "Brentford": 55_000_000,
    "Everton": 50_000_000,
    "Crystal Palace": 50_000_000,
    "Bournemouth": 45_000_000,
    "Wolves": 48_000_000,

    # --- BOTTOM TABLE / CHAMPIONSHIP ---
    "Forest": 40_000_000,
    "Leeds": 40_000_000,
    "Sunderland": 40_000_000,
    "Burnley": 40_000_000
}

def get_balance(team_name):
    """Возвращает баланс команды или 0, если команда не найдена"""
    return TEAM_BALANCES.get(team_name, 0)
