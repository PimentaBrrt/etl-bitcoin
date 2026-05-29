"""
test_binance.py — Teste rápido e isolado da fonte de mercado (Binance).

Rode ANTES de mexer no pipeline para confirmar que a sua rede alcança a Binance
e que o período tem interseção com o CSV do Fear & Greed.

    python test_binance.py

Se imprimir "OK", pode aplicar a troca da fonte no extract.py com segurança.
"""
import datetime as dt
import sys

import requests

# Janela que casa com o CSV (2018-02-01 .. 2023-03-31). Use um trecho pequeno p/ testar.
START = dt.date(2021, 1, 1)
END = dt.date(2021, 1, 10)

URL = "https://api.binance.com/api/v3/klines"


def to_ms(d: dt.date) -> int:
    return int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).timestamp() * 1000)


def main() -> int:
    params = {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "startTime": to_ms(START),
        "endTime": to_ms(END),
        "limit": 1000,
    }
    try:
        r = requests.get(URL, params=params, timeout=30)
    except Exception as e:
        print(f"FALHOU (rede): {type(e).__name__}: {e}")
        print("-> Sua rede pode estar bloqueando a Binance. Me avise que sugiro outra fonte.")
        return 1

    if r.status_code != 200:
        print(f"FALHOU (HTTP {r.status_code}): {r.text[:200]}")
        print("-> Binance pode estar geobloqueada na sua região. Me avise.")
        return 1

    rows = r.json()
    print(f"OK — {len(rows)} dias retornados de {START} a {END}")
    first = rows[0]
    d = dt.datetime.fromtimestamp(first[0] / 1000, dt.timezone.utc).date()
    print(f"   exemplo: {d}  open={first[1]}  high={first[2]}  low={first[3]}  "
          f"close={first[4]}  volume(BTC)={first[5]}")
    print("\nPode aplicar a troca da fonte com segurança.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
