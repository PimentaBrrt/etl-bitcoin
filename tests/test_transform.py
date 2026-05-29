"""
Testes automatizados (pytest) das funções puras de transformação.

Cobrem as regras de negócio e a normalização, sem depender do banco —
rodam em qualquer máquina com `pytest`.
"""

import sys
from pathlib import Path

# garante que o pacote ``etl`` seja importável ao rodar de qualquer lugar
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from etl.transform import (
    daily_return_pct,
    intraday_range_pct,
    _to_int,
    _canon_sentiment,
)


class TestDailyReturn:
    def test_positivo(self):
        assert daily_return_pct(100, 110) == 10.0

    def test_negativo(self):
        assert daily_return_pct(100, 90) == -10.0

    def test_sem_variacao(self):
        assert daily_return_pct(100, 100) == 0.0

    def test_open_zero_retorna_none(self):
        assert daily_return_pct(0, 100) is None

    def test_open_none_retorna_none(self):
        assert daily_return_pct(None, 100) is None


class TestIntradayRange:
    def test_amplitude(self):
        assert intraday_range_pct(120, 100) == 20.0

    def test_sem_amplitude(self):
        assert intraday_range_pct(100, 100) == 0.0

    def test_low_zero_retorna_none(self):
        assert intraday_range_pct(100, 0) is None


class TestToInt:
    def test_inteiro_string(self):
        assert _to_int("30") == 30

    def test_float_string(self):
        assert _to_int("30.0") == 30

    def test_arredonda(self):
        assert _to_int("29.6") == 30

    def test_vazio(self):
        assert _to_int("") is None

    def test_nan_textual(self):
        assert _to_int("nan") is None

    def test_none(self):
        assert _to_int(None) is None


class TestCanonSentiment:
    def test_normaliza_caixa(self):
        assert _canon_sentiment("extreme fear") == "Extreme Fear"

    def test_normaliza_com_espacos(self):
        assert _canon_sentiment("  Greed  ") == "Greed"

    def test_classe_desconhecida(self):
        assert _canon_sentiment("Panic") is None

    def test_none(self):
        assert _canon_sentiment(None) is None

    def test_todas_classes(self):
        for c in ["Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"]:
            assert _canon_sentiment(c) == c
