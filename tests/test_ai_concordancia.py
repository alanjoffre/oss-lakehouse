from __future__ import annotations

import pytest

from oss_lakehouse.ai.evaluation import kappa_cohen


def test_kappa_concordancia_total_e_um():
    assert kappa_cohen(["bug", "docs", "bug"], ["bug", "docs", "bug"]) == 1.0


def test_kappa_desconta_o_acaso():
    # 80% de concordância bruta, mas com classes 50/50 o acaso já daria 50%: kappa = 0,6.
    a = ["s"] * 5 + ["n"] * 5
    b = ["s"] * 4 + ["n"] + ["n"] * 4 + ["s"]
    assert kappa_cohen(a, b) == pytest.approx(0.6)


def test_kappa_classe_dominante_nao_engana():
    # 90% de concordância bruta e kappa ~0: um rotulador só responde a classe majoritária.
    a = ["n"] * 9 + ["s"]
    b = ["n"] * 10
    assert kappa_cohen(a, b) == pytest.approx(0.0)


def test_kappa_rejeita_tamanhos_diferentes():
    with pytest.raises(ValueError):
        kappa_cohen(["a"], ["a", "b"])
