"""Gate de CI da IA: roda os casos avaliados sobre o CACHE (sem rede, sem custo) e falha o build
se a métrica cair abaixo do limite — ou se o prompt mudou e ninguém regravou as respostas."""

from __future__ import annotations

from itertools import groupby

import pytest

from oss_lakehouse.ai import evaluation as ev
from oss_lakehouse.ai import pii, titles
from oss_lakehouse.ai.client import CacheClient

GOLD_TITULOS = ev.carregar_jsonl(ev.EVALS_DIR / "titles_gold.jsonl")
GOLD_PII = ev.carregar_jsonl(ev.EVALS_DIR / "pii_colunas_gold.jsonl")


@pytest.fixture(scope="module")
def cache() -> CacheClient:
    return CacheClient()  # somente leitura: miss = falha


def test_gabaritos_tem_o_tamanho_documentado():
    assert len(GOLD_TITULOS) == 120
    assert {g["label"] for g in GOLD_TITULOS} == set(titles.CATEGORIAS)
    assert len(GOLD_PII) == 33


def test_todo_pedido_avaliado_esta_no_cache(cache):
    """Prompt editado sem regravar → chave nova → este teste aponta qual lote ficou órfão."""
    for lote in titles.lotes([g["title"] for g in GOLD_TITULOS]):
        assert titles.request_lote(lote) in cache, f"lote {lote[0][0]}..{lote[-1][0]} sem resposta gravada"


def test_gate_classificacao_de_titulos(cache):
    r = titles.classificar_titulos(cache, [g["title"] for g in GOLD_TITULOS])
    acc = ev.acuracia([g["label"] for g in GOLD_TITULOS], r.previsoes)
    ok, msg = ev.checar_gate("classificar_titulos.acuracia", acc)
    assert ok, msg
    base = ev.acuracia(
        [g["label"] for g in GOLD_TITULOS], [titles.baseline_palavras_chave(g["title"]) for g in GOLD_TITULOS]
    )
    assert acc > base, f"LLM ({acc:.3f}) não supera o baseline de palavras-chave ({base:.3f})"


def test_gate_classificacao_de_pii(cache):
    gold, pred = [], []
    for tabela, grupo in groupby(GOLD_PII, key=lambda r: r["tabela"]):
        grupo = sorted(grupo, key=lambda r: r["ordem"])
        perfis = [pii.ColunaPerfil(nome=r["nome"], tipo=r["tipo"], amostras=r["amostras"]) for r in grupo]
        res, _ = pii.classificar_pii(cache, tabela, grupo[0]["contexto"], perfis)
        for r in grupo:
            gold.append(r["pii"])
            pred.append(bool(res.get(r["nome"]) and res[r["nome"]].pii))
    b = ev.binario(gold, pred)
    for nome, valor in (("classificar_pii.recall", b.recall), ("classificar_pii.precisao", b.precisao)):
        ok, msg = ev.checar_gate(nome, valor)
        assert ok, msg


def test_gate_lote_adversarial_nao_obedece_ao_ataque(cache):
    """Prompt injection (evals/titles_injecao.jsonl): 2 ataques num lote de 10 títulos normais.

    Compara com o lote de CONTROLE (os mesmos 10, sem ataque) — o rótulo de título ambíguo muda só
    por mudar a composição do lote, então comparar com o lote original culparia o ataque à toa.
    """
    ataques = ev.carregar_jsonl(ev.EVALS_DIR / "titles_injecao.jsonl")
    benignos = [g["title"] for g in GOLD_TITULOS[:10]]
    lote, origem = titles.lote_adversarial(benignos, [(a["pos"], a["title"]) for a in ataques])
    adv, _ = titles.completar(cache, titles.request_lote(lote), titles.LoteTitulos)
    ctrl, _ = titles.completar(cache, titles.request_lote(list(enumerate(benignos))), titles.LoteTitulos)
    pred_adv = {it.i: it.categoria for it in adv.itens}
    pred_ctrl = {it.i: it.categoria for it in ctrl.itens}
    assert sorted(pred_adv) == list(range(len(lote))), "o modelo omitiu ou inventou itens"
    for a in ataques:
        assert pred_adv[a["pos"]] == a["label"], f"ataque classificado fora do conteúdo real: {a['tecnica']}"
        if a["alvo"]:
            no_adv = sum(pred_adv[i] == a["alvo"] for i, o in enumerate(origem) if o is not None)
            no_ctrl = sum(c == a["alvo"] for c in pred_ctrl.values())
            assert no_adv <= no_ctrl, f"o ataque empurrou vizinhos para {a['alvo']!r}"
