"""Casos de uso de IA com LLM falso: o que importa testar é o NOSSO código em volta do modelo
(montagem do pedido, validação, compilação, aprovação, escape, lote, métricas)."""

from __future__ import annotations

import json
import re

import pytest

from oss_lakehouse.ai import docgen, pii, quality, titles, triage
from oss_lakehouse.ai import evaluation as ev
from oss_lakehouse.ai.batch import TokenBucket, inferir_em_lote_spark, processar_lotes
from oss_lakehouse.ai.client import LLMRequest, LLMResponse, Usage


class LLMRoteirizado:
    """Responde com uma função do pedido — simula o modelo sem rede."""

    name = "roteiro"

    def __init__(self, responder):
        self.responder, self.pedidos = responder, []

    def complete(self, request: LLMRequest) -> LLMResponse:
        self.pedidos.append(request)
        return LLMResponse(key=request.key(), data=self.responder(request), usage=Usage(100, 10))


def _itens(request: LLMRequest) -> list[dict]:
    return json.loads(request.user[request.user.index("[") : request.user.rindex("]") + 1])


# --- títulos -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("titulo", "esperado"),
    [
        ("chore(deps): bump lodash from 4.17.20 to 4.17.21", "chore"),
        ("fix(deps): update mastodon v4.7.2 → v4.7.3", "chore"),
        ("[fix](be) Restore unit tests", "bug"),
        ("feat(auth): add email registration", "feature"),
        ("Update documentation", "docs"),
        ("App crashes when config is empty", "bug"),
        ("Add dark mode", "feature"),
        ("[TVSHOW] The Rookie", "outro"),
    ],
)
def test_baseline_palavras_chave(titulo, esperado):
    assert titles.baseline_palavras_chave(titulo) == esperado


def test_regra_de_alta_confianca_so_decide_quando_ha_convencao():
    assert titles.regra_de_alta_confianca("fix(api): handle null") == "bug"
    assert titles.regra_de_alta_confianca("Bump lodash from 1 to 2") == "chore"
    assert titles.regra_de_alta_confianca("App crashes when config is empty") is None


def test_classificar_titulos_em_lotes_ignora_indice_inventado_e_conta_omissao():
    def responder(r):
        itens = _itens(r)
        # o modelo "esquece" o último item do lote e inventa um índice que não existe
        return {
            "itens": [{"i": it["i"], "categoria": "bug"} for it in itens[:-1]]
            + [{"i": 999, "categoria": "docs"}]
        }

    llm = LLMRoteirizado(responder)
    res = titles.classificar_titulos(llm, [f"titulo {k}" for k in range(30)], tamanho_lote=25)
    assert len(llm.pedidos) == 2
    assert res.sem_resposta == 2 and res.previsoes[24] is None and res.previsoes[29] is None
    assert res.previsoes.count("bug") == 28


def test_lote_adversarial_insere_ataques_e_guarda_a_origem():
    lote, origem = titles.lote_adversarial(["a", "b", "c"], [(3, "ATAQUE 2"), (1, "ATAQUE 1")])
    assert [t for _, t in lote] == ["a", "ATAQUE 1", "b", "ATAQUE 2", "c"]
    assert [i for i, _ in lote] == [0, 1, 2, 3, 4]
    assert origem == [0, None, 1, None, 2]


def test_titulo_tem_pii_removida_antes_de_ir_ao_llm():
    r = titles.request_lote([(0, "erro ao enviar para ana@empresa.com.br, cc @fulano")])
    assert "ana@empresa" not in r.user and "<EMAIL>" in r.user and "<USUARIO>" in r.user


# --- PII -----------------------------------------------------------------------


def test_cpf_ficticio_tem_digito_verificador_valido():
    import random

    def valido(cpf: str) -> bool:
        d = [int(c) for c in re.sub(r"\D", "", cpf)]
        for n in (9, 10):
            s = sum(v * p for v, p in zip(d[:n], range(n + 1, 1, -1), strict=True))
            if d[n] != (0 if (s * 10) % 11 == 10 else (s * 10) % 11):
                return False
        return True

    rng = random.Random(1)
    assert all(valido(pii.cpf_ficticio(rng)) for _ in range(200))


def test_baseline_regex_de_pii():
    assert pii.baseline_regex("x", ["123.456.789-09"], usar_nome=False)
    assert pii.baseline_regex("x", ["ana.1@example.com"], usar_nome=False)
    assert pii.baseline_regex("actor_login", ["octocat"])  # só pelo nome
    assert not pii.baseline_regex("actor_login", ["octocat"], usar_nome=False)
    assert not pii.baseline_regex("plano", ["premium"])


# --- avaliação -----------------------------------------------------------------


def test_metricas_matriz_e_wilson():
    gold = ["bug", "bug", "feature", "docs"]
    pred = ["bug", "feature", "feature", None]
    assert ev.acuracia(gold, pred) == 0.5
    m = ev.matriz_confusao(gold, pred, ["bug", "feature", "docs"])
    assert m == [[1, 1, 0, 0], [0, 1, 0, 0], [0, 0, 0, 1]]
    pc = ev.metricas_por_classe(gold, pred, ["bug", "feature", "docs"])
    assert pc["bug"].precisao == 1.0 and pc["bug"].recall == 0.5 and pc["feature"].precisao == 0.5
    lo, hi = ev.intervalo_wilson(105, 120)
    assert lo < 105 / 120 < hi and hi - lo > 0.1  # n=120 → intervalo largo
    b = ev.binario([True, True, False], [True, False, True])
    assert (b.tp, b.fp, b.fn, b.tn) == (1, 1, 1, 0)


# --- triagem -------------------------------------------------------------------


def test_normalizar_erro_remove_caminho_ids_e_stacktrace():
    bruto = (
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column `actor`.`logn` cannot be resolved. SQLSTATE: 42703;\n"
        "'Project ['actor.logn]\n+- Relation [id#12,type#13L] parquet\n"
        "\tat org.apache.spark.sql.X(X.scala:1)\n"
        "Path: file:/home/ana/projeto/data/demo/12/t/part-0001.parquet "
        "run 1b4e28ba-2fa1-11d2-883f-0016d3cca427"
    )
    n = triage.normalizar_erro(bruto, raizes=("/home/ana/projeto/data",))
    assert "#12" not in n and "#N" in n and "scala" not in n
    assert "/home/ana" not in n and "1b4e28ba" not in n
    assert n == triage.normalizar_erro(bruto.replace("#12", "#98"), raizes=("/home/ana/projeto/data",))


def test_normalizar_erro_remove_valor_de_dado_citado_na_mensagem():
    bruto = (
        "NumberFormatException: [CAST_INVALID_INPUT] The value 'ana-silva/repo-privado' of the type "
        '"STRING" cannot be cast to "INT" because it is malformed. SQLSTATE: 22018'
    )
    n = triage.normalizar_erro(bruto)
    assert "ana-silva" not in n and "The value '<valor>' of the type" in n and "CAST_INVALID_INPUT" in n


# --- documentação ----------------------------------------------------------------


def test_sql_de_comentario_escapa_texto_do_llm():
    doc = docgen.DocTabela(
        comentario_tabela="Eventos",
        colunas=[docgen.ComentarioColuna(nome="c", comentario="it's'); DROP TABLE x; --\nlinha 2")],
    )
    sql = docgen.sql_comentarios("/t", doc)[1]
    assert "\n" not in sql and "\\'" in sql
    assert sql.endswith("COMMENT 'it\\'s\\'); DROP TABLE x; -- linha 2'")


def test_sql_de_comentario_escapa_crase_no_nome_da_coluna():
    # O nome da coluna vem do modelo: uma crase fecharia o identificador e abriria espaço para outro comando.
    doc = docgen.DocTabela(
        comentario_tabela="t",
        colunas=[docgen.ComentarioColuna(nome="a` COMMENT 'x'; DROP TABLE t; --", comentario="c")],
    )
    sql = docgen.sql_comentarios("/t", doc)[1]
    assert "`a`` COMMENT 'x'; DROP TABLE t; --`" in sql


def test_revisao_de_doc_rejeita_substitui_e_descarta_coluna_inventada():
    doc = docgen.DocTabela(
        comentario_tabela="t",
        colunas=[docgen.ComentarioColuna(nome=n, comentario=n.upper()) for n in ("a", "b", "fantasma")],
    )
    rev = docgen.revisar_doc(doc, {"a": "A revisado", "b": None}, colunas_reais=["a", "b"])
    assert [(c.nome, c.comentario) for c in rev.colunas] == [("a", "A revisado")]


# --- rate limit e lote ---------------------------------------------------------------


def test_token_bucket_espera_o_necessario():
    agora = [0.0]
    esperas = []

    def dormir(s):
        esperas.append(s)
        agora[0] += s

    tb = TokenBucket(taxa=2.0, capacidade=2, clock=lambda: agora[0], sleep=dormir)
    assert tb.adquirir() == 0 and tb.adquirir() == 0  # rajada
    assert tb.adquirir() == pytest.approx(0.5)  # 3ª ficha: 1/taxa
    assert esperas == [pytest.approx(0.5)]


def test_processar_lotes_agrupa_ordena_e_chama_uma_vez_por_lote():
    def montar(itens):
        return LLMRequest("p", "1", "s", json.dumps([r["id"] for r in itens]), {})

    def interpretar(resp, itens):
        return [{"id": r["id"], "rotulo": f"lote{r['lote']}"} for r in itens]

    llm = LLMRoteirizado(lambda r: {})
    linhas = [{"id": i, "lote": i // 3} for i in (5, 0, 4, 1, 3, 2, 6)]
    out = list(processar_lotes(linhas, "lote", "id", montar, interpretar, llm, max_concorrencia=2))
    assert sorted(o["id"] for o in out) == list(range(7))
    assert sorted(r.user for r in llm.pedidos) == ["[0, 1, 2]", "[3, 4, 5]", "[6]"]


# --- Spark: qualidade e lote ---------------------------------------------------------


def _regra(**kw) -> quality.Regra:
    base = {"nome": "r", "coluna": "n", "regra": "not_null", "severidade": "warn"}
    return quality.Regra(**{**base, **kw})


def test_compilar_revisar_e_executar_regras(spark):
    df = spark.createDataFrame(
        [(1, "PushEvent", "a"), (2, "Bogus", None), (2, "PushEvent", "abc"), (None, "WatchEvent", "x")],
        "n bigint, tipo string, s string",
    )
    tipos = dict(df.dtypes)
    regras = [
        _regra(nome="n_nao_nulo"),
        _regra(nome="n_unico", regra="unique"),
        _regra(
            nome="tipo_valido", coluna="tipo", regra="accepted_values", valores=["PushEvent", "WatchEvent"]
        ),
        _regra(nome="s_curto", coluna="s", regra="max_length", maximo=2),
        _regra(nome="n_faixa", regra="range", minimo=1, maximo=1),
        _regra(nome="faixa_em_texto", coluna="s", regra="range", minimo=0),
        _regra(nome="coluna_fantasma", coluna="nao_existe"),
        _regra(nome="sem_decisao", coluna="s"),
    ]
    decisoes = {r.nome: "aprovar" for r in regras[:-1]} | {"n_faixa": {"severidade": "drop"}}
    rev = quality.revisar(regras, decisoes, tipos, revisor="ana")
    nomes = [r.nome for r in rev.aprovadas]
    assert nomes == ["n_nao_nulo", "n_unico", "tipo_valido", "s_curto", "n_faixa"]
    motivos = {r["regra"]: (r["por"], r["decisao"]) for r in rev.registro}
    assert motivos["faixa_em_texto"][0] == "compilador" and motivos["coluna_fantasma"][0] == "compilador"
    assert motivos["sem_decisao"] == ("ana", "rejeitada")  # default deny
    assert rev.aprovadas[-1].severidade == "drop"
    res = {r["regra"]: r["reprovadas"] for r in quality.executar(df, rev.aprovadas)}
    assert res == {"n_nao_nulo": 1, "n_unico": 2, "tipo_valido": 1, "s_curto": 1, "n_faixa": 2}


def test_profiling_mascara_coluna_pessoal(spark):
    df = spark.createDataFrame([("Ana", "x"), ("Bob", "x"), (None, "y")], "login string, tipo string")
    perfil = {p["coluna"]: p for p in quality.profiling(df, colunas_pessoais=["login"])}
    assert perfil["login"]["min"] == "Xxx" and "valores_frequentes" not in perfil["login"]
    assert perfil["login"]["nulos"] == 1 and perfil["tipo"]["valores_frequentes"] == {"x": 2, "y": 1}


def test_aplicar_comentarios_numa_tabela_delta(spark, tmp_path):
    caminho = str(tmp_path / "t")
    spark.createDataFrame([(1, "a")], "id int, nome string").write.format("delta").save(caminho)
    doc = docgen.DocTabela(
        comentario_tabela="Tabela 'teste'",
        colunas=[docgen.ComentarioColuna(nome="nome", comentario="O nome d'ela")],
    )
    assert docgen.aplicar(spark, caminho, doc) == 2
    desc = {r.col_name: r.comment for r in spark.sql(f"DESCRIBE TABLE delta.`{caminho}`").collect()}
    assert desc["nome"] == "O nome d'ela"
    assert spark.sql(f"DESCRIBE DETAIL delta.`{caminho}`").first()["description"] == "Tabela 'teste'"


def test_inferencia_em_lote_no_spark(spark, tmp_path):
    """Replay do cache dentro dos executores: a função de partição cria o próprio cliente."""
    from functools import partial

    from oss_lakehouse.ai.client import CacheClient

    df = spark.createDataFrame([(i, i // 2, f"t{i}") for i in range(6)], "id int, lote int, texto string")

    def montar(itens):
        return LLMRequest("p", "1", "s", ",".join(r["texto"] for r in itens), {})

    def interpretar(resp, itens):
        return [{"id": r["id"], "rotulo": resp.data["r"]} for r in itens]

    cache_path = tmp_path / "cache.jsonl"
    gravador = CacheClient(cache_path, gravador=LLMRoteirizado(lambda r: {"r": r.user}))
    for lote in range(3):
        gravador.complete(montar([{"texto": f"t{2 * lote}"}, {"texto": f"t{2 * lote + 1}"}]))

    out = inferir_em_lote_spark(
        spark,
        df,
        "lote",
        "id",
        montar,
        interpretar,
        partial(CacheClient, cache_path),
        "id int, rotulo string",
        particoes=2,
        req_por_segundo=1000,
    )
    got = {r.id: r.rotulo for r in out.collect()}
    assert got == {0: "t0,t1", 1: "t0,t1", 2: "t2,t3", 3: "t2,t3", 4: "t4,t5", 5: "t4,t5"}
