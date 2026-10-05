"""Governança: pseudonimização, ataque de dicionário, tokenização, criptografia, visão por papel
e o direito ao esquecimento numa tabela Delta (DELETE → REORG PURGE → VACUUM)."""

from __future__ import annotations

import hashlib

import pytest
from pyspark.sql import functions as F

from oss_lakehouse import governance as g

CHAVE = b"chave-de-teste"


def test_hmac_nativo_bate_com_hmac_do_python(spark):
    df = spark.createDataFrame([("octocat",), ("ana",), (None,)], "login string")
    got = [r.h for r in df.select(g.hmac_sha256("login", CHAVE).alias("h")).collect()]
    assert got[:2] == [g.hmac_sha256_py("octocat", CHAVE), g.hmac_sha256_py("ana", CHAVE)]
    assert got[2] is None
    longa = b"k" * 100  # chave maior que o bloco: é hasheada antes (RFC 2104)
    h = df.limit(1).select(g.hmac_sha256("login", longa)).first()[0]
    assert h == g.hmac_sha256_py("octocat", longa)


def test_hash_sem_sal_cai_em_ataque_de_dicionario_e_hmac_nao(spark):
    df = spark.createDataFrame([("octocat",), ("ana",), ("segredo-raro",)], "login string")
    protegido = df.select(g.sha256_sem_sal("login").alias("h"), g.hmac_sha256("login", CHAVE).alias("hm"))
    candidatos = spark.createDataFrame([("octocat",), ("ana",), ("bob",)], "c string")
    recuperados = g.ataque_dicionario(protegido, "h", candidatos, "c")
    assert sorted(r.valor_recuperado for r in recuperados.collect()) == ["ana", "octocat"]
    assert g.ataque_dicionario(protegido, "hm", candidatos, "c").count() == 0


def test_tokenizacao_com_cofre_persistido(spark, tmp_path):
    df = spark.createDataFrame([("ana", 1), ("bob", 2), ("ana", 3)], "login string, n int")
    caminho = str(tmp_path / "cofre")
    g.construir_cofre_tokens(df, "login").write.format("delta").save(caminho)
    cofre = spark.read.format("delta").load(caminho)
    tok = g.tokenizar(df, "login", cofre).orderBy("n").collect()
    assert tok[0].login == tok[2].login != tok[1].login and tok[0].login.startswith("tok_")
    reverso = {r.token: r.valor for r in cofre.collect()}
    assert reverso[tok[1].login] == "bob"


def test_criptografia_ida_e_volta_e_chave_errada_devolve_nulo(spark):
    k1, k2 = hashlib.sha256(b"a").hexdigest(), hashlib.sha256(b"b").hexdigest()
    df = spark.createDataFrame([("ana@example.com", k1, k2)], "v string, k1 string, k2 string")
    r = (
        df.select(g.criptografar("v", "k1").alias("c"), "k1", "k2")
        .select("c", g.descriptografar("c", "k1").alias("ok"), g.descriptografar("c", "k2").alias("errada"))
        .first()
    )
    assert r.c != "ana@example.com" and r.ok == "ana@example.com" and r.errada is None


def test_mascaras_spark_e_python_concordam(spark):
    valores = ["Ana.Silva99@x.com", "123.456.789-09", "octocat"]
    df = spark.createDataFrame([(v,) for v in valores], "v string")
    got = [r[0] for r in df.select(g.mascara_formato("v")).collect()]
    assert got == [g.mascara_formato_py(v) for v in valores]
    assert got[1] == "999.999.999-99"
    assert df.select(g.mascara_parcial("v", 2)).collect()[2][0] == "*****at"


def test_mascara_email_e_cpf(spark):
    df = spark.createDataFrame(
        [("ana.silva@example.com", "123.456.789-00"), ("sem-arroba", "12345678900"), (None, "123")],
        "email string, cpf string",
    )
    got = df.select(g.mascara_email("email").alias("e"), g.mascara_cpf("cpf").alias("c")).collect()
    assert [r.e for r in got] == ["a***@example.com", "***", None]
    assert [r.c for r in got] == ["***.456.789-**", "***.456.789-**", "***"]


def test_k_anonimato_mede_o_menor_grupo(spark):
    df = spark.createDataFrame([("a", 1), ("a", 1), ("a", 2), ("b", 1), ("b", 1)], "q1 string, q2 int")
    assert g.medir_k_anonimato(df, ["q1", "q2"]) == {"k": 1, "grupos": 3, "grupos_unicos": 1, "linhas": 5}
    assert g.medir_k_anonimato(df, ["q1"])["k"] == 2
    assert g.medir_k_anonimato(df.limit(0), ["q1"])["k"] == 0


def test_classificacao_vira_metadado_da_tabela_delta(spark, tmp_path):
    caminho = str(tmp_path / "classif")
    spark.createDataFrame(
        [("1", "octocat", "u", "PushEvent")], "id string, actor_login string, actor_url string, type string"
    ).write.format("delta").save(caminho)
    marcadas = g.aplicar_classificacao_delta(spark, caminho, g.POLITICA_GH_EVENTS)
    # colunas da política que não existem na tabela são ignoradas
    assert marcadas == ["id", "type", "actor_login", "actor_url"]
    props = g.ler_classificacao_delta(spark, caminho)
    # chave com "url": o SHOW TBLPROPERTIES redige o valor; a leitura por DESCRIBE DETAIL não
    assert props["governanca.classificacao.actor_url"] == "confidencial"
    show = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES delta.`{caminho}`").collect()}
    assert "redacted" in show["governanca.classificacao.actor_url"]
    assert props["governanca.classificacao.actor_login"] == "confidencial"
    assert props["governanca.classificacao_maxima"] == "confidencial"
    assert props["governanca.contem_dado_pessoal"] == "true"
    comentario = spark.read.format("delta").load(caminho).schema["actor_login"].metadata["comment"]
    assert comentario.startswith("[classificacao=confidencial; dado_pessoal=sim; tratamento=hmac]")


def test_limpar_texto_livre():
    t = g.limpar_texto_livre("ligar (11) 98765-4321, cpf 123.456.789-09, ana@x.com, ip 10.0.0.1 cc @octocat")
    assert t == "ligar <TELEFONE>, cpf <CPF>, <EMAIL>, ip <IP> cc <USUARIO>"


def test_politica_por_papel(spark):
    df = spark.createDataFrame(
        [("1", "octocat", "{}", "PushEvent")], "id string, actor_login string, payload string, type string"
    )
    pol = [p for p in g.POLITICA_GH_EVENTS if p.coluna in df.columns]
    analista = g.aplicar_politica(df, pol, "analista", CHAVE)
    assert "payload" not in analista.columns
    assert analista.first().actor_login == g.hmac_sha256_py("octocat", CHAVE)
    assert g.aplicar_politica(df, pol, "dpo", CHAVE).first().actor_login == "octocat"


def test_visao_dinamica_reage_a_variavel_de_sessao(spark):
    spark.createDataFrame(
        [("octocat", "PushEvent")], "actor_login string, type string"
    ).createOrReplaceTempView("ev_t")
    pol = [
        g.PoliticaColuna("actor_login", g.Classificacao.CONFIDENCIAL, True, "mascara"),
        g.PoliticaColuna("type", g.Classificacao.PUBLICO, False),
    ]
    spark.sql("DECLARE OR REPLACE VARIABLE papel STRING DEFAULT 'analista'")
    spark.sql(g.sql_visao_dinamica("ev_v", "ev_t", pol))
    assert spark.sql("SELECT actor_login FROM ev_v").first()[0] == "xxxxxxx"
    spark.sql("SET VAR papel = 'dpo'")
    assert spark.sql("SELECT actor_login FROM ev_v").first()[0] == "octocat"
    spark.sql("SET VAR papel = 'analista'")


def test_visao_dinamica_com_chave_usa_hmac_de_verdade(spark):
    spark.createDataFrame(
        [("octocat", 583231), (None, None)], "actor_login string, actor_id long"
    ).createOrReplaceTempView("ev_h")
    pol = [p for p in g.POLITICA_GH_EVENTS if p.coluna in ("actor_login", "actor_id")]
    spark.sql("DECLARE OR REPLACE VARIABLE papel STRING DEFAULT 'analista'")
    spark.sql(g.sql_visao_dinamica("ev_hv", "ev_h", pol, chave=CHAVE))
    r = spark.sql("SELECT * FROM ev_hv WHERE actor_login IS NOT NULL").first()
    assert r.actor_login == g.hmac_sha256_py("octocat", CHAVE)
    assert r.actor_id == g.hmac_sha256_py("583231", CHAVE)
    assert spark.sql("SELECT count(*) FROM ev_hv WHERE actor_login IS NULL").first()[0] == 1
    longa = b"k" * 100
    got = spark.sql(f"SELECT {g.sql_hmac_sha256('actor_login', longa)} FROM ev_h WHERE actor_id = 583231")
    assert got.first()[0] == g.hmac_sha256_py("octocat", longa)


def test_tags_uc_geradas():
    sql = g.sql_tags_uc("main.bronze.gh_events", g.POLITICA_GH_EVENTS[:4])
    assert sql[3] == (
        "ALTER TABLE main.bronze.gh_events ALTER COLUMN actor_login "
        "SET TAGS ('classificacao' = 'confidencial', 'pii' = 'true');"
    )


def test_obter_chave_falha_sem_segredo(monkeypatch):
    monkeypatch.delenv("OSSLH_PSEUDO_KEY", raising=False)
    with pytest.raises(RuntimeError, match="OSSLH_PSEUDO_KEY"):
        g.obter_chave()
    assert g.obter_chave(permitir_demo=True) == g.DEMO_KEY
    monkeypatch.setenv("OSSLH_PSEUDO_KEY", "abc")
    assert g.obter_chave() == b"abc"


def test_esquecimento_so_some_do_disco_depois_de_purge_e_vacuum(spark, tmp_path):
    caminho = str(tmp_path / "eventos")
    linhas = [(i, "alvo" if i % 10 == 0 else f"u{i}") for i in range(200)]
    (
        spark.createDataFrame(linhas, "id int, login string")
        .coalesce(1)
        .write.format("delta")
        .option("delta.enableDeletionVectors", "true")
        .save(caminho)
    )
    m = g.esquecer_titular(spark, caminho, "login = 'alvo'")
    assert m["linhas_apagadas"] == 20 and m["deletion_vectors"] >= 1
    assert spark.read.format("delta").load(caminho).where("login = 'alvo'").count() == 0
    # versão 0 ainda tem o titular: time travel
    assert (
        spark.read.format("delta").option("versionAsOf", 0).load(caminho).where("login='alvo'").count() == 20
    )
    assert g.contar_no_parquet_bruto(spark, caminho, "login", "alvo") == 20  # arquivo antigo no disco
    g.vacuum_imediato(spark, caminho)
    assert g.contar_no_parquet_bruto(spark, caminho, "login", "alvo") == 0
    assert spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled") == "true"
    with pytest.raises(Exception):  # noqa: B017 - o tipo exato varia entre versões do Spark
        spark.read.format("delta").option("versionAsOf", 0).load(caminho).where(
            F.col("login") == "alvo"
        ).count()
