"""Streamlit dashboard: Recebimentos de Recursos por Favorecido over time.

Run from the repository root with `gov-spending dashboard`, or
`streamlit run src/gov_spending_analytics/dashboard/app.py`. The page reads the
DuckDB file built by dbt on short-lived read-only connections, so the scheduled
sync can rebuild it while the page is open. Interface text is in Brazilian
Portuguese for the public audience; code and docs stay in English.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from gov_spending_analytics.dashboard import queries
from gov_spending_analytics.dashboard.formatting import (
    MONTH_ABBREVIATIONS,
    amount_scale,
    format_brl,
    format_cnpj,
    format_number,
    month_label,
)

DATABASE_PATH = Path(os.environ.get("GOV_SPENDING_DUCKDB", "data/curated/gov_spending.duckdb"))
SYNC_MANIFEST_PATH = Path(
    os.environ.get("GOV_SPENDING_SYNC_MANIFEST", str(queries.DEFAULT_SYNC_MANIFEST_PATH))
)
DEFAULT_PERIOD_MONTHS = 13
SERIES_COLOR = "#2a78d6"
LABEL_INK_LIGHT = "#52514e"
LABEL_INK_DARK = "#c3c2b7"
SOURCE_URL = "https://portaldatransparencia.gov.br/download-de-dados/despesas-favorecidos"
REPOSITORY_URL = "https://github.com/phaiffer/public-spending-analytics"
KIND_LABELS = {
    "cnpj": "CNPJ (empresas e entidades)",
    "cpf_masked": "CPF mascarado (pessoas físicas)",
    "foreign_code": "Favorecido no exterior",
    "placeholder": "Sem identificação ou sigiloso",
    "other_code": "Outros códigos (ex.: unidades gestoras)",
}
COMPLETE_MONTH = "Mês completo"
OPEN_MONTH = "Mês em andamento (parcial)"
AXIS_LABEL_EXPR = "replace(format(datum.value, '.1~f'), '.', ',')"
AMOUNT_COLUMN = st.column_config.NumberColumn(format="localized")


@st.cache_data(ttl=600, show_spinner=False)
def run_query(name: str, *args: object):
    """Run one query on a short-lived read-only connection."""
    with duckdb.connect(str(DATABASE_PATH), read_only=True) as con:
        return getattr(queries, name)(con, *args)


def format_timestamp(value: str | None) -> str:
    if not value:
        return "data não registrada"
    moment = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    return moment.strftime("%d/%m/%Y às %H:%M UTC")


def render_header(freshness: queries.SourceFreshness) -> None:
    st.title("Recebimentos de recursos do Governo Federal")
    st.markdown(
        "Quanto o Governo Federal repassou a empresas, entidades e pessoas, mês a mês, "
        f"segundo os arquivos de [Recebimentos de Recursos por Favorecido]({SOURCE_URL}) "
        "do Portal da Transparência."
    )
    st.caption(
        f"Dados sincronizados com o Portal em {format_timestamp(freshness.last_retrieved_at)}."
    )


def render_filters(month_keys: list[int]) -> queries.DashboardFilters | None:
    period_column, body_column = st.columns([3, 2])

    if len(month_keys) == 1:
        start = end = month_keys[0]
        period_column.markdown(f"**Período:** {month_label(start)}")
    else:
        default_start = month_keys[max(0, len(month_keys) - DEFAULT_PERIOD_MONTHS)]
        start, end = period_column.select_slider(
            "Período",
            options=month_keys,
            value=(default_start, month_keys[-1]),
            format_func=month_label,
        )

    bodies = run_query("superior_bodies")
    body_names = dict(zip(bodies["body_id"], bodies["body_name"], strict=True))
    selected_bodies = body_column.multiselect(
        "Órgão superior",
        options=list(body_names),
        format_func=body_names.get,
        placeholder="Todos os órgãos",
    )
    selected_kinds = st.pills(
        "Tipo de favorecido",
        options=list(KIND_LABELS),
        default=list(KIND_LABELS),
        format_func=KIND_LABELS.get,
        selection_mode="multi",
    )
    if not selected_kinds:
        st.info("Selecione ao menos um tipo de favorecido.")
        return None
    return queries.DashboardFilters(
        start_month_key=start,
        end_month_key=end,
        superior_body_ids=tuple(selected_bodies),
        identifier_kinds=tuple(selected_kinds),
    )


def render_kpis(filters: queries.DashboardFilters, open_months: set[int]) -> None:
    totals = run_query("headline_totals", filters)
    in_period = range(filters.start_month_key, filters.end_month_key + 1)
    partial = sum(1 for key in open_months if key in in_period)
    month_text = format_number(totals["month_count"])
    if partial:
        month_text += f" ({partial} parcial)"

    total_column, records_column, negative_column, months_column = st.columns(4)
    total_column.metric("Total recebido", format_brl(totals["total_amount_brl"]))
    records_column.metric("Lançamentos", format_number(totals["record_count"]))
    negative_column.metric(
        "Lançamentos negativos",
        format_number(totals["negative_record_count"]),
        help="Estornos e ajustes publicados com valor negativo; eles reduzem o total.",
    )
    months_column.metric("Meses com dados", month_text)


def time_axis(month_keys: pd.Series) -> alt.Axis:
    """Year ticks on Januaries for multi-year periods, pt-BR month ticks otherwise."""
    first_year, last_year = int(month_keys.min() // 100), int(month_keys.max() // 100)
    if first_year == last_year:
        month_names = str(list(MONTH_ABBREVIATIONS))
        return alt.Axis(
            labelExpr=f"{month_names}[month(datum.value)] + '/' + year(datum.value)",
            labelAngle=0,
            grid=False,
        )
    years = range(first_year + 1, last_year + 1)
    step = 2 if len(years) > 10 else 1
    return alt.Axis(
        values=[alt.DateTime(year=year, month=1, date=1) for year in years[::step]],
        format="%Y",
        labelAngle=0,
        grid=False,
    )


def monthly_chart(monthly: pd.DataFrame, open_months: set[int]) -> alt.Chart:
    """One bar per month on a time axis, so months missing locally show as gaps."""
    divisor, unit = amount_scale(monthly["total_amount_brl"].abs().max())
    data = monthly.assign(
        amount=monthly["total_amount_brl"] / divisor,
        month=monthly["launch_month_key"].map(month_label),
        total_text=monthly["total_amount_brl"].map(format_brl),
        records_text=monthly["record_count"].map(format_number),
        status=monthly["launch_month_key"].map(
            lambda key: OPEN_MONTH if key in open_months else COMPLETE_MONTH
        ),
    )
    return (
        alt.Chart(data)
        .mark_bar(color=SERIES_COLOR, cornerRadiusTopLeft=2, cornerRadiusTopRight=2)
        .encode(
            x=alt.X(
                "yearmonth(launch_month_start_date):T",
                title=None,
                axis=time_axis(monthly["launch_month_key"]),
            ),
            y=alt.Y("amount:Q", title=unit, axis=alt.Axis(labelExpr=AXIS_LABEL_EXPR)),
            opacity=alt.condition(
                alt.datum.status == COMPLETE_MONTH, alt.value(1.0), alt.value(0.4)
            ),
            tooltip=[
                alt.Tooltip("month:N", title="Mês"),
                alt.Tooltip("total_text:N", title="Total recebido"),
                alt.Tooltip("records_text:N", title="Lançamentos"),
                alt.Tooltip("status:N", title="Situação"),
            ],
        )
        .properties(height=320)
    )


def label_ink() -> str:
    """Secondary text color for the active theme; Vega's default text is always black."""
    theme = getattr(st.context, "theme", None)
    return LABEL_INK_DARK if theme is not None and theme.type == "dark" else LABEL_INK_LIGHT


def ranking_chart(data: pd.DataFrame, label_column: str, label_title: str) -> alt.Chart:
    """Horizontal bars, largest first, with the formatted total at each bar end."""
    divisor, unit = amount_scale(data["total_amount_brl"].abs().max())
    data = data.assign(
        amount=data["total_amount_brl"] / divisor,
        total_text=data["total_amount_brl"].map(format_brl),
    )
    y = alt.Y(f"{label_column}:N", sort="-x", title=None, axis=alt.Axis(labelLimit=260))
    bars = (
        alt.Chart(data)
        .mark_bar(color=SERIES_COLOR, cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            x=alt.X("amount:Q", title=unit, axis=alt.Axis(labelExpr=AXIS_LABEL_EXPR)),
            y=y,
            tooltip=[
                alt.Tooltip(f"{label_column}:N", title=label_title),
                alt.Tooltip("total_text:N", title="Total recebido"),
            ],
        )
    )
    labels = alt.Chart(data).mark_text(align="left", dx=4, color=label_ink()).encode(
        x="amount:Q", y=y, text="total_text:N"
    )
    return (bars + labels).properties(height=alt.Step(28))


def render_monthly(filters: queries.DashboardFilters, open_months: set[int]) -> None:
    st.subheader("Total recebido por mês")
    monthly = run_query("monthly_totals", filters)
    st.altair_chart(monthly_chart(monthly, open_months), width="stretch")
    if open_months & set(monthly["launch_month_key"]):
        st.caption("Barras mais claras: mês em andamento, com dados parciais.")
    with st.expander("Ver tabela mensal"):
        table = pd.DataFrame(
            {
                "Mês": monthly["launch_month_key"].map(month_label),
                "Total recebido (R$)": monthly["total_amount_brl"],
                "Lançamentos": monthly["record_count"],
                "Lançamentos negativos": monthly["negative_record_count"],
            }
        )
        st.dataframe(
            table,
            hide_index=True,
            width="stretch",
            column_config={"Total recebido (R$)": AMOUNT_COLUMN},
        )


def render_breakdowns(filters: queries.DashboardFilters) -> None:
    body_column, kind_column = st.columns(2)

    with body_column:
        st.subheader("Maiores órgãos superiores")
        bodies = run_query("body_ranking", filters)
        st.altair_chart(ranking_chart(bodies, "body_name", "Órgão"), width="stretch")

    with kind_column:
        st.subheader("Por tipo de favorecido")
        kinds = run_query("kind_breakdown", filters)
        kinds = kinds.assign(kind_label=kinds["identifier_kind"].map(KIND_LABELS))
        st.altair_chart(ranking_chart(kinds, "kind_label", "Tipo"), width="stretch")


def render_organizations(filters: queries.DashboardFilters) -> None:
    st.subheader("Maiores organizações favorecidas")
    st.caption(
        "Somente CNPJs e favorecidos no exterior; pessoas físicas não são listadas. "
        "Este ranking segue o período e o tipo de favorecido, não o filtro de órgão."
    )
    organizations = run_query("top_organizations", filters)
    if organizations.empty:
        st.info("Nenhuma organização entre os tipos de favorecido selecionados.")
        return
    table = pd.DataFrame(
        {
            "Favorecido": organizations["beneficiary_name"],
            "CNPJ ou código": organizations["beneficiary_id"].map(format_cnpj),
            "Tipo": organizations["identifier_kind"].map(KIND_LABELS),
            "Total recebido (R$)": organizations["total_amount_brl"],
            "Meses": organizations["month_count"],
        }
    )
    st.dataframe(
        table,
        hide_index=True,
        width="stretch",
        column_config={"Total recebido (R$)": AMOUNT_COLUMN},
    )


def render_notes() -> None:
    with st.expander("Sobre os dados"):
        st.markdown(
            f"""
- **Fonte:** arquivos mensais de Recebimentos de Recursos por Favorecido do
  [Portal da Transparência]({SOURCE_URL}). O Portal atualiza o mês corrente todos os
  dias e às vezes republica meses passados; cada versão baixada é registrada por
  SHA-256 antes de entrar nos números.
- **Valores com sinal:** estornos aparecem como lançamentos negativos e reduzem o total.
- **Mês em andamento:** a barra mais clara marca um mês que ainda recebia lançamentos
  quando foi baixado; o valor final tende a mudar.
- **Pessoas físicas:** o Portal publica CPFs mascarados (`***.123.456-**`), e um mesmo
  CPF mascarado pode corresponder a pessoas diferentes. Por isso o painel não lista
  pessoas, apenas totais.
- **Sem identificação ou sigiloso:** códigos como SEM INFORMACAO, NAO SE APLICA,
  Sigiloso e FOLHA DE PAGAMENTO, que não identificam um favorecido.
- **Código e metodologia:** [{REPOSITORY_URL.removeprefix("https://")}]({REPOSITORY_URL}).
"""
        )


def main() -> None:
    st.set_page_config(page_title="Recebimentos do Governo Federal", layout="wide")
    if not DATABASE_PATH.exists():
        st.error(
            f"Base não encontrada em {DATABASE_PATH}. Rode `gov-spending sync-recebimentos "
            "--dbt-build` para baixar os dados e construir a base."
        )
        st.stop()
    try:
        month_keys = run_query("available_month_keys")
    except duckdb.Error:
        st.error(
            "Não foi possível ler a base agora. Ela pode estar sendo atualizada; "
            "tente novamente em alguns minutos."
        )
        st.stop()
    if not month_keys:
        st.warning("A base ainda não tem meses carregados.")
        st.stop()

    freshness = queries.source_freshness(SYNC_MANIFEST_PATH)
    open_months = set(freshness.open_month_keys)

    render_header(freshness)
    filters = render_filters(month_keys)
    if filters is None:
        st.stop()
    render_kpis(filters, open_months)
    render_monthly(filters, open_months)
    render_breakdowns(filters)
    render_organizations(filters)
    render_notes()


main()
