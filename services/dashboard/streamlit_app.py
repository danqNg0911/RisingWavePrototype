import os

import pandas as pd
import plotly.express as px
import psycopg
import streamlit as st


DSN = os.getenv("RISINGWAVE_DSN", "postgresql://root@risingwave:4566/dev")


st.set_page_config(page_title="RisingWave AI/RPA Prototype", layout="wide")
st.title("RisingWave AI/RPA Prototype Dashboard")
st.caption("Nexmark-style streaming data with AI scoring and RPA action simulation.")


def query_df(query: str) -> pd.DataFrame:
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc.name for desc in cur.description]
    return pd.DataFrame(rows, columns=columns)


def metric_value(query: str, column: str) -> str:
    df = query_df(query)
    if df.empty:
        return "0"
    value = df.iloc[0][column]
    if value is None:
        return "0"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


try:
    overview_1, overview_2, overview_3, overview_4, overview_5 = st.columns(5)
    overview_1.metric("Transactions", metric_value("SELECT COUNT(*) AS value FROM clean_transactions", "value"))
    overview_2.metric("Risk Candidates", metric_value("SELECT COUNT(*) AS value FROM risk_candidates", "value"))
    overview_3.metric("AI Scored", metric_value("SELECT COUNT(*) AS value FROM ai_scored_events", "value"))
    overview_4.metric("RPA Decisions", metric_value("SELECT COUNT(*) AS value FROM rpa_decisions", "value"))
    overview_5.metric(
        "Workflow Success Rate",
        metric_value(
            """
            SELECT
                COALESCE(
                    SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END)::DOUBLE PRECISION
                    / NULLIF(COUNT(*), 0),
                    0
                ) AS value
            FROM workflow_audit_log
            """,
            "value",
        ),
    )

    tab_overview, tab_actions, tab_audit = st.tabs(["Overview", "Actions", "Audit"])

    with tab_overview:
        latency = query_df(
            """
            SELECT AVG(EXTRACT(EPOCH FROM (decision_time - event_time))) AS avg_latency_seconds
            FROM rpa_decisions
            """
        )
        st.subheader("Latency")
        st.dataframe(latency, use_container_width=True)

        latest = query_df(
            """
            SELECT
                transaction_id,
                user_id,
                merchant_name,
                amount,
                final_risk_score,
                rpa_action,
                decision_time
            FROM rpa_decisions
            ORDER BY decision_time DESC
            LIMIT 20
            """
        )
        st.subheader("Latest Decisions")
        st.dataframe(latest, use_container_width=True)

    with tab_actions:
        action_counts = query_df(
            """
            SELECT rpa_action, COUNT(*) AS total
            FROM rpa_decisions
            GROUP BY rpa_action
            ORDER BY total DESC, rpa_action
            """
        )
        if not action_counts.empty:
            fig = px.bar(action_counts, x="rpa_action", y="total", title="RPA Actions")
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(action_counts, use_container_width=True)

        risky_users = query_df(
            """
            SELECT
                user_id,
                COUNT(*) AS risky_events,
                AVG(final_risk_score) AS avg_risk_score,
                MAX(final_risk_score) AS max_risk_score
            FROM rpa_decisions
            GROUP BY user_id
            ORDER BY risky_events DESC
            LIMIT 10
            """
        )
        st.subheader("Top Risky Users")
        st.dataframe(risky_users, use_container_width=True)

    with tab_audit:
        audit_counts = query_df(
            """
            SELECT status, COUNT(*) AS total
            FROM workflow_audit_log
            GROUP BY status
            ORDER BY total DESC, status
            """
        )
        if not audit_counts.empty:
            fig = px.pie(audit_counts, names="status", values="total", title="Workflow Status")
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(audit_counts, use_container_width=True)

        failures = query_df(
            """
            SELECT
                action_id,
                transaction_id,
                rpa_action,
                status,
                retry_count,
                processed_at,
                error_message
            FROM workflow_audit_log
            ORDER BY processed_at DESC NULLS LAST, created_at DESC
            LIMIT 25
            """
        )
        st.subheader("Recent Audit Events")
        st.dataframe(failures, use_container_width=True)
except Exception as exc:
    st.error(f"Dashboard query failed: {exc}")
    st.info("Start infrastructure and run .\\scripts\\init-sql.ps1 before opening the dashboard.")
