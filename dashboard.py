import streamlit as st
import redis
import pandas as pd
import altair as alt
from streamlit_autorefresh import st_autorefresh
from datetime import datetime
import json

st.set_page_config(page_title="Real-Time Shopping Dashboard", layout="wide")

st.markdown("""
    <style>
        .big-title {
            font-size: 32px !important;
            font-weight: bold;
            margin-top: 20px;
            margin-bottom: 10px;
        }
    </style>
""", unsafe_allow_html=True)

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

st.title("Real-Time Shopping Dashboard")
st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")

tabs = st.tabs([
    "Top Purchased Products",
    "Abandoned Carts",
    "Unconverted Views",
    "Top Removed from Cart",
    "Most Active Users"
])

with tabs[0]:
    st.markdown('<div class="big-title">Top Purchased Products</div>', unsafe_allow_html=True)

    def get_top_products():
        keys = r.keys("top_products:*")
        data = []
        for key in keys:
            product_id = key.split(":")[1]
            count = r.get(key)
            if count is not None:
                data.append((product_id, int(count)))
        return sorted(data, key=lambda x: x[1], reverse=True)

    top_n = 25
    data = get_top_products()
    df = pd.DataFrame(data, columns=["Product ID", "Purchases"]).head(top_n)

    st.dataframe(df, use_container_width=True)

    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X("Product ID:N", sort="-y"),
        y="Purchases:Q",
        tooltip=["Product ID", "Purchases"]
    ).properties(
        width=1200,
        height=500
    )

    st.altair_chart(chart, use_container_width=True)

with tabs[1]:
    st.markdown('<div class="big-title">Abandoned Carts</div>', unsafe_allow_html=True)

    PAGE_SIZE = 10
    total_sessions = r.llen("abandoned_sessions")
    max_pages = max(1, (total_sessions + PAGE_SIZE - 1) // PAGE_SIZE)

    current_page = st.number_input("Page", min_value=0, max_value=max_pages - 1, step=1, value=0)

    start = current_page * PAGE_SIZE
    end = start + PAGE_SIZE - 1

    session_ids = r.lrange("abandoned_sessions", start, end)
    results = []

    for sid in session_ids:
        raw = r.get(f"abandoned:{sid}")
        if raw:
            try:
                products = json.loads(raw)
                results.append((sid, products))
            except:
                continue

    if results:
        for session_id, products in results:
            with st.expander(f"Session: {session_id}"):
                st.write(f"Abandoned products: {', '.join(products)}")
    else:
        st.info("No abandoned sessions found on this page.")

with tabs[2]:
    st.markdown('<div class="big-title">Unconverted Product Views</div>', unsafe_allow_html=True)

    st.write("Products viewed multiple times but not added to cart.")
    user_id = st.text_input("Enter user ID to fetch product suggestions:", "")

    if user_id:
        keys = r.keys(f"views:{user_id}:*")
        results = []

        for k in keys:
            parts = k.split(":")
            if len(parts) != 3:
                continue

            _, _, product_id = parts
            views = int(r.get(k) or 0)
            was_added = r.exists(f"added:{user_id}:{product_id}")

            if views >= 3 and not was_added:
                results.append((product_id, views))

        if results:
            st.success(f"Found {len(results)} products for user '{user_id}':")
            for pid, count in results:
                st.write(f"Product '{pid}' was viewed {count} times but not added to cart.")
        else:
            st.info("No relevant products found for this user.")

with tabs[3]:
    st.markdown('<div class="big-title">Top Removed from Cart</div>', unsafe_allow_html=True)

    def get_removed_products():
        keys = r.keys("removed:*")
        data = []
        for key in keys:
            product_id = key.split(":")[1]
            count = r.get(key)
            if count is not None:
                data.append((product_id, int(count)))
        return sorted(data, key=lambda x: x[1], reverse=True)

    removed = get_removed_products()
    df_removed = pd.DataFrame(removed, columns=["Product ID", "Removals"]).head(25)

    st.dataframe(df_removed, use_container_width=True)

    if not df_removed.empty:
        chart2 = alt.Chart(df_removed).mark_bar().encode(
            x=alt.X("Product ID:N", sort="-y"),
            y="Removals:Q",
            tooltip=["Product ID", "Removals"]
        ).properties(
            width=1200,
            height=500
        )
        st.altair_chart(chart2, use_container_width=True)
    else:
        st.info("No product removal data available.")

with tabs[4]:
    st.markdown('<div class="big-title">Most Active Users</div>', unsafe_allow_html=True)

    def get_top_active_users():
        keys = r.keys("user_score:*")
        scores = []

        for key in keys:
            user_id = key.split(":")[1]
            score = int(r.get(key) or 0)
            event_data = r.get(f"user_events:{user_id}")
            try:
                events = json.loads(event_data) if event_data else {}
            except:
                events = {}

            scores.append({
                "User ID": user_id,
                "Score": score,
                "Views": events.get("view", 0),
                "Cart Adds": events.get("cart", 0),
                "Purchases": events.get("purchase", 0),
                "Removals": events.get("remove_from_cart", 0)
            })

        sorted_users = sorted(scores, key=lambda x: x["Score"], reverse=True)[:15]
        return pd.DataFrame(sorted_users)

    df_users = get_top_active_users()

    if not df_users.empty:
        st.dataframe(df_users, use_container_width=True)
    else:
        st.info("No active users found.")

st_autorefresh(interval=5000, limit=None, key="refresh")
