from __future__ import annotations

import base64
import os
import folium
import pandas as pd
import streamlit as st
from folium.plugins import Fullscreen, MarkerCluster

from data_generator import CITY_CENTER, seed_database
from database import (
    add_fuel,
    assign_owner_to_stations,
    authenticate_user,
    create_station_with_inventory,
    create_user,
    disable_fuel_type,
    enable_fuel_type,
    get_admin_overview,
    get_all_users,
    get_area_fuel_mix,
    get_areas,
    get_average_price_per_area,
    get_daily_demand_trend,
    get_hourly_demand_pattern,
    get_nearby_stations,
    get_owner_station_ids,
    get_owner_summary,
    get_price_spread_by_area,
    get_revenue_estimate_per_station,
    get_station_inventory_row,
    get_station_fuel_types,
    get_station_lookup,
    get_stations_inventory_view,
    get_stockout_risk_report_dynamic,
    get_total_demand_per_station,
    get_inventory_turnover_report,
    get_demand_growth_by_station,
    username_exists,
    subtract_fuel,
    update_price,
)
from utils import recommend_stations

st.set_page_config(page_title="Fuel Demand & Inventory Platform", layout="wide", initial_sidebar_state="collapsed")
ADMIN_SIGNUP_CODE = os.getenv("ADMIN_SIGNUP_CODE", "ADMIN2026")


@st.cache_data(show_spinner=False)
def _seed_once() -> str:
    seed_database(force_reset=False, use_real_data=False, transaction_rows=50000)
    return "ok"


def _format_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        if any(x in col.lower() for x in ["price", "liters", "distance", "score"]):
            out[col] = out[col].map(lambda x: round(float(x), 2) if pd.notna(x) else x)
    return out


def _fmt_number(value: float | int, precision: int = 2) -> str:
    try:
        return f"{float(value):,.{precision}f}"
    except Exception:
        return str(value)


def _show_df(df: pd.DataFrame) -> None:
    st.dataframe(_format_dataframe(df), use_container_width=True, hide_index=True)


def _render_kpi_row(cards: list[tuple[str, str, str]]) -> None:
    cols = st.columns(len(cards))
    for col, (label, value, hint) in zip(cols, cards):
        with col:
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="kpi-label">{label}</div>
                    <div class="kpi-value">{value}</div>
                    <div class="kpi-hint">{hint}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _section_card_start() -> None:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)


def _section_card_end() -> None:
    st.markdown("</div>", unsafe_allow_html=True)


def _role_hero(role: str) -> None:
    role_map = {
        "fuel_user": (
            "Fuel User Workspace",
            "Find the best nearby stations with live price and stock intelligence.",
            "https://images.unsplash.com/photo-1493238792000-8113da705763?auto=format&fit=crop&w=1600&q=80",
        ),
        "station_owner": (
            "Station Owner Workspace",
            "Manage operations, reduce stockout risk, and optimize revenue.",
            "https://images.unsplash.com/photo-1464802686167-b939a6910659?auto=format&fit=crop&w=1600&q=80",
        ),
        "admin": (
            "Admin Command Center",
            "Monitor city-wide demand, pricing strategy, and network performance.",
            "https://images.unsplash.com/photo-1469474968028-56623f02e42e?auto=format&fit=crop&w=1600&q=80",
        ),
    }
    title, subtitle, img = role_map.get(role, role_map["admin"])
    st.markdown(
        f"""
        <div class="hero-panel" style="background-image: linear-gradient(90deg, rgba(7,36,69,0.78), rgba(12,69,119,0.56)), url('{img}');">
            <div class="hero-title">{title}</div>
            <div class="hero-sub">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _top_app_bar(user: dict) -> bool:
    role_text = str(user["role"]).replace("_", " ").title()
    c1, c2 = st.columns([8, 2])
    with c1:
        st.markdown(
            f"""
            <div class="top-appbar">
                <div class="top-appbar-title">FuelOps Control Center</div>
                <div class="top-appbar-sub">{user['username']} • {role_text}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown('<div style="height: 1cm;"></div>', unsafe_allow_html=True)
        return st.button("Logout", key="top_logout_btn", use_container_width=True)


def _landing_hero_image() -> None:
    local_path = os.path.join(os.path.dirname(__file__), "assets", "landing_hero.jpg")
    if os.path.exists(local_path):
        st.image(local_path, use_container_width=True)
    else:
        st.image(
            "https://images.unsplash.com/photo-1553095066-5014bc7b7f2d?auto=format&fit=crop&w=1600&q=80",
            use_container_width=True,
        )


def _landing_image_base64() -> str:
    local_path = os.path.join(os.path.dirname(__file__), "assets", "landing_hero.jpg")
    if os.path.exists(local_path):
        with open(local_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    return ""


def _page_header(title: str, subtitle: str, chip: str) -> None:
    st.markdown(f'<span class="page-chip">{chip}</span>', unsafe_allow_html=True)
    st.title(title)
    st.markdown(f'<p class="page-note">{subtitle}</p>', unsafe_allow_html=True)


def _role_nav(options: list[str], key: str, label: str = "Navigation") -> str:
    return st.segmented_control(
        label,
        options=options,
        default=options[0],
        key=key,
        selection_mode="single",
    )


def _apply_ui_theme() -> None:
    st.markdown(
        """
        <style>
        :root {
            --bg: #eef4fb;
            --bg-soft: #edf3fb;
            --surface: #ffffff;
            --text: #12324f;
            --muted: #5c7590;
            --primary: #1f6feb;
            --primary-soft: #dce9ff;
            --border: #d5e3f3;
            --shadow: 0 12px 30px rgba(19, 54, 92, 0.10);
        }
        .stApp {
            background:
                radial-gradient(circle at 0% 0%, #ffffff 0%, #f4f8fe 40%, #e9f1fb 100%) !important;
            color: var(--text);
        }
        [data-testid="stAppViewContainer"] { background: transparent !important; }
        .block-container { padding-top: 0.8rem; max-width: 1280px; }
        .app-card {
            background: var(--surface); border: 1px solid var(--border); border-radius: 14px;
            padding: 14px 18px; margin-bottom: 12px;
            box-shadow: var(--shadow);
        }
        .top-appbar {
            background: linear-gradient(135deg, #ffffff 0%, #f0f6ff 100%);
            border: 1px solid #d5e4f7;
            border-radius: 14px;
            padding: 10px 14px;
            margin-bottom: 12px;
            box-shadow: 0 8px 22px rgba(20, 53, 91, 0.08);
        }
        .top-appbar-title {
            font-size: 1.15rem;
            font-weight: 800;
            color: #0f355a;
            line-height: 1.2;
        }
        .top-appbar-sub {
            font-size: 0.9rem;
            color: #5d7895;
            margin-top: 2px;
        }
        .hero-panel {
            border-radius: 16px;
            padding: 20px 24px;
            margin-bottom: 12px;
            background-size: cover;
            background-position: center;
            box-shadow: 0 14px 28px rgba(19, 54, 92, 0.2);
        }
        .hero-title {
            color: #ffffff;
            font-size: 1.55rem;
            font-weight: 800;
            margin-bottom: 4px;
        }
        .hero-sub {
            color: #e8f3ff;
            font-size: 0.95rem;
            font-weight: 500;
        }
        .section-card {
            background: #ffffff;
            border: 1px solid #d4e3f5;
            border-radius: 14px;
            padding: 14px 14px 8px 14px;
            margin-bottom: 14px;
            box-shadow: 0 8px 20px rgba(19, 54, 92, 0.08);
            animation: fadeInUp .28s ease-out;
        }
        .auth-title {
            text-align: center;
            margin-bottom: 0.3rem;
            font-size: clamp(1.7rem, 2.8vw, 2.4rem);
            font-weight: 800;
            color: #0f355a;
            letter-spacing: 0.2px;
        }
        .auth-sub {
            text-align: center;
            color: var(--muted);
            margin-top: 0;
            margin-bottom: 1rem;
            font-size: 1.05rem;
        }
        .auth-panel {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 18px 22px;
            box-shadow: var(--shadow);
        }
        .auth-hero-wrap {
            border-radius: 14px;
            overflow: hidden;
            border: 1px solid #d4e3f5;
            box-shadow: 0 10px 24px rgba(19, 54, 92, 0.12);
            margin-bottom: 12px;
        }
        h1, h2, h3 { color: var(--text); }

        /* Sidebar: remove black look */
        [data-testid="stSidebar"] { display: none !important; }

        /* Tabs */
        .stTabs [role="tablist"] {
            gap: 0.4rem;
            background: #f4f8ff;
            padding: 0.35rem;
            border-radius: 12px;
            border: 1px solid var(--border);
        }
        .stTabs [role="tab"] {
            border-radius: 9px;
            color: #345877;
            background: transparent;
            padding: 0.45rem 0.9rem;
            font-weight: 600;
        }
        .stTabs [aria-selected="true"] {
            background: #ffffff !important;
            color: var(--primary) !important;
            border: 1px solid var(--primary-soft) !important;
        }
        [data-testid="stSegmentedControl"] {
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 0.35rem;
            margin-bottom: 1rem;
            box-shadow: 0 4px 12px rgba(21, 52, 84, 0.05);
        }
        [data-testid="stSegmentedControl"] label { font-weight: 700; color: #1d4d7d; }

        /* Inputs: remove dark fields */
        .stTextInput input,
        .stNumberInput input,
        .stSelectbox [data-baseweb="select"] > div,
        .stMultiSelect [data-baseweb="select"] > div,
        .stTextArea textarea {
            background: #ffffff !important;
            color: #153a5d !important;
            border: 1px solid #c7d9ef !important;
            border-radius: 10px !important;
        }
        .stTextInput input:focus,
        .stNumberInput input:focus,
        .stTextArea textarea:focus {
            border-color: #1f6feb !important;
            box-shadow: 0 0 0 2px rgba(31, 111, 235, 0.15) !important;
        }

        /* Buttons */
        .stButton button,
        .stFormSubmitButton button {
            background: linear-gradient(135deg, #1f6feb, #2e82ff) !important;
            color: #ffffff !important;
            border: none !important;
            border-radius: 10px !important;
            font-weight: 700 !important;
            padding: 0.5rem 1rem !important;
            box-shadow: 0 6px 16px rgba(31, 111, 235, 0.25);
        }
        .stButton button:hover,
        .stFormSubmitButton button:hover {
            filter: brightness(1.05);
            transform: translateY(-1px);
        }

        /* Data/table polish */
        [data-testid="stDataFrame"] {
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
        }
        .page-chip {
            display: inline-block;
            padding: 0.25rem 0.6rem;
            border-radius: 999px;
            background: #e9f1ff;
            color: #2a5e9a;
            font-size: 0.8rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
        }
        .page-note {
            color: #55718f;
            margin-top: -0.2rem;
            margin-bottom: 1rem;
        }
        .kpi-card {
            background: #ffffff;
            border: 1px solid #d3e2f4;
            border-radius: 14px;
            padding: 10px 12px;
            min-height: 96px;
            box-shadow: 0 5px 14px rgba(31, 111, 235, 0.08);
        }
        .kpi-label {
            font-size: 0.82rem;
            color: #5d7895;
            font-weight: 700;
            margin-bottom: 0.2rem;
        }
        .kpi-value {
            font-size: 1.6rem;
            color: #133a61;
            font-weight: 800;
            line-height: 1.1;
            margin-bottom: 0.15rem;
        }
        .kpi-hint {
            font-size: 0.75rem;
            color: #7c92aa;
        }
        .sidebar-profile {
            background: rgba(255, 255, 255, 0.75);
            border: 1px solid #c8dcf4;
            border-radius: 12px;
            padding: 10px 12px;
            margin-bottom: 10px;
            color: #173a5e;
            font-size: 0.92rem;
        }
        @keyframes fadeInUp {
            from { opacity: 0; transform: translateY(4px); }
            to { opacity: 1; transform: translateY(0); }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_station_map(df: pd.DataFrame, user_lat: float | None = None, user_lon: float | None = None) -> None:
    if df.empty:
        st.info("No station points available for map.")
        return

    center_lat = float(df["latitude"].mean()) if user_lat is None else user_lat
    center_lon = float(df["longitude"].mean()) if user_lon is None else user_lon
    fmap = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles="CartoDB Voyager")
    Fullscreen(position="topright").add_to(fmap)
    cluster = MarkerCluster(name="Fuel Stations").add_to(fmap)

    max_stock = max(float(df["available_liters"].max()), 1.0)
    min_stock = float(df["available_liters"].min())

    if user_lat is not None and user_lon is not None:
        folium.CircleMarker(
            [user_lat, user_lon],
            radius=8,
            color="#0b63d8",
            fill=True,
            fill_color="#0b63d8",
            fill_opacity=0.95,
            popup="Your Location",
        ).add_to(fmap)

        if "distance_km" in df.columns and not df["distance_km"].isna().all():
            search_radius = float(max(df["distance_km"].max(), 0.5))
            folium.Circle(
                [user_lat, user_lon],
                radius=search_radius * 1000.0,
                color="#2e82ff",
                fill=True,
                fill_opacity=0.08,
                weight=1.5,
                tooltip=f"Search radius ~ {search_radius:.1f} km",
            ).add_to(fmap)

    for _, row in df.iterrows():
        stock = float(row["available_liters"])
        if stock < 200:
            color = "#d92d20"
        elif stock < 800:
            color = "#f79009"
        else:
            color = "#16a34a"

        radius = 5 + (9 * ((stock - min_stock) / (max_stock - min_stock + 1e-9)))
        popup = (
            f"<b>{row['name']}</b><br>"
            f"Area: {row['area']}<br>"
            f"Fuel: {row['fuel_type']}<br>"
            f"Stock: {stock:.2f} L<br>"
            f"Price: {float(row['price']):.2f}"
        )
        if "distance_km" in row and pd.notna(row.get("distance_km")):
            popup += f"<br>Distance: {float(row['distance_km']):.2f} km"

        folium.CircleMarker(
            [float(row["latitude"]), float(row["longitude"])],
            popup=folium.Popup(popup, max_width=350),
            radius=float(radius),
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            tooltip=f"{row['name']} | {row['fuel_type']}",
        ).add_to(cluster)
    st.components.v1.html(fmap._repr_html_(), height=600, scrolling=True)


def _login_screen() -> None:
    b64 = _landing_image_base64()
    if b64:
        st.markdown(
            f"""
            <style>
            .stApp {{
                background:
                    linear-gradient(rgba(11, 34, 58, 0.56), rgba(11, 34, 58, 0.56)),
                    url("data:image/jpeg;base64,{b64}") center/cover no-repeat fixed !important;
            }}
            .auth-panel {{
                position: relative;
                overflow: hidden;
                background: linear-gradient(165deg, rgba(8, 31, 56, 0.78), rgba(10, 42, 74, 0.70)) !important;
                backdrop-filter: blur(8px);
                border: 1px solid rgba(185, 214, 245, 0.45) !important;
                box-shadow: 0 18px 34px rgba(8, 27, 46, 0.42) !important;
                border-radius: 18px !important;
                animation: authFade .35s ease-out;
            }}
            .auth-panel::before {{
                content: "";
                position: absolute;
                inset: 0;
                background: radial-gradient(circle at 100% 0%, rgba(124, 188, 255, 0.22), transparent 45%);
                pointer-events: none;
            }}
            .auth-panel [data-testid="stWidgetLabel"] p strong,
            .auth-panel p strong {{
                color: #ffffff !important;
                font-weight: 800 !important;
                letter-spacing: 0.2px;
            }}
            .auth-field-label {{
                color: #ffffff !important;
                font-weight: 800 !important;
                margin: 6px 0 4px 0 !important;
                letter-spacing: 0.2px;
            }}
            .auth-panel [data-testid="stWidgetLabel"] p,
            .auth-panel [data-testid="stWidgetLabel"] label,
            .auth-panel label,
            .auth-panel small,
            .auth-panel .stCaption {{
                color: #d8eaff !important;
                font-weight: 700 !important;
                opacity: 1 !important;
            }}
            .auth-panel .stTextInput input,
            .auth-panel .stNumberInput input,
            .auth-panel .stSelectbox [data-baseweb="select"] > div,
            .auth-panel .stMultiSelect [data-baseweb="select"] > div {{
                background: rgba(255, 255, 255, 0.95) !important;
                border: 1px solid #8cb5df !important;
                color: #0e3358 !important;
            }}
            .auth-panel .stTextInput input::placeholder,
            .auth-panel .stNumberInput input::placeholder {{
                color: #6e87a0 !important;
                opacity: 1;
            }}
            .auth-panel .stTabs [role="tablist"] {{
                background: rgba(239, 246, 255, 0.16) !important;
                border-color: rgba(183, 214, 248, 0.45) !important;
            }}
            .auth-panel .stTabs [role="tab"] {{
                color: #d7eaff !important;
                font-weight: 700 !important;
            }}
            .auth-panel .stTabs [aria-selected="true"] {{
                color: #ffffff !important;
                background: linear-gradient(135deg, #1f6feb, #2e82ff) !important;
                border: none !important;
                box-shadow: 0 6px 16px rgba(31, 111, 235, 0.34);
            }}
            .auth-panel .stFormSubmitButton button {{
                background: linear-gradient(135deg, #1f6feb, #2e82ff) !important;
                border: 1px solid rgba(167, 206, 249, 0.55) !important;
            }}
            .auth-panel .stExpander {{
                border-color: rgba(183, 214, 248, 0.4) !important;
                background: rgba(255,255,255,0.06) !important;
            }}
            @keyframes authFade {{
                from {{
                    opacity: 0;
                    transform: translateY(6px);
                }}
                to {{
                    opacity: 1;
                    transform: translateY(0);
                }}
            }}
            .auth-title, .auth-sub {{
                color: #ffffff !important;
                text-shadow: 0 2px 8px rgba(0,0,0,0.45);
            }}
            .auth-sub {{
                font-weight: 600 !important;
                letter-spacing: 0.1px;
            }}
            </style>
            """,
            unsafe_allow_html=True,
        )

    left, center, right = st.columns([1, 2, 1])
    with center:
        st.markdown('<h1 class="auth-title">Fuel Station Demand & Inventory Platform</h1>', unsafe_allow_html=True)
        st.markdown('<p class="auth-sub">Secure access for Fuel Users, Station Owners, and Admins</p>', unsafe_allow_html=True)
        st.markdown('<div class="auth-panel">', unsafe_allow_html=True)

        login_tab, signup_tab = st.tabs(["Login", "Sign Up"])

        with login_tab:
            with st.form("login_form"):
                st.markdown('<div class="auth-field-label">Username</div>', unsafe_allow_html=True)
                username = st.text_input("Username", label_visibility="collapsed")
                st.markdown('<div class="auth-field-label">Password</div>', unsafe_allow_html=True)
                password = st.text_input("Password", type="password", label_visibility="collapsed")
                submitted = st.form_submit_button("Login")
            if submitted:
                user = authenticate_user(username.strip(), password)
                if user:
                    st.session_state["auth_user"] = user
                    st.rerun()
                else:
                    st.error("Invalid credentials.")

        with signup_tab:
            with st.form("signup_form"):
                st.markdown('<div class="auth-field-label">New Username</div>', unsafe_allow_html=True)
                new_username = st.text_input("New Username", label_visibility="collapsed")
                st.markdown('<div class="auth-field-label">Password</div>', unsafe_allow_html=True)
                new_password = st.text_input("Password", type="password", label_visibility="collapsed")
                st.markdown('<div class="auth-field-label">Confirm Password</div>', unsafe_allow_html=True)
                confirm_password = st.text_input("Confirm Password", type="password", label_visibility="collapsed")
                st.markdown('<div class="auth-field-label">Role</div>', unsafe_allow_html=True)
                role = st.selectbox("Role", ["fuel_user", "station_owner", "admin"], label_visibility="collapsed")
                st.markdown('<div class="auth-field-label">Home Latitude</div>', unsafe_allow_html=True)
                home_lat = st.number_input(
                    "Home Latitude",
                    value=float(CITY_CENTER[0]),
                    format="%.6f",
                    label_visibility="collapsed",
                )
                st.markdown('<div class="auth-field-label">Home Longitude</div>', unsafe_allow_html=True)
                home_lon = st.number_input(
                    "Home Longitude",
                    value=float(CITY_CENTER[1]),
                    format="%.6f",
                    label_visibility="collapsed",
                )
                admin_code = ""
                if role == "admin":
                    st.markdown('<div class="auth-field-label">Admin Signup Code</div>', unsafe_allow_html=True)
                    admin_code = st.text_input("Admin Signup Code", type="password", label_visibility="collapsed")

                selected_station_ids: list[int] = []
                if role == "station_owner":
                    station_df = get_station_lookup()
                    station_df["label"] = station_df["name"] + " | " + station_df["area"]
                    st.markdown('<div class="auth-field-label">Assign Petrol Bunks</div>', unsafe_allow_html=True)
                    selected_labels = st.multiselect(
                        "Assign Petrol Bunks",
                        station_df["label"].tolist(),
                        label_visibility="collapsed",
                    )
                    label_to_id = dict(zip(station_df["label"], station_df["station_id"]))
                    selected_station_ids = [int(label_to_id[label]) for label in selected_labels]

                create_btn = st.form_submit_button("Create Account")

            if create_btn:
                try:
                    if not new_username.strip():
                        raise ValueError("Username is required.")
                    if len(new_password) < 6:
                        raise ValueError("Password must be at least 6 characters.")
                    if new_password != confirm_password:
                        raise ValueError("Passwords do not match.")
                    if username_exists(new_username.strip()):
                        raise ValueError("Username already exists.")
                    if role == "admin" and admin_code != ADMIN_SIGNUP_CODE:
                        raise ValueError("Invalid admin signup code.")

                    user_id = create_user(
                        username=new_username.strip(),
                        password=new_password,
                        role=role,
                        home_latitude=float(home_lat),
                        home_longitude=float(home_lon),
                    )
                    if role == "station_owner":
                        assign_owner_to_stations(user_id, selected_station_ids)
                    st.success("Account created successfully. Please login now.")
                except Exception as exc:
                    st.error(str(exc))

        with st.expander("Demo Accounts", expanded=False):
            st.write("Admin: `admin` / `admin123`")
            st.write("Owners: `owner1..owner5` / `ownerX123`")
            st.write("Fuel Users: `user1..user10` / `userX123`")
            st.write(f"Admin signup code: `{ADMIN_SIGNUP_CODE}`")

        st.markdown("</div>", unsafe_allow_html=True)


def _owner_or_admin_inventory_ops(allowed_station_ids: list[int] | None = None, key_prefix: str = "ops") -> None:
    station_df = get_station_lookup(station_ids=allowed_station_ids)
    if station_df.empty:
        st.warning("No stations available for your role.")
        return
    station_df["label"] = station_df["name"] + " | " + station_df["area"]
    selected_label = st.selectbox("Select Station", station_df["label"].tolist())
    station_row = station_df.loc[station_df["label"] == selected_label].iloc[0]
    station_id = int(station_row["station_id"])
    if allowed_station_ids is not None and station_id not in set(allowed_station_ids):
        st.error("Unauthorized station selection.")
        return

    fuels_df = get_station_fuel_types(station_id)
    available_fuels = fuels_df["fuel_type"].tolist() if not fuels_df.empty else []
    missing_fuels = [ft for ft in ["Petrol", "Diesel"] if ft not in available_fuels]

    st.markdown("**Enabled Fuel Types**")
    _show_df(fuels_df)

    op_tab, fuel_tab = st.tabs(["Inventory Update", "Fuel Type Management"])

    with op_tab:
        if not available_fuels:
            st.info("No fuel type is enabled for this station yet. Enable one in Fuel Type Management.")
        else:
            fuel_type = st.selectbox("Fuel Type", available_fuels, key=f"{key_prefix}_fuel_type")
            inv_row = get_station_inventory_row(station_id, fuel_type)
            if inv_row:
                c1, c2 = st.columns(2)
                c1.metric("Current Available (L)", f"{float(inv_row['available_liters']):.2f}")
                c2.metric("Current Price", f"{float(inv_row['price']):.2f}")

            st.subheader("Refill / Sale / Price")
            add_val = st.number_input("Add liters", min_value=0.0, value=0.0, step=10.0, key=f"{key_prefix}_add")
            sub_val = st.number_input("Subtract liters", min_value=0.0, value=0.0, step=10.0, key=f"{key_prefix}_sub")
            price_val = st.number_input("New price", min_value=0.0, value=0.0, step=0.1, key=f"{key_prefix}_price")

            c1, c2, c3 = st.columns(3)
            if c1.button("Apply Refill", key=f"{key_prefix}_apply_refill"):
                try:
                    add_fuel(station_id, fuel_type, add_val)
                    st.success("Refill updated.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
            if c2.button("Apply Sale", key=f"{key_prefix}_apply_sale"):
                try:
                    subtract_fuel(station_id, fuel_type, sub_val)
                    st.success("Sale updated.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
            if c3.button("Apply Price Update", key=f"{key_prefix}_apply_price"):
                try:
                    update_price(station_id, fuel_type, price_val)
                    st.success("Price updated.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

    with fuel_tab:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Enable Fuel Type**")
            if missing_fuels:
                enable_type = st.selectbox("Fuel to Enable", missing_fuels, key=f"{key_prefix}_enable_type")
                enable_liters = st.number_input(
                    "Initial liters", min_value=0.0, value=500.0, step=100.0, key=f"{key_prefix}_enable_liters"
                )
                enable_price_val = st.number_input(
                    "Initial price", min_value=0.1, value=100.0, step=0.1, key=f"{key_prefix}_enable_price"
                )
                if st.button("Enable Fuel Type", key=f"{key_prefix}_enable_btn"):
                    try:
                        enable_fuel_type(station_id, enable_type, enable_liters, enable_price_val)
                        st.success(f"{enable_type} enabled.")
                        st.rerun()
                    except Exception as exc:
                        st.error(str(exc))
            else:
                st.info("Both Petrol and Diesel are already enabled.")

        with c2:
            st.markdown("**Disable Fuel Type**")
            if available_fuels:
                disable_type = st.selectbox("Fuel to Disable", available_fuels, key=f"{key_prefix}_disable_type")
                st.caption("Fuel type can be removed only when its available liters = 0.")
                if st.button("Disable Fuel Type", key=f"{key_prefix}_disable_btn"):
                    try:
                        disable_fuel_type(station_id, disable_type)
                        st.success(f"{disable_type} disabled.")
                        st.rerun()
                    except Exception as exc:
                        st.error(str(exc))
            else:
                st.info("No fuel type available to disable.")


def _fuel_user_pages(user: dict) -> None:
    _role_hero("fuel_user")
    page = _role_nav(["Nearby Stations", "Area Insights", "Map View"], key="fuel_user_nav", label="Fuel User Workspace")
    st.markdown(
        f'<div class="app-card"><b>My Saved Location:</b> {float(user.get("home_latitude") or CITY_CENTER[0]):.6f}, '
        f'{float(user.get("home_longitude") or CITY_CENTER[1]):.6f}</div>',
        unsafe_allow_html=True,
    )

    if page == "Nearby Stations":
        _page_header("Find Nearby Fuel Stations", "Live location search with inventory + price context.", "FUEL USER")
        _section_card_start()
        fuel_type = st.selectbox("Fuel Type", ["Petrol", "Diesel"])
        default_lat = float(user.get("home_latitude") or CITY_CENTER[0])
        default_lon = float(user.get("home_longitude") or CITY_CENTER[1])
        if "fuel_user_lat" not in st.session_state:
            st.session_state["fuel_user_lat"] = default_lat
        if "fuel_user_lon" not in st.session_state:
            st.session_state["fuel_user_lon"] = default_lon

        c1, c2, c3 = st.columns(3)
        with c1:
            lat = st.number_input("Your Latitude", key="fuel_user_lat", format="%.6f")
        with c2:
            lon = st.number_input("Your Longitude", key="fuel_user_lon", format="%.6f")
        with c3:
            radius = st.slider("Radius (km)", min_value=1, max_value=25, value=8)
        if st.button("Use My Saved Location"):
            st.session_state["fuel_user_lat"] = default_lat
            st.session_state["fuel_user_lon"] = default_lon
            st.rerun()
        _section_card_end()

        nearby = get_nearby_stations(lat, lon, fuel_type=fuel_type, radius_km=float(radius), limit=30)
        _section_card_start()
        _render_kpi_row(
            [
                ("Stations Found", str(int(len(nearby))), "Within selected radius"),
                ("Best Price", _fmt_number(float(nearby["price"].min()) if not nearby.empty else 0.0), "Lowest nearby"),
                (
                    "Nearest (km)",
                    _fmt_number(float(nearby["distance_km"].min()) if "distance_km" in nearby.columns and not nearby.empty else 0.0),
                    "Closest option",
                ),
            ]
        )
        st.subheader("Nearby Station Results")
        _show_df(nearby)
        _section_card_end()
        if not nearby.empty:
            _section_card_start()
            _render_station_map(nearby, user_lat=lat, user_lon=lon)
            _section_card_end()

    elif page == "Area Insights":
        _page_header("Area Price & Recommendation Insights", "Decision support by area-level demand and pricing.", "FUEL USER")
        _section_card_start()
        areas = get_areas()
        selected_area = st.selectbox("Area", areas)
        fuel_type = st.selectbox("Fuel Type", ["Petrol", "Diesel"], key="fuel_user_area_fuel")
        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider("Top Recommendations", min_value=3, max_value=15, value=5)
        with c2:
            ranking_mode = st.selectbox("Ranking Focus", ["Balanced", "Lowest Price", "Highest Availability"])

        if ranking_mode == "Lowest Price":
            rec = get_stations_inventory_view(area=selected_area, fuel_type=fuel_type).sort_values(
                ["price", "available_liters"], ascending=[True, False]
            ).head(top_n)
        elif ranking_mode == "Highest Availability":
            rec = get_stations_inventory_view(area=selected_area, fuel_type=fuel_type).sort_values(
                ["available_liters", "price"], ascending=[False, True]
            ).head(top_n)
        else:
            rec = recommend_stations(selected_area, fuel_type=fuel_type, top_n=top_n)

        avg_price = get_average_price_per_area(fuel_type=fuel_type)
        spread_df = get_price_spread_by_area(fuel_type=fuel_type)
        avg_price_value = avg_price.loc[avg_price["area"] == selected_area, "avg_price"]
        spread_val = spread_df.loc[spread_df["area"] == selected_area, "price_spread"]
        _render_kpi_row(
            [
                ("Area Avg Price", _fmt_number(float(avg_price_value.iloc[0]) if not avg_price_value.empty else 0.0), "Current benchmark"),
                ("Price Spread", _fmt_number(float(spread_val.iloc[0]) if not spread_val.empty else 0.0), "Competition intensity"),
            ]
        )
        _show_df(rec)
        _section_card_end()
    else:
        _page_header("City Fuel Map", "Operational map of station inventory and pricing.", "FUEL USER")
        _section_card_start()
        fuel_type = st.selectbox("Fuel Type", ["Petrol", "Diesel"], key="fuel_user_map_fuel")
        area_filter = st.selectbox("Area Filter", ["All"] + get_areas(), key="fuel_user_map_area")
        area = None if area_filter == "All" else area_filter
        df = get_stations_inventory_view(fuel_type=fuel_type, area=area)
        _render_station_map(df)
        _section_card_end()


def _station_owner_pages(user: dict) -> None:
    station_ids = get_owner_station_ids(int(user["user_id"]))
    _role_hero("station_owner")
    page = _role_nav(
        ["Owner Operations", "Owner Analytics", "Owner Map"],
        key="owner_nav",
        label="Owner Workspace",
    )

    if page == "Owner Operations":
        _page_header("Owner Inventory Operations", "You can update only your assigned stations.", "STATION OWNER")
        st.caption(f"Assigned stations: {len(station_ids)}")
        _section_card_start()
        _owner_or_admin_inventory_ops(allowed_station_ids=station_ids, key_prefix="owner_ops")
        _section_card_end()
    elif page == "Owner Analytics":
        _page_header("Owner SQL Analytics", "Demand, low-stock, and utilization analytics for your stations only.", "STATION OWNER")
        _section_card_start()
        c1, c2, c3 = st.columns(3)
        with c1:
            days_window = st.slider("Analysis Window (days)", min_value=7, max_value=90, value=30, key="owner_days")
        with c2:
            fuel_filter = st.selectbox("Fuel Filter", ["All", "Petrol", "Diesel"], key="owner_fuel_filter")
        with c3:
            top_n = st.slider("Top Stations", min_value=5, max_value=30, value=10, key="owner_top_n")

        ftype = None if fuel_filter == "All" else fuel_filter
        summary = get_owner_summary(int(user["user_id"]))
        low_stock = get_stockout_risk_report_dynamic(
            lookback_days=days_window,
            fuel_type=ftype,
            station_ids=station_ids,
        )
        low_stock = low_stock[low_stock["estimated_days_to_stockout"] <= 7].copy()
        daily = get_daily_demand_trend(days=days_window, station_ids=station_ids, fuel_type=ftype)
        hourly = get_hourly_demand_pattern(days=days_window, station_ids=station_ids, fuel_type=ftype)
        demand_totals = get_total_demand_per_station(station_ids=station_ids, fuel_type=ftype, days=days_window)
        revenue = get_revenue_estimate_per_station(days=days_window, fuel_type=ftype, station_ids=station_ids).head(top_n)
        growth = get_demand_growth_by_station(fuel_type=ftype, station_ids=station_ids).head(top_n)
        turnover = get_inventory_turnover_report(days=days_window, fuel_type=ftype, station_ids=station_ids).head(top_n)
        total_sales = float(demand_totals["total_liters_sold"].sum()) if not demand_totals.empty else 0.0
        avg_price_df = get_average_price_per_area(fuel_type=ftype, station_ids=station_ids)
        owner_avg_price = float(avg_price_df["avg_price"].mean()) if not avg_price_df.empty else 0.0
        total_revenue = float(revenue["estimated_revenue"].sum()) if not revenue.empty else 0.0

        _render_kpi_row(
            [
                ("Total Sales (L)", _fmt_number(total_sales), "Selected period"),
                ("Low Stock Risk Rows", str(int(len(low_stock))), "Stockout <= 7 days"),
                ("Avg Price", _fmt_number(owner_avg_price), "Across managed stations"),
                (f"Est. Revenue ({days_window}d)", _fmt_number(total_revenue), "Approximation"),
                ("Stations Managed", str(int(len(station_ids))), "Owner scope"),
            ]
        )
        _section_card_end()

        _section_card_start()
        st.subheader("Your Stations Performance")
        _show_df(summary)
        st.subheader("Low Stock Risk (<= 7 Days to Stockout)")
        _show_df(low_stock)
        st.subheader("30-Day Demand Trend")
        if not daily.empty:
            st.line_chart(daily.set_index("day")["liters_sold"])
        st.subheader("Hourly Demand Pattern")
        if not hourly.empty:
            st.bar_chart(hourly.set_index("hour_of_day")["liters_sold"])
        st.subheader("Revenue by Station")
        if not revenue.empty:
            st.bar_chart(revenue.set_index("name")["estimated_revenue"])
        _show_df(revenue)
        st.subheader("Demand Growth vs Previous Window")
        if not growth.empty:
            st.bar_chart(growth.set_index("name")["growth_pct"])
        _show_df(growth)
        st.subheader("Inventory Turnover")
        _show_df(turnover)
        _section_card_end()
    else:
        _page_header("Owner Station Map", "Map scoped strictly to your assigned stations.", "STATION OWNER")
        _section_card_start()
        fuel_type = st.selectbox("Fuel Type", ["Petrol", "Diesel"], key="owner_map_fuel")
        df = get_stations_inventory_view(fuel_type=fuel_type, station_ids=station_ids)
        _render_station_map(df)
        _section_card_end()


def _admin_pages() -> None:
    _role_hero("admin")
    page = _role_nav(
        ["Admin Dashboard", "Inventory Management", "Advanced Analytics", "System Map", "User Directory", "Master Setup"],
        key="admin_nav",
        label="Admin Workspace",
    )

    if page == "Admin Dashboard":
        _page_header("Admin Dashboard", "Network-wide controls, alerts, and KPI monitoring.", "ADMIN")
        _section_card_start()
        overview = get_admin_overview()
        _render_kpi_row(
            [
                ("Users", str(int(overview.get("users_count", 0))), "All roles"),
                ("Stations", str(int(overview.get("stations_count", 0))), "Active network"),
                ("Low Stock", str(int(overview.get("low_stock_count", 0))), "Immediate attention"),
                ("Total Sold (L)", _fmt_number(float(overview.get("total_sold_liters") or 0.0)), "Cumulative"),
                ("Avg Price", _fmt_number(float(overview.get("overall_avg_price") or 0.0)), "Network avg"),
            ]
        )

        areas = get_areas()
        selected_area = st.selectbox("Area", areas)
        fuel_type = st.selectbox("Fuel Type", ["Petrol", "Diesel"], key="admin_dash_fuel")
        st.subheader("Top Recommended")
        _show_df(recommend_stations(selected_area, fuel_type, top_n=3))
        st.subheader("Area Inventory View")
        _show_df(get_stations_inventory_view(area=selected_area, fuel_type=fuel_type))
        _section_card_end()

    elif page == "Inventory Management":
        _page_header("Global Inventory Management", "Admin can update all stations and fuel-type settings.", "ADMIN")
        _section_card_start()
        _owner_or_admin_inventory_ops(allowed_station_ids=None, key_prefix="admin_ops")
        _section_card_end()

    elif page == "Advanced Analytics":
        _page_header("Advanced SQL Analytics", "Cross-network demand, price, risk, and mix intelligence.", "ADMIN")
        _section_card_start()
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            fuel_type = st.selectbox("Fuel Type", ["Petrol", "Diesel"], key="admin_adv_fuel")
        with c2:
            days = st.slider("Time Window (days)", min_value=14, max_value=120, value=60, key="admin_adv_days")
        with c3:
            area_filter = st.selectbox("Area", ["All"] + get_areas(), key="admin_adv_area")
        with c4:
            top_n = st.slider("Top N", min_value=10, max_value=60, value=20, key="admin_adv_topn")

        area_station_ids = None
        if area_filter != "All":
            area_stations = get_stations_inventory_view(area=area_filter, fuel_type=fuel_type)
            area_station_ids = sorted({int(x) for x in area_stations["station_id"].tolist()}) if not area_stations.empty else []

        demand_station = get_total_demand_per_station(fuel_type=fuel_type, station_ids=area_station_ids)
        price_area = get_average_price_per_area(fuel_type=fuel_type, station_ids=area_station_ids)
        daily = get_daily_demand_trend(days=days, fuel_type=fuel_type, station_ids=area_station_ids)
        hourly = get_hourly_demand_pattern(days=days, fuel_type=fuel_type, station_ids=area_station_ids)
        fuel_mix = get_area_fuel_mix()
        if area_filter != "All":
            fuel_mix = fuel_mix[fuel_mix["area"] == area_filter]
        stock_risk = get_stockout_risk_report_dynamic(lookback_days=days, fuel_type=fuel_type, station_ids=area_station_ids)
        if area_station_ids is not None:
            stock_risk = stock_risk[stock_risk["station_id"].isin(area_station_ids)]
        stock_risk = stock_risk.head(top_n)
        revenue = get_revenue_estimate_per_station(days=days, fuel_type=fuel_type, station_ids=area_station_ids).head(top_n)
        growth = get_demand_growth_by_station(fuel_type=fuel_type, station_ids=area_station_ids).head(top_n)
        turnover = get_inventory_turnover_report(days=days, fuel_type=fuel_type, station_ids=area_station_ids).head(top_n)
        spread = get_price_spread_by_area(fuel_type=fuel_type, station_ids=area_station_ids)

        _render_kpi_row(
            [
                ("Estimated Revenue", _fmt_number(float(revenue["estimated_revenue"].sum()) if not revenue.empty else 0.0), "Selected slice"),
                ("Average Growth %", _fmt_number(float(growth["growth_pct"].mean()) if not growth.empty else 0.0), "Demand acceleration"),
                ("Average Turnover", _fmt_number(float(turnover["turnover_ratio"].mean()) if not turnover.empty else 0.0, 3), "Sales-to-stock"),
            ]
        )

        st.subheader("Total Demand Per Station")
        st.bar_chart(demand_station.head(top_n).set_index("name")["total_liters_sold"])
        _show_df(demand_station)

        st.subheader("Average Price by Area")
        st.bar_chart(price_area.set_index("area")["avg_price"])
        _show_df(price_area)

        st.subheader(f"Daily Demand Trend ({days} days)")
        if not daily.empty:
            st.line_chart(daily.set_index("day")["liters_sold"])

        st.subheader(f"Hourly Demand Pattern ({days} days)")
        if not hourly.empty:
            st.bar_chart(hourly.set_index("hour_of_day")["liters_sold"])

        st.subheader("Area Fuel Mix")
        if not fuel_mix.empty:
            pivot_mix = fuel_mix.pivot(index="area", columns="fuel_type", values="sold_liters").fillna(0)
            st.bar_chart(pivot_mix)
        _show_df(fuel_mix)

        st.subheader(f"Price Spread by Area ({fuel_type})")
        _show_df(spread)
        st.subheader("Revenue by Station")
        if not revenue.empty:
            st.bar_chart(revenue.set_index("name")["estimated_revenue"])
        _show_df(revenue)
        st.subheader("Demand Growth by Station")
        _show_df(growth)
        st.subheader("Inventory Turnover by Station")
        _show_df(turnover)

        st.subheader(f"Stockout Risk Report (Top {top_n} Highest Risk)")
        _show_df(stock_risk)
        _section_card_end()

    elif page == "System Map":
        _page_header("System Map", "City-wide operational monitoring view.", "ADMIN")
        _section_card_start()
        fuel_type = st.selectbox("Fuel Type", ["Petrol", "Diesel"], key="admin_map_fuel")
        area_filter = st.selectbox("Area Filter", ["All"] + get_areas())
        area = None if area_filter == "All" else area_filter
        _render_station_map(get_stations_inventory_view(area=area, fuel_type=fuel_type))
        _section_card_end()

    elif page == "User Directory":
        _page_header("User Directory", "All platform users and roles.", "ADMIN")
        _section_card_start()
        _show_df(get_all_users())
        _section_card_end()
    else:
        _page_header("Master Setup", "Create stations and privileged users.", "ADMIN")
        st.markdown('<div class="app-card">Create new petrol bunks and admin users from one place.</div>', unsafe_allow_html=True)

        add_station_tab, add_admin_tab = st.tabs(["Add Petrol Bunk", "Add Admin User"])

        with add_station_tab:
            with st.form("add_station_form"):
                name = st.text_input("Station Name")
                area = st.text_input("Area")
                c1, c2 = st.columns(2)
                with c1:
                    lat = st.number_input("Latitude", value=float(CITY_CENTER[0]), format="%.6f")
                    petrol_liters = st.number_input("Initial Petrol Liters", min_value=0.0, value=1500.0, step=100.0)
                    petrol_price = st.number_input("Petrol Price", min_value=0.1, value=105.0, step=0.1)
                with c2:
                    lon = st.number_input("Longitude", value=float(CITY_CENTER[1]), format="%.6f")
                    diesel_liters = st.number_input("Initial Diesel Liters", min_value=0.0, value=1500.0, step=100.0)
                    diesel_price = st.number_input("Diesel Price", min_value=0.1, value=95.0, step=0.1)
                submit_station = st.form_submit_button("Create Petrol Bunk")
            if submit_station:
                try:
                    station_id = create_station_with_inventory(
                        name=name,
                        area=area,
                        latitude=float(lat),
                        longitude=float(lon),
                        petrol_liters=float(petrol_liters),
                        petrol_price=float(petrol_price),
                        diesel_liters=float(diesel_liters),
                        diesel_price=float(diesel_price),
                    )
                    st.success(f"Petrol bunk created successfully with station_id={station_id}.")
                except Exception as exc:
                    st.error(str(exc))

        with add_admin_tab:
            with st.form("add_admin_user_form"):
                admin_username = st.text_input("Admin Username")
                admin_password = st.text_input("Admin Password", type="password")
                admin_confirm = st.text_input("Confirm Password", type="password")
                admin_signup_code = st.text_input("Admin Signup Code", type="password")
                submit_admin = st.form_submit_button("Create Admin User")
            if submit_admin:
                try:
                    if admin_password != admin_confirm:
                        raise ValueError("Passwords do not match.")
                    if admin_signup_code != ADMIN_SIGNUP_CODE:
                        raise ValueError("Invalid admin signup code.")
                    create_user(
                        username=admin_username.strip(),
                        password=admin_password,
                        role="admin",
                        home_latitude=CITY_CENTER[0],
                        home_longitude=CITY_CENTER[1],
                    )
                    st.success("Admin user created successfully.")
                except Exception as exc:
                    st.error(str(exc))


def main() -> None:
    _apply_ui_theme()
    _seed_once()
    if "auth_user" not in st.session_state:
        st.session_state["auth_user"] = None
    user = st.session_state["auth_user"]
    if not user:
        _login_screen()
        return

    if _top_app_bar(user):
        st.session_state["auth_user"] = None
        st.rerun()

    role = user["role"]
    if role == "fuel_user":
        _fuel_user_pages(user)
    elif role == "station_owner":
        _station_owner_pages(user)
    else:
        _admin_pages()


if __name__ == "__main__":
    main()
