#!/usr/bin/env python3
"""
Marharyta Zhytnyk Portfolio — щотижневий аналіз Bad Orders / Failed Orders.

Джерело: Databricks SQL (ng_delivery_spark).
Тиждень за замовчуванням — останній повний календарний тиждень (пн–нд).
Можна передати BAD_ORDERS_WEEK_START=YYYY-MM-DD або згенерувати кілька тижнів.
"""

from __future__ import annotations

import html
import json
import os
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from databricks import sql

ACCOUNT_MANAGER = "Marharyta Zhytnyk"
COUNTRY_CODE = "ua"

SERVER_HOSTNAME = "bolt-incentives.cloud.databricks.com"
HTTP_PATH = "sql/protocolv1/o/2472566184436351/0221-081903-9ag4bh69"

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_HTML = SCRIPT_DIR / "bad_orders_weekly.html"
COOKING_BASELINE_PATH = SCRIPT_DIR / "cooking_time_baseline.json"

# Причини Bad Orders, пов'язані з часом приготування / cooking ETA
PREP_RELATED_REASONS = (
    "provider_preparation_delay_seconds",
    "provider_preparation_overestimate_seconds",
    "bolt_cooking_eta_underestimate_seconds",
    "pickup_delay_provider_fault_seconds",
    "bolt_prep_instruction_delay_seconds",
)

CITY_UA = {
    "Bila Tserkva": "Біла Церква",
    "Boryspil": "Бориспіль",
    "Cherkasy": "Черкаси",
    "Chernihiv": "Чернігів",
    "Chernivtsi": "Чернівці",
    "Dnipro": "Дніпро",
    "Kharkiv": "Харків",
    "Kremenchuk": "Кременчук",
    "Kryvyi Rih": "Кривий Ріг",
    "Kyiv": "Київ",
    "Lviv": "Львів",
    "Odesa": "Одеса",
    "Oleksandriia": "Олександрія",
    "Pavlohrad": "Павлоград",
    "Poltava": "\u041f\u043e\u043b\u0442\u0430\u0432\u0430",
    "Sumy": "Суми",
    "Vinnytsia": "Вінниця",
    "Zaporizhia": "Запоріжжя",
    "Zhytomyr": "Житомир",
}

ACTOR_UA = {
    "bolt": "Платформа Bolt",
    "supply": "Платформа Bolt (дефіцит кур'єрів)",
    "courier": "Кур'єр",
    "provider": "Заклад",
    "eater": "Клієнт",
    "client": "Клієнт",
    "unknown": "Не атрибутовано",
    None: "Не атрибутовано",
    "": "Не атрибутовано",
}

# Зони відповідальності — верхній рівень класифікації, з яким працює акаунт-менеджер
RESP_UA = {
    "provider": "Заклад",
    "courier": "Кур'єр",
    "client": "Клієнт",
    "platform": "Платформа Bolt",
    "unassigned": "Не атрибутовано",
}

RESP_ORDER = ("provider", "courier", "platform", "client", "unassigned")

# bad_order_actor_at_fault → зона відповідальності
ACTOR_RESP = {
    "provider": "provider",
    "courier": "courier",
    "eater": "client",
    "client": "client",
    "bolt": "platform",
    "supply": "platform",
}

# Причини Bad Orders. Ключ — код без суфіксів _seconds / _eater.
REASON_UA = {
    # Заклад
    "did_not_respond": "Заклад: не прийняв замовлення (не відповів)",
    "provider_preparation_delay": "Заклад: затримка приготування",
    "provider_preparation_overestimate": "Заклад: переоцінка часу приготування",
    "pickup_delay_provider_fault": "Заклад: затримка на видачі кур'єру",
    "missing_item": "Заклад: відсутня позиція в замовленні",
    "items_out_of_stock": "Заклад: позиції немає в наявності (стоп-лист не оновлено)",
    "too_many_orders": "Заклад: відмова через завантаженість",
    "closed": "Заклад: закритий у робочі години",
    "do_not_wish_to_serve_this_client": "Заклад: відмова обслуговувати клієнта",
    "wrong_item": "Заклад: видано не ту позицію",
    "received_an_entirely_wrong_order": "Заклад: видано зовсім інше замовлення",
    "item_had_a_spoiled_taste_or_smell": "Заклад: зіпсований смак або запах страви",
    "food_was_overcooked_or_burnt": "Заклад: страва переготована або підгоріла",
    "food_was_undercooked_or_raw": "Заклад: страва недогорована або сира",
    "object_detected_in_food": "Заклад: сторонній предмет у страві",
    "foreign_object_in_food": "Заклад: сторонній предмет у страві",
    "food_poisoning": "Заклад: скарга на отруєння",
    "item_does_not_match_the_description": "Заклад: страва не відповідає опису",
    "item_does_not_match_the_photo": "Заклад: страва не відповідає фото",
    "item_does_not_match_the_expectations": "Заклад: страва не відповідає очікуванням клієнта",
    "restaurant_ignored_my_order_notes": "Заклад: проігноровано комментар до замовлення",
    "missing_cutlery": "Заклад: не поклали прибори",
    "there_was_a_mistake_in_the_menu": "Заклад: помилка в меню",
    "unable_to_contact_the_restaurant": "Заклад: не вдалося зв'язатися із закладом",
    "question_about_menu_item": "Заклад: питання щодо позиції меню",
    "device_issue": "Заклад: проблема з планшетом або терміналом",
    # Кур'єр
    "courier_to_provider_eta_error": "Кур'єр: приїхав до закладу пізніше за ETA",
    "provider_to_eater_eta_error": "Кур'єр: привіз клієнту пізніше за ETA",
    "pickup_delay_courier_fault": "Кур'єр: затримка на забиранні замовлення",
    "courier_dropoff_delay_adjusted": "Кур'єр: затримка на видачі клієнту",
    "my_courier_is_late": "Спізнення доставки",
    "my_courier_is_not_moving": "Кур'єр: не рухається за маршрутом",
    "my_courier_cannot_find_me": "Кур'єр: не може знайти клієнта",
    "my_courier_was_rude": "Кур'єр: неввічлива поведінка",
    "unable_to_contact_the_courier": "Кур'єр: не виходить на зв'язок",
    "courier_added_the_wrong_cash_amount": "Кур'єр: помилка з готівкою",
    "courier_no_change": "Кур'єр: не мав решти",
    "order_never_delivered": "Замовлення так і не доставлено клієнту",
    "my_order_arrived_cold": "Замовлення привезли холодним",
    "order_damaged": "Замовлення пошкоджено під час доставки",
    "courier_caused_delay_and_user_cancelled": "Кур'єр затримав, клієнт скасував",
    # Платформа Bolt
    "bolt_cooking_eta_underestimate": "Bolt: система занизила прогноз часу приготування",
    "bolt_prep_instruction_delay": "Bolt: із запізненням передав закладу команду готувати",
    "bolt_assignment_delay_from_supply_starvation": "Bolt: не було вільних кур'єрів у зоні",
    "bolt_assignment_delay_from_rejections": "Bolt: довгий пошук через відмови кур'єрів",
    "no_courier_is_assigned_to_the_order": "Кур'єра не було призначено на замовлення",
    "bolt_batching_delay": "Bolt: затримка через об'єднання замовлень (батчинг)",
    "bolt_dispatch_start_delay": "Bolt: пізно розпочато пошук кур'єра",
    "courier_redispatch_duration": "Повторний пошук кур'єра (перепризначення)",
    "manually_failed_by_cs": "Скасовано агентом підтримки",
    "automatically_failed": "Скасовано автоматично системою",
    "bolt_caused_delay_and_user_cancelled": "Bolt затримав, клієнт скасував",
    "failed_payment": "Не пройшла оплата",
    "charged_twice_for_my_order": "Подвійне списання за замовлення",
    "question_about_price_calculation": "Питання щодо розрахунку ціни",
    # Клієнт
    "eater_contaminated_food": "Клієнт: зіпсував їжу",
    # Без атрибуції
    "unknown_delay_dropoff": "Затримка на етапі доставки, причину не визначено",
    "unknown_delay_pickup": "Затримка на етапі видачі, причину не визначено",
    "order_took_longer": "Замовлення тривало довше очікуваного, етап не визначено",
    "other": "Інша причина",
    "unknown": "Причину не визначено",
    "none": "Без деталізації",
    None: "Без деталізації",
}

# Причина → зона відповідальності. Використовується, коли модель атрибуції не дала актора.
REASON_RESP = {
    "did_not_respond": "provider",
    "provider_preparation_delay": "provider",
    "provider_preparation_overestimate": "provider",
    "pickup_delay_provider_fault": "provider",
    "missing_item": "provider",
    "items_out_of_stock": "provider",
    "too_many_orders": "provider",
    "closed": "provider",
    "do_not_wish_to_serve_this_client": "provider",
    "wrong_item": "provider",
    "received_an_entirely_wrong_order": "provider",
    "item_had_a_spoiled_taste_or_smell": "provider",
    "food_was_overcooked_or_burnt": "provider",
    "food_was_undercooked_or_raw": "provider",
    "object_detected_in_food": "provider",
    "foreign_object_in_food": "provider",
    "food_poisoning": "provider",
    "item_does_not_match_the_description": "provider",
    "item_does_not_match_the_photo": "provider",
    "item_does_not_match_the_expectations": "provider",
    "restaurant_ignored_my_order_notes": "provider",
    "missing_cutlery": "provider",
    "there_was_a_mistake_in_the_menu": "provider",
    "unable_to_contact_the_restaurant": "provider",
    "device_issue": "provider",
    "courier_to_provider_eta_error": "courier",
    "provider_to_eater_eta_error": "courier",
    "pickup_delay_courier_fault": "courier",
    "courier_dropoff_delay_adjusted": "courier",
    "my_courier_is_late": "courier",
    "my_courier_is_not_moving": "courier",
    "my_courier_cannot_find_me": "courier",
    "my_courier_was_rude": "courier",
    "unable_to_contact_the_courier": "courier",
    "courier_added_the_wrong_cash_amount": "courier",
    "courier_no_change": "courier",
    "order_never_delivered": "courier",
    "my_order_arrived_cold": "courier",
    "order_damaged": "courier",
    "courier_caused_delay_and_user_cancelled": "courier",
    "bolt_cooking_eta_underestimate": "platform",
    "bolt_prep_instruction_delay": "platform",
    "bolt_assignment_delay_from_supply_starvation": "platform",
    "bolt_assignment_delay_from_rejections": "platform",
    "no_courier_is_assigned_to_the_order": "platform",
    "bolt_batching_delay": "platform",
    "bolt_dispatch_start_delay": "platform",
    "courier_redispatch_duration": "platform",
    "manually_failed_by_cs": "platform",
    "automatically_failed": "platform",
    "bolt_caused_delay_and_user_cancelled": "platform",
    "failed_payment": "platform",
    "charged_twice_for_my_order": "platform",
    "eater_contaminated_food": "client",
}

# З чого складається «вина платформи» — підгрупи з технічним змістом
PLATFORM_GROUP = {
    "bolt_cooking_eta_underestimate": "eta",
    "bolt_prep_instruction_delay": "eta",
    "bolt_assignment_delay_from_supply_starvation": "supply",
    "bolt_assignment_delay_from_rejections": "supply",
    "no_courier_is_assigned_to_the_order": "supply",
    "bolt_batching_delay": "dispatch",
    "bolt_dispatch_start_delay": "dispatch",
    "courier_redispatch_duration": "dispatch",
    "manually_failed_by_cs": "support",
    "automatically_failed": "support",
    "bolt_caused_delay_and_user_cancelled": "support",
    "failed_payment": "payment",
    "charged_twice_for_my_order": "payment",
    "question_about_price_calculation": "payment",
    "order_damaged": "delivery",
    "my_order_arrived_cold": "delivery",
    "my_courier_is_late": "delivery",
    "order_never_delivered": "delivery",
    "order_took_longer": "delivery",
}

PLATFORM_GROUP_UA = {
    "eta": "Прогноз часу приготування (алгоритм ETA)",
    "supply": "Немає вільних кур'єрів у зоні",
    "dispatch": "Пошук, батчинг і перепризначення кур'єра",
    "support": "Скасування підтримкою або автоматикою",
    "payment": "Оплата й тарифікація",
    "delivery": "Якість доставки, віднесена до платформи",
    "other": "Інше або без деталізації",
}

PLATFORM_GROUP_HINT = {
    "eta": "Модель Bolt спрогнозувала час приготування коротшим за реальний, "
    "тому клієнту показали занадто оптимістичний час доставки. Заклад свій час не порушував.",
    "supply": "У момент замовлення в зоні не було вільних кур'єрів, "
    "тому замовлення чекало призначення. Це питання щільності кур'єрів, а не закладу.",
    "dispatch": "Алгоритм пізно почав пошук кур'єра, об'єднав два замовлення в один маршрут "
    "або перепризначив кур'єра посеред доставки.",
    "support": "Замовлення закрив агент підтримки або автоматика "
    "(наприклад, після довгого очікування чи звернення клієнта).",
    "payment": "Технічні проблеми з оплатою: не пройшов платіж, подвійне списання, питання щодо ціни.",
    "delivery": "Пошкоджене, холодне або недоставлене замовлення, яке модель атрибуції "
    "віднесла до платформи, а не до конкретного кур'єра.",
    "other": "Причина позначена як платформа, але без конкретного технічного коду.",
}

# Етап, на якому замовлення зірвалось (from_state) → зона відповідальності
STAGE_UA = {
    "waiting_acceptance": "заклад не прийняв замовлення вчасно",
    "waiting_starting_preparation": "заклад не почав приготування",
    "waiting_preparation": "зрив під час приготування",
    "ready_for_pickup": "замовлення готове, але кур'єр не забрав",
    "waiting_delivery": "зрив на етапі доставки",
    "waiting_payment": "зрив на етапі оплати",
}

STAGE_RESP = {
    "waiting_acceptance": "provider",
    "waiting_starting_preparation": "provider",
    "waiting_preparation": "provider",
    "ready_for_pickup": "platform",
    "waiting_delivery": "platform",
    "waiting_payment": "platform",
}


def get_token() -> str:
    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        return token
    for env_path in (
        Path(__file__).resolve().parent.parent / "databricks-setup" / ".env",
        Path.home()
        / "Library"
        / "CloudStorage"
        / "GoogleDrive-marharyta.zhytnyk@bolt.eu"
        / "My Drive"
        / "Events project"
        / "databricks-setup"
        / ".env",
    ):
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("DATABRICKS_TOKEN="):
                    return line.split("=", 1)[1].strip()
    raise RuntimeError("DATABRICKS_TOKEN not found")


def week_bounds(week_start: date | None = None) -> tuple[date, date]:
    if week_start:
        return week_start, week_start + timedelta(days=6)
    if os.environ.get("BAD_ORDERS_WEEK_START"):
        start = date.fromisoformat(os.environ["BAD_ORDERS_WEEK_START"])
        return start, start + timedelta(days=6)
    today = date.today()
    end = today if today.weekday() == 6 else today - timedelta(days=today.weekday() + 1)
    return end - timedelta(days=6), end


def city_ua(name: str | None) -> str:
    if not name:
        return "—"
    return CITY_UA.get(name, name)


def norm_code(code: str) -> str:
    """Код причини без технічних суфіксів: provider_delay_seconds → provider_delay."""
    c = str(code).strip()
    for suffix in ("_seconds", "_eater"):
        if c.endswith(suffix):
            c = c[: -len(suffix)]
    return c


def split_codes(code) -> list[str]:
    """Причина може містити кілька кодів через кому, часто дублів (…_eater)."""
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return []
    out: list[str] = []
    for part in str(code).split(","):
        c = norm_code(part)
        if c and c not in ("none", "unknown") and c not in out:
            out.append(c)
    return out


def reason_ua(code) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return REASON_UA[None]
    codes = split_codes(code)
    if not codes:
        return REASON_UA["none"] if str(code) in ("none", "nan") else REASON_UA["unknown"]
    labels: list[str] = []
    for c in codes:
        label = REASON_UA.get(c, c.replace("_", " "))
        if label not in labels:
            labels.append(label)
    return " + ".join(labels)


def actor_ua(code) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return ACTOR_UA[None]
    return ACTOR_UA.get(str(code).lower(), str(code))


def classify_stage(state: str, from_state: str, courier_rejects: int, eater_cancelled: bool) -> str:
    """Зона відповідальності за зірване замовлення на основі етапу зриву."""
    if state == "rejected":
        return "provider"
    if courier_rejects > 0:
        return "courier"
    if eater_cancelled:
        return "client"
    return STAGE_RESP.get(from_state, "unassigned")


def classify_failed(row: pd.Series) -> str:
    state = str(row.get("final_state") or "")
    if state == "rejected" or row.get("is_rejected_by_provider") is True:
        return "provider"
    return classify_stage(
        state,
        str(row.get("from_state") or ""),
        int(row.get("number_courier_rejects") or 0),
        row.get("has_eater_cancellation_ticket") is True,
    )


def stage_ua(from_state: str) -> str:
    if not from_state or from_state == "—":
        return "етап зриву не визначено"
    return STAGE_UA.get(from_state, f"зрив з етапу «{from_state}»")


def failed_detail_ua(row: pd.Series) -> str:
    state = str(row.get("final_state") or "")
    from_st = str(row.get("from_state") or "") or "—"
    ncr = int(row.get("number_courier_rejects") or 0)
    if state == "rejected":
        return "Заклад відхилив замовлення"
    if state == "failed":
        parts = [stage_ua(from_st)]
        if ncr:
            parts.append(f"відмови кур'єра: {ncr}")
        if row.get("has_eater_cancellation_ticket") is True:
            parts.append("є скасування з боку клієнта")
        if row.get("is_rejected_by_provider") is True:
            parts.append("позначено як відхилення закладом")
        return "; ".join(parts)
    return state


def bad_comment_ua(row: pd.Series) -> str:
    parts = []
    rating = row.get("order_food_rating_value")
    if rating is not None and not (isinstance(rating, float) and pd.isna(rating)):
        rv = float(rating)
        if rv <= 2:
            parts.append(f"Оцінка їжі: {int(rv)}/5")
    late = row.get("late_delivery_actor_at_fault_reason")
    if late is not None and not (isinstance(late, float) and pd.isna(late)):
        parts.append(reason_ua(str(late).replace("_seconds", "")))
    return "; ".join(parts) if parts else "—"


FAULT_REASON_UA = {
    "provider": "Заклад відхилив або не прийняв замовлення",
    "courier": "Відмови кур'єрів під час пошуку",
    "client": "Скасування з боку клієнта",
    "platform": "Кур'єра не знайдено або зрив на етапі доставки",
    "unassigned": "Зрив без визначеного етапу",
}


def run_query(conn, q: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(q)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def fetch_week_data(conn, week_start: date, week_end: date) -> dict:
    d0, d1 = week_start.isoformat(), week_end.isoformat()
    print(f"  Fetching {d0} .. {d1} …")

    sql_summary = f"""
    SELECT
        p.brand_name,
        p.city_name,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN o.state = 'delivered' THEN 1 ELSE 0 END) AS delivered,
        SUM(CASE WHEN f.is_bad_order = true OR o.state IN ('failed','rejected') THEN 1 ELSE 0 END) AS bad_count,
        SUM(CASE WHEN o.state IN ('failed','rejected') THEN 1 ELSE 0 END) AS failed_count
    FROM ng_delivery_spark.delivery_order_order o
    INNER JOIN ng_delivery_spark.fact_order_delivery f ON f.order_id = o.id
    INNER JOIN ng_delivery_spark.dim_provider_v2 p ON p.provider_id = o.provider_id
    WHERE p.account_manager_name = '{ACCOUNT_MANAGER}'
      AND p.country_code = '{COUNTRY_CODE}'
      AND o.created_date BETWEEN '{d0}' AND '{d1}'
    GROUP BY p.brand_name, p.city_name
    """

    sql_orders = f"""
    SELECT
        o.id AS order_id,
        o.reference_id AS order_ref,
        o.state AS final_state,
        o.created AS order_created,
        p.provider_id,
        p.provider_name,
        p.brand_name,
        p.city_name,
        f.is_bad_order,
        f.is_rejected_by_provider,
        f.number_courier_rejects,
        f.has_eater_cancellation_ticket,
        f.order_food_rating_value,
        a.bad_order_actor_at_fault,
        a.bad_order_main_reason,
        a.late_delivery_actor_at_fault_reason
    FROM ng_delivery_spark.delivery_order_order o
    INNER JOIN ng_delivery_spark.fact_order_delivery f ON f.order_id = o.id
    INNER JOIN ng_delivery_spark.dim_provider_v2 p ON p.provider_id = o.provider_id
    LEFT JOIN ng_delivery_spark.int_order_bad_order_attribution a ON a.order_id = o.id
    WHERE p.account_manager_name = '{ACCOUNT_MANAGER}'
      AND p.country_code = '{COUNTRY_CODE}'
      AND o.created_date BETWEEN '{d0}' AND '{d1}'
      AND (
        f.is_bad_order = true
        OR o.state IN ('failed', 'rejected')
      )
    ORDER BY p.city_name, p.brand_name, o.created
    """

    df_summary = run_query(conn, sql_summary)
    df_orders = run_query(conn, sql_orders)

    if len(df_orders):
        ids = ",".join(str(int(x)) for x in df_orders["order_id"].tolist())
        sql_log = f"""
        SELECT order_id, from_state, to_state
        FROM (
          SELECT order_id, from_state, to_state,
                 ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created DESC) AS rn
          FROM ng_delivery_spark.delivery_order_order_state_log
          WHERE order_id IN ({ids})
            AND created_date >= DATE_SUB('{d0}', 5)
            AND created_date <= DATE_ADD('{d1}', 8)
        ) t WHERE rn = 1
        """
        df_log = run_query(conn, sql_log)
        df_orders = df_orders.merge(df_log, on="order_id", how="left")
    else:
        df_orders["from_state"] = None
        df_orders["to_state"] = None

    return build_week_payload(week_start, week_end, df_summary, df_orders)


def prep_time_window() -> tuple[date, date]:
    """Останні 2 повні календарні тижні (пн–нд), закінчуючись минулою неділею."""
    today = date.today()
    last_sunday = today - timedelta(days=today.weekday() + 1)
    return last_sunday - timedelta(days=13), last_sunday


def empty_prep_payload() -> dict:
    return {
        "period_start": "",
        "period_end": "",
        "label": "",
        "rows": [],
    }


def recommend_cooking_time_min(actual: float | None) -> int | None:
    """Рекомендований cooking time у системі: actual, округлений до 5 хв."""
    if actual is None:
        return None
    # Крок 5 хв — типовий для налаштування cooking time в Bolt
    rounded = int(round(max(5.0, float(actual)) / 5.0) * 5)
    return min(rounded, 90)


def fetch_prep_time_data(conn) -> dict:
    """Actual vs estimated preparation time по провайдерах портфоліо за останні 2 тижні."""
    d0, d1 = prep_time_window()
    print(f"  Fetching preparation time {d0} .. {d1} …")

    q = f"""
    SELECT
        p.provider_id,
        p.provider_name,
        p.brand_name,
        p.city_name,
        ROUND(MAX(p.average_cooking_time_minutes) / 60.0, 1) AS cooking_time_min,
        COUNT(*) AS orders,
        ROUND(AVG(f.order_actual_cooking_time_minutes), 1) AS actual_prep_min,
        ROUND(AVG(COALESCE(
            f.provider_ml_estimated_adjusted_cooking_time_minutes,
            f.provider_stated_cooking_time_minutes,
            f.provider_estimated_cooking_time,
            f.order_provider_estimated_cooking_time_seconds / 60.0
        )), 1) AS estimated_prep_min
    FROM ng_delivery_spark.fact_order_delivery f
    INNER JOIN ng_delivery_spark.dim_provider_v2 p
        ON p.provider_id = f.provider_id
    WHERE p.account_manager_name = '{ACCOUNT_MANAGER}'
      AND p.country_code = '{COUNTRY_CODE}'
      AND f.order_created_date_local BETWEEN '{d0.isoformat()}' AND '{d1.isoformat()}'
      AND f.order_actual_cooking_time_minutes IS NOT NULL
      AND f.order_actual_cooking_time_minutes > 0
    GROUP BY
        p.provider_id,
        p.provider_name,
        p.brand_name,
        p.city_name
    HAVING COUNT(*) >= 1
    ORDER BY
        ABS(
            AVG(f.order_actual_cooking_time_minutes)
            - AVG(COALESCE(
                f.provider_ml_estimated_adjusted_cooking_time_minutes,
                f.provider_stated_cooking_time_minutes,
                f.provider_estimated_cooking_time,
                f.order_provider_estimated_cooking_time_seconds / 60.0
            ))
        ) DESC
    """

    df = run_query(conn, q)
    rows: list[dict] = []
    for _, row in df.iterrows():
        actual = float(row["actual_prep_min"]) if row["actual_prep_min"] is not None else None
        estimated = (
            float(row["estimated_prep_min"]) if row["estimated_prep_min"] is not None else None
        )
        cooking = float(row["cooking_time_min"]) if row["cooking_time_min"] is not None else None
        diff = None
        if actual is not None and estimated is not None:
            diff = round(actual - estimated, 1)
        recommended = recommend_cooking_time_min(actual)
        city = str(row["city_name"] or "—")
        rows.append(
            {
                "provider_id": int(row["provider_id"]),
                "provider_name": str(row["provider_name"] or "—"),
                "brand_name": str(row["brand_name"] or "—"),
                "city_name": city,
                "city_ua": CITY_UA.get(city, city),
                "cooking_time_min": cooking,
                "actual_prep_min": actual,
                "estimated_prep_min": estimated,
                "diff_min": diff,
                "recommended_cooking_min": recommended,
                "orders": int(row["orders"]),
            }
        )

    return {
        "period_start": d0.isoformat(),
        "period_end": d1.isoformat(),
        "label": f"{d0:%d.%m.%Y} – {d1:%d.%m.%Y}",
        "rows": rows,
    }


def load_cooking_baseline() -> dict:
    if not COOKING_BASELINE_PATH.exists():
        return {}
    try:
        return json.loads(COOKING_BASELINE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def ensure_cooking_baseline(prep_data: dict) -> dict:
    """Зберігає baseline cooking time один раз (до змін партнера). Не перезаписує без COOKING_BASELINE_RESET=1."""
    if COOKING_BASELINE_PATH.exists() and not os.environ.get("COOKING_BASELINE_RESET"):
        return load_cooking_baseline()

    providers = {}
    for r in prep_data.get("rows") or []:
        pid = str(r["provider_id"])
        providers[pid] = {
            "provider_id": r["provider_id"],
            "provider_name": r.get("provider_name"),
            "brand_name": r.get("brand_name"),
            "city_name": r.get("city_name"),
            "city_ua": r.get("city_ua"),
            "cooking_time_min": r.get("cooking_time_min"),
            "recommended_cooking_min": r.get("recommended_cooking_min"),
            "actual_prep_min": r.get("actual_prep_min"),
            "estimated_prep_min": r.get("estimated_prep_min"),
        }
    before_start, _ = week_bounds()
    # Тиждень ДО змін (останній повний перед baseline-снімком)
    # Якщо baseline створюється вперше mid-week — фіксуємо попередній повний тиждень.
    payload = {
        "saved_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "note": "Baseline cooking time for impact analysis. Do not overwrite unless COOKING_BASELINE_RESET=1.",
        "before_week_start": before_start.isoformat(),
        "before_week_label": f"{before_start:%d.%m.%Y} – {before_start + timedelta(days=6):%d.%m.%Y}",
        "prep_period_label": prep_data.get("label"),
        "providers": providers,
    }
    COOKING_BASELINE_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"  Saved cooking baseline ({len(providers)} providers) → {COOKING_BASELINE_PATH.name}")
    return payload


def fetch_provider_week_quality(conn, week_start: date, week_end: date) -> dict[int, dict]:
    """Bad Orders / prep-related metrics на рівні provider_id за тиждень."""
    d0, d1 = week_start.isoformat(), week_end.isoformat()
    reasons_sql = ", ".join(f"'{r}'" for r in PREP_RELATED_REASONS)
    q = f"""
    SELECT
        p.provider_id,
        p.provider_name,
        p.brand_name,
        p.city_name,
        ROUND(MAX(p.average_cooking_time_minutes) / 60.0, 1) AS cooking_time_min,
        SUM(CASE WHEN o.state = 'delivered' THEN 1 ELSE 0 END) AS delivered,
        SUM(CASE WHEN f.is_bad_order = true OR o.state IN ('failed','rejected') THEN 1 ELSE 0 END) AS bad_count,
        SUM(CASE WHEN o.state IN ('failed','rejected') THEN 1 ELSE 0 END) AS failed_count,
        SUM(CASE
              WHEN a.bad_order_main_reason IN ({reasons_sql})
                OR a.late_delivery_actor_at_fault_reason IN ({reasons_sql})
              THEN 1 ELSE 0 END) AS prep_related_bad
    FROM ng_delivery_spark.delivery_order_order o
    INNER JOIN ng_delivery_spark.fact_order_delivery f ON f.order_id = o.id
    INNER JOIN ng_delivery_spark.dim_provider_v2 p ON p.provider_id = o.provider_id
    LEFT JOIN ng_delivery_spark.int_order_bad_order_attribution a ON a.order_id = o.id
    WHERE p.account_manager_name = '{ACCOUNT_MANAGER}'
      AND p.country_code = '{COUNTRY_CODE}'
      AND o.created_date BETWEEN '{d0}' AND '{d1}'
    GROUP BY p.provider_id, p.provider_name, p.brand_name, p.city_name
    """
    df = run_query(conn, q)
    out: dict[int, dict] = {}
    for _, row in df.iterrows():
        pid = int(row["provider_id"])
        delivered = int(row["delivered"] or 0)
        bad = int(row["bad_count"] or 0)
        failed = int(row["failed_count"] or 0)
        prep_bad = int(row["prep_related_bad"] or 0)
        city = str(row["city_name"] or "—")
        out[pid] = {
            "provider_id": pid,
            "provider_name": str(row["provider_name"] or "—"),
            "brand_name": str(row["brand_name"] or "—"),
            "city_name": city,
            "city_ua": CITY_UA.get(city, city),
            "cooking_time_min": float(row["cooking_time_min"])
            if row["cooking_time_min"] is not None
            else None,
            "delivered": delivered,
            "bad_count": bad,
            "failed_count": failed,
            "prep_related_bad": prep_bad,
            "bad_pct": round(bad / delivered * 100, 2) if delivered else None,
            "failed_pct": round(failed / delivered * 100, 2) if delivered else None,
            "prep_related_pct": round(prep_bad / delivered * 100, 2) if delivered else None,
        }
    return out


def empty_cooking_impact() -> dict:
    return {
        "ready": False,
        "message": "Очікуємо дані після наступного повного тижня з новими cooking time.",
        "baseline_saved_at": "",
        "before_week_start": "",
        "before_week_label": "",
        "after_week_start": "",
        "after_week_label": "",
        "changed_count": 0,
        "improved_count": 0,
        "worsened_count": 0,
        "rows": [],
    }


def build_cooking_impact(conn, prep_data: dict, weeks_data: dict[str, dict]) -> dict:
    """
    Порівнює Bad Orders до/після зміни cooking time для критичних партнерів.
    Baseline фіксує cooking time на момент змін (14.07.2026).
    After-тиждень — останній повний тиждень у звіті після baseline before_week.
    """
    baseline = load_cooking_baseline()
    if not baseline.get("providers"):
        baseline = ensure_cooking_baseline(prep_data)

    before_key = baseline.get("before_week_start") or "2026-07-06"
    before_start = date.fromisoformat(before_key)
    before_end = before_start + timedelta(days=6)

    week_keys = sorted(weeks_data.keys())
    after_candidates = [k for k in week_keys if date.fromisoformat(k) > before_start]
    if not after_candidates:
        impact = empty_cooking_impact()
        impact["baseline_saved_at"] = baseline.get("saved_at", "")
        impact["before_week_start"] = before_key
        impact["before_week_label"] = baseline.get(
            "before_week_label",
            f"{before_start:%d.%m.%Y} – {before_end:%d.%m.%Y}",
        )
        impact["message"] = (
            "Базовий тиждень зафіксовано "
            f"({impact['before_week_label']}). "
            "Аналіз впливу з’явиться після оновлення за наступний повний тиждень (пн)."
        )
        return impact

    after_key = after_candidates[-1]
    after_start = date.fromisoformat(after_key)
    after_end = after_start + timedelta(days=6)

    print(f"  Cooking impact: before {before_key} vs after {after_key}")
    before_stats = fetch_provider_week_quality(conn, before_start, before_end)
    after_stats = fetch_provider_week_quality(conn, after_start, after_end)

    current_by_id = {int(r["provider_id"]): r for r in (prep_data.get("rows") or [])}
    rows: list[dict] = []

    for pid_str, base in (baseline.get("providers") or {}).items():
        try:
            pid = int(pid_str)
        except (TypeError, ValueError):
            continue
        base_cook = base.get("cooking_time_min")
        cur = current_by_id.get(pid) or after_stats.get(pid) or {}
        cur_cook = cur.get("cooking_time_min")
        if base_cook is None or cur_cook is None:
            continue
        delta_cook = round(float(cur_cook) - float(base_cook), 1)
        if abs(delta_cook) < 5:
            continue

        b = before_stats.get(pid, {})
        a = after_stats.get(pid, {})
        bad_before = b.get("bad_pct")
        bad_after = a.get("bad_pct")
        delta_bad = None
        if bad_before is not None and bad_after is not None:
            delta_bad = round(bad_after - bad_before, 2)

        prep_before = b.get("prep_related_pct")
        prep_after = a.get("prep_related_pct")
        delta_prep = None
        if prep_before is not None and prep_after is not None:
            delta_prep = round(prep_after - prep_before, 2)

        rows.append(
            {
                "provider_id": pid,
                "provider_name": cur.get("provider_name")
                or a.get("provider_name")
                or base.get("provider_name")
                or "—",
                "brand_name": cur.get("brand_name")
                or a.get("brand_name")
                or base.get("brand_name")
                or "—",
                "city_ua": cur.get("city_ua")
                or a.get("city_ua")
                or base.get("city_ua")
                or CITY_UA.get(str(base.get("city_name") or ""), str(base.get("city_name") or "—")),
                "cooking_before": float(base_cook),
                "cooking_after": float(cur_cook),
                "delta_cooking": delta_cook,
                "recommended_at_baseline": base.get("recommended_cooking_min"),
                "bad_pct_before": bad_before,
                "bad_pct_after": bad_after,
                "delta_bad_pp": delta_bad,
                "prep_related_pct_before": prep_before,
                "prep_related_pct_after": prep_after,
                "delta_prep_related_pp": delta_prep,
                "delivered_before": b.get("delivered", 0),
                "delivered_after": a.get("delivered", 0),
                "bad_count_before": b.get("bad_count", 0),
                "bad_count_after": a.get("bad_count", 0),
            }
        )

    rows.sort(
        key=lambda r: (
            0 if r.get("delta_bad_pp") is None else 1,
            abs(r.get("delta_bad_pp") or 0),
        ),
        reverse=True,
    )

    improved = sum(1 for r in rows if (r.get("delta_bad_pp") or 0) < -0.5)
    worsened = sum(1 for r in rows if (r.get("delta_bad_pp") or 0) > 0.5)

    return {
        "ready": True,
        "message": (
            "Порівняння провайдерів, у яких cooking time змінився ≥5 хв "
            "відносно baseline до ваших правок."
        ),
        "baseline_saved_at": baseline.get("saved_at", ""),
        "before_week_start": before_key,
        "before_week_label": baseline.get(
            "before_week_label",
            f"{before_start:%d.%m.%Y} – {before_end:%d.%m.%Y}",
        ),
        "after_week_start": after_key,
        "after_week_label": f"{after_start:%d.%m.%Y} – {after_end:%d.%m.%Y}",
        "changed_count": len(rows),
        "improved_count": improved,
        "worsened_count": worsened,
        "rows": rows,
    }


def build_week_payload(
    week_start: date, week_end: date, df_summary: pd.DataFrame, df_orders: pd.DataFrame
) -> dict:
    partners: dict[str, dict] = {}

    for _, row in df_summary.iterrows():
        brand = str(row["brand_name"] or "—")
        city = str(row["city_name"] or "—")
        key = f"{brand}|||{city}"
        delivered = int(row["delivered"] or 0)
        bad = int(row["bad_count"] or 0)
        failed = int(row["failed_count"] or 0)
        partners[key] = {
            "brand": brand,
            "city": city,
            "city_ua": city_ua(city),
            "delivered": delivered,
            "bad_count": bad,
            "failed_count": failed,
            "bad_pct": round(bad / delivered * 100, 2) if delivered else 0,
            "failed_pct": round(failed / delivered * 100, 2) if delivered else 0,
            "failed_by_fault": defaultdict(int),
            "bad_by_actor": defaultdict(int),
            "bad_by_reason": defaultdict(int),
            "failed_orders": [],
            "bad_orders": [],
        }

    for _, r in df_orders.iterrows():
        brand = str(r["brand_name"] or "—")
        city = str(r["city_name"] or "—")
        key = f"{brand}|||{city}"
        if key not in partners:
            partners[key] = {
                "brand": brand,
                "city": city,
                "city_ua": city_ua(city),
                "delivered": 0,
                "bad_count": 0,
                "failed_count": 0,
                "bad_pct": 0,
                "failed_pct": 0,
                "failed_by_fault": defaultdict(int),
                "bad_by_actor": defaultdict(int),
                "bad_by_reason": defaultdict(int),
                "failed_orders": [],
                "bad_orders": [],
            }
        p = partners[key]
        state = str(r.get("final_state") or "")
        order_rec = {
            "order_id": int(r["order_id"]),
            "order_ref": str(r.get("order_ref") or "—"),
            "location": str(r.get("provider_name") or "—"),
            "state": state,
            "created": str(r.get("order_created") or ""),
            "rating": None if pd.isna(r.get("order_food_rating_value")) else float(r["order_food_rating_value"]),
            "from_state": str(r.get("from_state") or ""),
            "courier_rejects": int(r.get("number_courier_rejects") or 0),
            "eater_cancelled": r.get("has_eater_cancellation_ticket") is True,
        }

        if state in ("failed", "rejected"):
            fault = classify_failed(r)
            p["failed_by_fault"][fault] += 1
            reason_code = r.get("bad_order_main_reason")
            has_reason = reason_code is not None and not (
                isinstance(reason_code, float) and pd.isna(reason_code)
            )
            order_rec["fault"] = fault
            order_rec["culprit_ua"] = actor_ua(fault if fault != "client" else "eater")
            order_rec["reason_ua"] = (
                reason_ua(reason_code) if has_reason else FAULT_REASON_UA.get(fault, fault)
            )
            order_rec["comment"] = failed_detail_ua(r)
            p["failed_orders"].append(order_rec)

        actor = r.get("bad_order_actor_at_fault")
        actor_key = str(actor).lower() if actor is not None and not (isinstance(actor, float) and pd.isna(actor)) else "unknown"
        reason_code = r.get("bad_order_main_reason")
        reason_key = str(reason_code) if reason_code is not None and not (isinstance(reason_code, float) and pd.isna(reason_code)) else "none"

        p["bad_by_actor"][actor_key] += 1
        p["bad_by_reason"][reason_key] += 1

        bad_rec = {
            **order_rec,
            "actor": actor_key,
            "culprit_ua": actor_ua(actor_key if actor_key != "unknown" else None),
            "reason_code": reason_key,
            "reason_ua": reason_ua(reason_code),
            "comment": bad_comment_ua(r),
        }
        p["bad_orders"].append(bad_rec)

    # Convert defaultdicts to plain dicts for JSON
    out_partners = {}
    for key, p in partners.items():
        if p["bad_count"] == 0 and p["failed_count"] == 0 and not p["bad_orders"]:
            continue
        out_partners[key] = {
            **{k: v for k, v in p.items() if k not in ("failed_by_fault", "bad_by_actor", "bad_by_reason")},
            "failed_by_fault": dict(p["failed_by_fault"]),
            "bad_by_actor": {actor_ua(k): v for k, v in p["bad_by_actor"].items()},
            "bad_by_reason": {reason_ua(k if k != "none" else None): v for k, v in p["bad_by_reason"].items()},
        }

    cities = sorted({p["city_ua"] for p in out_partners.values()}, key=str.lower)
    brands = sorted({p["brand"] for p in out_partners.values()}, key=str.lower)

    total_delivered = int(df_summary["delivered"].sum()) if len(df_summary) else 0
    total_bad = int(df_summary["bad_count"].sum()) if len(df_summary) else 0
    total_failed = int(df_summary["failed_count"].sum()) if len(df_summary) else 0

    return {
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "label": f"{week_start:%d.%m.%Y} – {week_end:%d.%m.%Y}",
        "portfolio": {
            "delivered": total_delivered,
            "bad_count": total_bad,
            "failed_count": total_failed,
            "bad_pct": round(total_bad / total_delivered * 100, 2) if total_delivered else 0,
            "failed_pct": round(total_failed / total_delivered * 100, 2) if total_delivered else 0,
        },
        "cities": cities,
        "brands": brands,
        "partners": out_partners,
    }


# Текстові описи етапу зриву, які могли зберегтися у звітах попередніх тижнів
LEGACY_STAGE_TEXT = {
    "зрив після прийняття": "waiting_acceptance",
    "заклад не прийняв замовлення вчасно": "waiting_acceptance",
    "заклад не почав приготування": "waiting_starting_preparation",
    "зрив під час приготування": "waiting_preparation",
    "кур'єр не забрав": "ready_for_pickup",
    "зрив на етапі доставки": "waiting_delivery",
    "зрив на етапі оплати": "waiting_payment",
}


def parse_failed_comment(comment) -> tuple[str, int, bool]:
    """Відновлює (етап, відмови кур'єра, скасування клієнтом) з текстового коментаря."""
    text = str(comment or "")
    from_state = ""
    for pattern in (r"етап:\s*([a-z_]+)", r"«([a-z_]+)»"):
        m = re.search(pattern, text)
        if m:
            from_state = m.group(1)
            break
    if not from_state:
        low = text.lower()
        for phrase, state in LEGACY_STAGE_TEXT.items():
            if phrase in low:
                from_state = state
                break
    m = re.search(r"відмови кур'єра:\s*(\d+)", text)
    rejects = int(m.group(1)) if m else 0
    return from_state, rejects, "скасування з боку клієнта" in text


def failed_signals(rec: dict, failed_lookup: dict[int, dict]) -> tuple[str, int, bool]:
    """Сигнали зриву: із самого запису, інакше з коментаря парного failed-запису."""
    if rec.get("from_state") is not None and rec.get("courier_rejects") is not None:
        return (
            str(rec.get("from_state") or ""),
            int(rec.get("courier_rejects") or 0),
            bool(rec.get("eater_cancelled")),
        )
    twin = failed_lookup.get(rec.get("order_id")) or {}
    return parse_failed_comment(twin.get("comment") or rec.get("comment"))


def late_reason_from_comment(comment) -> str | None:
    """У коментарі попередніх версій причина затримки лишилась англійською: «unknown delay dropoff»."""
    for part in str(comment or "").split(";"):
        candidate = norm_code(part.strip().replace(" ", "_").lower())
        if candidate in REASON_UA and candidate not in ("none", "unknown"):
            return candidate
    return None


def extract_late_reason(rec: dict) -> str | None:
    """Причина затримки доставки, збережена один раз, щоб перегенерація була ідемпотентною."""
    if "late_reason" in rec:
        return rec["late_reason"] or None
    late = late_reason_from_comment(rec.get("comment"))
    rec["late_reason"] = late or ""
    return late


# Коди, які самі по собі не кажуть, хто відповідальний
VAGUE_CODES = ("order_took_longer", "unknown_delay_dropoff", "unknown_delay_pickup")


def platform_group(codes: list[str]) -> str:
    for code in codes:
        if code in PLATFORM_GROUP:
            return PLATFORM_GROUP[code]
    return "other"


def classify_order(rec: dict, failed_lookup: dict[int, dict]) -> dict:
    """
    Визначає зону відповідальності для одного поганого замовлення.

    Пріоритет: атрибуція моделі Bolt → етап зриву → код причини →
    причина затримки з коментаря → низька оцінка їжі.
    """
    codes = split_codes(rec.get("reason_code"))
    state = str(rec.get("state") or "")
    actor = str(rec.get("actor") or "unknown").lower()
    late = extract_late_reason(rec)

    # Якщо основний код нічого не пояснює, беремо конкретну причину затримки доставки
    if late and (not codes or all(c in VAGUE_CODES for c in codes)):
        codes = [late] + [c for c in codes if c != late]

    if actor in ACTOR_RESP:
        return {"resp": ACTOR_RESP[actor], "derived": "", "codes": codes}

    if state in ("failed", "rejected"):
        from_state, rejects, cancelled = failed_signals(rec, failed_lookup)
        resp = classify_stage(state, from_state, rejects, cancelled)
        if state == "rejected":
            note = "визначено за статусом замовлення"
        elif rejects:
            note = f"визначено за відмовами кур'єрів ({rejects})"
        elif cancelled:
            note = "визначено за скасуванням клієнта"
        else:
            note = "визначено за етапом зриву"
        return {"resp": resp, "derived": note, "codes": codes, "from_state": from_state}

    for code in codes:
        if code in REASON_RESP:
            return {
                "resp": REASON_RESP[code],
                "derived": "визначено за кодом причини",
                "codes": codes,
            }

    if late and late in REASON_RESP:
        return {
            "resp": REASON_RESP[late],
            "derived": "визначено за причиною затримки доставки",
            "codes": codes or [late],
        }

    rating = rec.get("rating")
    if rating is not None and float(rating) <= 2:
        return {
            "resp": "provider",
            "derived": f"визначено за оцінкою їжі {int(float(rating))}/5",
            "codes": codes,
        }

    return {"resp": "unassigned", "derived": "", "codes": codes or ([late] if late else [])}


def describe_reason(rec: dict, info: dict) -> str:
    codes = info.get("codes") or []
    if codes:
        label = reason_ua(",".join(codes))
        if info.get("resp") == "client":
            # Модель віднесла звернення до клієнта: скарга не підтвердилась
            # або ситуацію спричинив сам клієнт.
            return "Скарга клієнта: " + re.sub(r"^(Заклад|Кур'єр|Bolt|Клієнт):\s*", "", label)
        return label
    state = str(rec.get("state") or "")
    if state in ("failed", "rejected"):
        from_state = info.get("from_state") or ""
        if state == "rejected":
            return "Заклад відхилив замовлення"
        return f"Зрив замовлення: {stage_ua(from_state)}"
    return REASON_UA["none"]


def describe_comment(rec: dict, reason_label: str) -> str:
    """Деталі доставленого поганого замовлення: оцінка їжі та причина затримки."""
    parts: list[str] = []
    rating = rec.get("rating")
    if rating is not None and float(rating) <= 2:
        parts.append(f"Оцінка їжі: {int(float(rating))}/5")
    late = extract_late_reason(rec)
    if late:
        label = REASON_UA.get(late, late.replace("_", " "))
        if label not in reason_label:
            parts.append(f"Причина затримки: {label}")
    return "; ".join(parts) if parts else "—"


def enrich_week_payload(week: dict) -> dict:
    """
    Перекласифіковує вже зібраний тиждень: зона відповідальності, підгрупа платформи,
    людські назви причин. Працює і на даних, вивантажених попередніми версіями звіту,
    тому звіт можна перебудувати без повторного запиту в Databricks.
    """
    for partner in week.get("partners", {}).values():
        failed_lookup = {o.get("order_id"): o for o in partner.get("failed_orders") or []}

        for rec in partner.get("failed_orders") or []:
            state = str(rec.get("state") or "")
            from_state, rejects, cancelled = failed_signals(rec, failed_lookup)
            resp = "provider" if state == "rejected" else classify_stage(
                state, from_state, rejects, cancelled
            )
            rec["from_state"] = from_state
            rec["courier_rejects"] = rejects
            rec["eater_cancelled"] = cancelled
            rec["resp"] = resp
            rec["resp_ua"] = RESP_UA[resp]
            rec["culprit_ua"] = RESP_UA[resp]
            rec["stage_ua"] = (
                "заклад відхилив замовлення" if state == "rejected" else stage_ua(from_state)
            )
            extra: list[str] = []
            if rejects:
                extra.append(f"відмови кур'єра: {rejects}")
            if cancelled:
                extra.append("є скасування з боку клієнта")
            rec["extra_ua"] = "; ".join(extra) if extra else "—"
            rec["comment"] = "; ".join([rec["stage_ua"], *extra])
            codes = split_codes(rec.get("reason_code"))
            rec["reason_ua"] = (
                reason_ua(",".join(codes)) if codes else FAULT_REASON_UA.get(resp, RESP_UA[resp])
            )

        by_resp: dict[str, int] = defaultdict(int)
        by_platform: dict[str, int] = defaultdict(int)
        by_reason: dict[str, int] = defaultdict(int)
        by_stage: dict[str, int] = defaultdict(int)

        for rec in partner.get("bad_orders") or []:
            info = classify_order(rec, failed_lookup)
            resp = info["resp"]
            rec["resp"] = resp
            rec["resp_ua"] = RESP_UA[resp]
            rec["culprit_ua"] = RESP_UA[resp]
            rec["derived"] = info.get("derived") or ""
            rec["actor_ua"] = actor_ua(rec.get("actor"))
            rec["reason_ua"] = describe_reason(rec, info)
            if str(rec.get("state")) not in ("failed", "rejected"):
                rec["comment"] = describe_comment(rec, rec["reason_ua"])
            else:
                twin = failed_lookup.get(rec.get("order_id")) or {}
                rec["comment"] = twin.get("extra_ua") or "—"
            if resp == "platform":
                group = platform_group(info.get("codes") or [])
                rec["platform_group"] = group
                rec["platform_group_ua"] = PLATFORM_GROUP_UA[group]
                by_platform[group] += 1
            else:
                rec["platform_group"] = ""
                rec["platform_group_ua"] = ""
            if rec.get("derived") and str(rec.get("state")) in ("failed", "rejected"):
                by_stage[info.get("from_state") or ""] += 1
            by_resp[resp] += 1
            by_reason[rec["reason_ua"]] += 1

        partner["bad_by_resp"] = {k: by_resp[k] for k in RESP_ORDER if by_resp.get(k)}
        partner["bad_by_platform"] = dict(
            sorted(by_platform.items(), key=lambda kv: kv[1], reverse=True)
        )
        partner["bad_by_reason"] = dict(sorted(by_reason.items(), key=lambda kv: kv[1], reverse=True))
        partner["bad_by_actor"] = {
            actor_ua(k): v
            for k, v in sorted(
                _count_actors(partner.get("bad_orders") or []).items(),
                key=lambda kv: kv[1],
                reverse=True,
            )
        }
        failed_resp: dict[str, int] = defaultdict(int)
        for rec in partner.get("failed_orders") or []:
            failed_resp[rec["resp"]] += 1
        partner["failed_by_fault"] = {k: failed_resp[k] for k in RESP_ORDER if failed_resp.get(k)}

    week["portfolio"] = {**week.get("portfolio", {}), **_portfolio_resp_totals(week)}
    return week


def _count_actors(records: list[dict]) -> dict[str, int]:
    out: dict[str, int] = defaultdict(int)
    for rec in records:
        out[str(rec.get("actor") or "unknown").lower()] += 1
    return dict(out)


def _portfolio_resp_totals(week: dict) -> dict:
    by_resp: dict[str, int] = defaultdict(int)
    by_platform: dict[str, int] = defaultdict(int)
    for partner in week.get("partners", {}).values():
        for key, value in (partner.get("bad_by_resp") or {}).items():
            by_resp[key] += value
        for key, value in (partner.get("bad_by_platform") or {}).items():
            by_platform[key] += value
    return {
        "bad_by_resp": {k: by_resp[k] for k in RESP_ORDER if by_resp.get(k)},
        "bad_by_platform": dict(sorted(by_platform.items(), key=lambda kv: kv[1], reverse=True)),
    }


def load_existing_weeks(html_path: Path) -> dict[str, dict]:
    if not html_path.exists():
        return {}
    text = html_path.read_text(encoding="utf-8")
    m = re.search(r"const REPORT_WEEKS = (\{.*?\});\s*\n", text, re.DOTALL)
    if not m:
        return {}
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return {}


def load_existing_prep(html_path: Path) -> dict:
    if not html_path.exists():
        return empty_prep_payload()
    text = html_path.read_text(encoding="utf-8")
    m = re.search(r"const PREP_TIME = (\{.*?\});\s*\n", text, re.DOTALL)
    if not m:
        return empty_prep_payload()
    try:
        data = json.loads(m.group(1))
        if not isinstance(data, dict):
            return empty_prep_payload()
        data.setdefault("rows", [])
        return data
    except json.JSONDecodeError:
        return empty_prep_payload()


def load_existing_cooking_impact(html_path: Path) -> dict:
    if not html_path.exists():
        return empty_cooking_impact()
    text = html_path.read_text(encoding="utf-8")
    m = re.search(r"const COOKING_IMPACT = (\{.*?\});\s*\n", text, re.DOTALL)
    if not m:
        return empty_cooking_impact()
    try:
        data = json.loads(m.group(1))
        return data if isinstance(data, dict) else empty_cooking_impact()
    except json.JSONDecodeError:
        return empty_cooking_impact()


def build_html(
    weeks_data: dict[str, dict],
    prep_data: dict,
    generated_at: str,
    cooking_impact: dict | None = None,
) -> str:
    weeks_json = json.dumps(weeks_data, ensure_ascii=False, separators=(",", ":"))
    prep_json = json.dumps(prep_data or empty_prep_payload(), ensure_ascii=False, separators=(",", ":"))
    impact_json = json.dumps(
        cooking_impact or empty_cooking_impact(), ensure_ascii=False, separators=(",", ":")
    )
    resp_json = json.dumps(RESP_UA, ensure_ascii=False, separators=(",", ":"))
    resp_order_json = json.dumps(list(RESP_ORDER), ensure_ascii=False, separators=(",", ":"))
    platform_group_json = json.dumps(PLATFORM_GROUP_UA, ensure_ascii=False, separators=(",", ":"))
    platform_hint_json = json.dumps(PLATFORM_GROUP_HINT, ensure_ascii=False, separators=(",", ":"))
    week_keys = sorted(weeks_data.keys(), reverse=True)
    default_week = week_keys[0] if week_keys else ""

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Bad Orders — Marharyta Zhytnyk Portfolio</title>
  <style>
    :root {{
      --green:#1DC462; --dark:#1A1A1A; --bg:#F7F9FC; --card:#fff;
      --border:#E0E0E0; --text:#222; --muted:#666;
      --bolt:#2563EB; --courier:#9333EA; --provider:#EA580C; --client:#64748B;
      --fail:#E53935;
    }}
    * {{ box-sizing:border-box; margin:0; padding:0; }}
    body {{
      font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
      background:var(--bg); color:var(--text); line-height:1.5;
    }}
    .header {{
      background:var(--dark); color:#fff; padding:18px 28px;
      position:sticky; top:0; z-index:100; box-shadow:0 2px 12px rgba(0,0,0,.2);
    }}
    .header h1 {{ font-size:1.2rem; color:var(--green); margin-bottom:4px; }}
    .header p {{ font-size:.82rem; color:#aaa; }}
    .view-tabs {{
      display:flex; gap:8px; margin-top:14px; flex-wrap:wrap;
    }}
    .view-tabs button {{
      background:#2a2a2a; color:#bbb; border:1px solid #444; border-radius:8px;
      padding:8px 16px; font-size:.88rem; cursor:pointer; font-weight:600;
    }}
    .view-tabs button.active {{
      background:var(--green); color:#fff; border-color:var(--green);
    }}
    .filters {{
      display:flex; flex-wrap:wrap; gap:12px; margin-top:14px; align-items:flex-end;
    }}
    .filters label {{ font-size:.72rem; color:#bbb; display:block; margin-bottom:4px; }}
    .filters select, .filters input {{
      background:#2a2a2a; color:#fff; border:1px solid #444; border-radius:8px;
      padding:8px 12px; font-size:.88rem; min-width:180px;
    }}
    .filters input {{ min-width:220px; }}
    .wrap {{ max-width:1200px; margin:0 auto; padding:24px 20px 48px; }}
    .meta {{ color:var(--muted); font-size:.82rem; margin-bottom:20px; }}
    .kpi-row {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:14px; margin-bottom:24px; }}
    .kpi {{
      background:var(--card); border:1px solid var(--border); border-radius:12px;
      padding:16px 18px; box-shadow:0 1px 4px rgba(0,0,0,.04);
    }}
    .kpi .n {{ font-size:1.8rem; font-weight:800; }}
    .kpi .l {{ font-size:.72rem; color:var(--muted); text-transform:uppercase; letter-spacing:.04em; margin-top:4px; }}
    .kpi.bad .n {{ color:var(--fail); }}
    .kpi.fail .n {{ color:#FB8C00; }}
    h2 {{
      font-size:1rem; margin:28px 0 12px; padding-left:10px;
      border-left:4px solid var(--green);
    }}
    h2.fail-h {{ border-left-color:#FB8C00; }}
    h2.bad-h {{ border-left-color:var(--fail); }}
    .grid-2 {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
    @media(max-width:768px) {{ .grid-2 {{ grid-template-columns:1fr; }} }}
    .card {{
      background:var(--card); border:1px solid var(--border); border-radius:12px;
      padding:16px; box-shadow:0 1px 4px rgba(0,0,0,.04);
    }}
    .bar-row {{ display:flex; align-items:center; gap:10px; margin:8px 0; font-size:.85rem; }}
    .bar-label {{ min-width:140px; flex-shrink:0; }}
    .bar-track {{ flex:1; height:8px; background:#eee; border-radius:4px; overflow:hidden; }}
    .bar-fill {{ height:100%; border-radius:4px; }}
    .bar-fill.bolt {{ background:var(--bolt); }}
    .bar-fill.courier {{ background:var(--courier); }}
    .bar-fill.provider {{ background:var(--provider); }}
    .bar-fill.client {{ background:var(--client); }}
    .bar-fill.unknown {{ background:#B0B0B0; }}
    .derived-note {{
      display:block; font-size:.7rem; color:#8A8A8A; margin-top:2px;
    }}
    .bar-val {{ min-width:36px; text-align:right; font-weight:700; }}
    .reason-list {{ list-style:none; }}
    .reason-list li {{
      display:flex; justify-content:space-between; gap:12px;
      padding:8px 0; border-bottom:1px solid #f0f0f0; font-size:.84rem;
    }}
    .reason-list li:last-child {{ border-bottom:none; }}
    .btn-detail {{
      background:var(--green); color:#fff; border:none; border-radius:8px;
      padding:10px 20px; font-size:.9rem; font-weight:600; cursor:pointer; margin-top:8px;
    }}
    .btn-detail:hover {{ filter:brightness(1.05); }}
    .btn-partner {{
      background:#fff; color:var(--provider); border:2px solid var(--provider);
      border-radius:8px; padding:10px 20px; font-size:.9rem; font-weight:600;
      cursor:pointer; margin-top:8px; margin-left:10px;
    }}
    .btn-partner:hover {{ background:#FFF7ED; }}
    .modal-overlay {{
      display:none; position:fixed; inset:0; background:rgba(0,0,0,.45);
      z-index:200; align-items:center; justify-content:center; padding:20px;
    }}
    .modal-overlay.open {{ display:flex; }}
    .modal-box {{
      background:#fff; border-radius:14px; width:min(720px,100%);
      max-height:85vh; display:flex; flex-direction:column;
      box-shadow:0 12px 40px rgba(0,0,0,.2);
    }}
    .modal-head {{
      padding:16px 20px; border-bottom:1px solid var(--border);
      display:flex; justify-content:space-between; align-items:center; gap:12px;
    }}
    .modal-head h3 {{ font-size:1rem; }}
    .modal-close {{
      background:none; border:none; font-size:1.4rem; cursor:pointer; color:var(--muted);
    }}
    .modal-body {{ padding:16px 20px; overflow:auto; flex:1; }}
    .modal-body textarea {{
      width:100%; min-height:320px; border:1px solid var(--border); border-radius:10px;
      padding:12px; font-size:.85rem; line-height:1.55; resize:vertical; font-family:inherit;
    }}
    .modal-actions {{
      padding:12px 20px 18px; display:flex; gap:10px; justify-content:flex-end;
      border-top:1px solid var(--border);
    }}
    .btn-copy {{
      background:var(--green); color:#fff; border:none; border-radius:8px;
      padding:9px 18px; font-weight:600; cursor:pointer;
    }}
    .btn-close-modal {{
      background:#f0f0f0; color:var(--text); border:none; border-radius:8px;
      padding:9px 18px; cursor:pointer;
    }}
    .detail-panel {{
      display:none; margin-top:16px; background:var(--card);
      border:1px solid var(--border); border-radius:12px; overflow:hidden;
    }}
    .detail-panel.open {{ display:block; }}
    .detail-tabs {{ display:flex; gap:0; border-bottom:1px solid var(--border); flex-wrap:wrap; }}
    .detail-tabs button {{
      background:none; border:none; padding:10px 16px; cursor:pointer;
      font-size:.82rem; color:var(--muted); border-bottom:2px solid transparent;
    }}
    .detail-tabs button.active {{ color:var(--green); border-bottom-color:var(--green); font-weight:600; }}
    .detail-toolbar {{
      display:flex; flex-wrap:wrap; align-items:flex-end; gap:12px;
      padding:12px 16px; border-bottom:1px solid var(--border); background:#fafafa;
    }}
    .detail-toolbar label {{ font-size:.72rem; color:var(--muted); display:block; margin-bottom:4px; }}
    .detail-toolbar select {{
      border:1px solid var(--border); border-radius:8px; padding:7px 12px;
      font-size:.85rem; min-width:200px; background:#fff;
    }}
    .detail-count {{ font-size:.78rem; color:var(--muted); margin-left:auto; }}
    table {{ width:100%; border-collapse:collapse; font-size:.8rem; }}
    th {{
      text-align:left; padding:10px 12px; background:#f5f5f5;
      color:var(--muted); font-weight:600; border-bottom:1px solid var(--border);
      white-space:nowrap;
    }}
    td {{ padding:8px 12px; border-bottom:1px solid #f0f0f0; vertical-align:top; }}
    tr:hover td {{ background:#fafafa; }}
    .mono {{ font-family:ui-monospace,monospace; }}
    .num {{ font-variant-numeric:tabular-nums; text-align:right; white-space:nowrap; }}
    .tag {{
      display:inline-block; padding:2px 8px; border-radius:6px;
      font-size:.72rem; font-weight:700;
    }}
    .tag-failed {{ background:#FFF3E0; color:#E65100; }}
    .tag-rejected {{ background:#FFEBEE; color:#C62828; }}
    .tag-provider {{ background:#FFF1E6; color:#C2410C; }}
    .tag-courier {{ background:#F3E8FF; color:#7E22CE; }}
    .tag-bolt {{ background:#E0EAFF; color:#1D4ED8; }}
    .tag-client {{ background:#EEF2F6; color:#475569; }}
    .tag-unknown {{ background:#F0F0F0; color:#616161; }}
    .diff-pos {{
      background:#FFEBEE; color:#C62828; font-weight:700;
      padding:3px 8px; border-radius:6px; display:inline-block;
    }}
    .diff-neg {{
      background:#E8F5E9; color:#2E7D32; font-weight:700;
      padding:3px 8px; border-radius:6px; display:inline-block;
    }}
    .diff-zero {{
      background:#f0f0f0; color:#555; font-weight:600;
      padding:3px 8px; border-radius:6px; display:inline-block;
    }}
    .rec-change {{
      background:#E3F2FD; color:#1565C0; font-weight:700;
      padding:3px 8px; border-radius:6px; display:inline-block;
    }}
    .rec-ok {{
      background:#E8F5E9; color:#2E7D32; font-weight:600;
      padding:3px 8px; border-radius:6px; display:inline-block;
    }}
    .prep-table-wrap {{
      background:var(--card); border:1px solid var(--border); border-radius:12px;
      overflow:auto; box-shadow:0 1px 4px rgba(0,0,0,.04);
    }}
    .prep-legend {{
      display:flex; flex-wrap:wrap; gap:14px; font-size:.8rem; color:var(--muted);
      margin:8px 0 16px;
    }}
    .empty {{ color:var(--muted); text-align:center; padding:40px 20px; }}
    .portfolio-note {{
      background:#E8F9EE; border:1px solid #b8e6c8; border-radius:10px;
      padding:12px 16px; font-size:.84rem; margin-bottom:20px;
    }}
  </style>
</head>
<body>
  <div class="header">
    <h1>Аналіз Bad Orders — Портфоліо Marharyta Zhytnyk</h1>
    <p>Щотижневий розбір поганих та невдалих замовлень · Bolt Food Ukraine</p>
    <div class="view-tabs">
      <button type="button" class="active" data-view="bad">Bad Orders</button>
      <button type="button" data-view="prep">Preparation time</button>
    </div>
    <div class="filters" id="filtersBad">
      <div>
        <label>Тиждень</label>
        <select id="selWeek"></select>
      </div>
      <div>
        <label>Місто</label>
        <select id="selCity"><option value="">— Усі міста —</option></select>
      </div>
      <div>
        <label>Бренд (партнер)</label>
        <select id="selBrand"><option value="">— Оберіть партнера —</option></select>
      </div>
    </div>
    <div class="filters" id="filtersPrep" style="display:none">
      <div>
        <label>Місто</label>
        <select id="selPrepCity"><option value="">— Усі міста —</option></select>
      </div>
      <div>
        <label>Пошук провайдера</label>
        <input id="prepSearch" type="search" placeholder="Назва або Provider ID" />
      </div>
    </div>
  </div>

  <div class="wrap">
    <div class="meta" id="metaLine">Згенеровано: {html.escape(generated_at)} UTC</div>
    <div id="viewBad">
      <div id="portfolioKpis"></div>
      <div id="content">
        <div class="empty">Оберіть місто та бренд, щоб переглянути деталі.</div>
      </div>
    </div>
    <div id="viewPrep" style="display:none"></div>
  </div>

  <div class="modal-overlay" id="partnerModal">
    <div class="modal-box">
      <div class="modal-head">
        <h3>Повідомлення для партнера</h3>
        <button type="button" class="modal-close" id="btnClosePartnerModal" title="Закрити">&times;</button>
      </div>
      <div class="modal-body">
        <textarea id="partnerMessageText" readonly></textarea>
      </div>
      <div class="modal-actions">
        <button type="button" class="btn-copy" id="btnCopyPartnerMsg">Скопіювати текст</button>
        <button type="button" class="btn-close-modal" id="btnClosePartnerModal2">Закрити</button>
      </div>
    </div>
  </div>

  <script>
  const REPORT_WEEKS = {weeks_json};
  const PREP_TIME = {prep_json};
  const COOKING_IMPACT = {impact_json};

  const RESP_UA = {resp_json};
  const RESP_ORDER = {resp_order_json};
  const PLATFORM_GROUP_UA = {platform_group_json};
  const PLATFORM_GROUP_HINT = {platform_hint_json};

  const RESP_CLASS = {{
    provider: 'provider',
    courier: 'courier',
    platform: 'bolt',
    client: 'client',
    unassigned: 'unknown'
  }};

  const FAULT_CLASS = {{
    'Платформа Bolt': 'bolt',
    'Кур\\'єр': 'courier',
    'Заклад': 'provider',
    'Клієнт': 'client',
    'Не атрибутовано': 'unknown'
  }};

  let activeView = 'bad';

  function $(id) {{ return document.getElementById(id); }}

  function fmtMin(v) {{
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    return Number(v).toFixed(1);
  }}

  function fmtDiff(v) {{
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    const n = Number(v);
    const sign = n > 0 ? '+' : '';
    const cls = Math.abs(n) < 0.05 ? 'diff-zero' : (n > 0 ? 'diff-pos' : 'diff-neg');
    return `<span class="${{cls}}">${{sign}}${{n.toFixed(1)}} хв</span>`;
  }}

  function recommendCooking(actual) {{
    if (actual === null || actual === undefined || Number.isNaN(Number(actual))) return null;
    const rounded = Math.round(Math.max(5, Number(actual)) / 5) * 5;
    return Math.min(rounded, 90);
  }}

  function fmtRecommended(row) {{
    const rec = (row.recommended_cooking_min != null)
      ? Number(row.recommended_cooking_min)
      : recommendCooking(row.actual_prep_min);
    if (rec === null || rec === undefined || Number.isNaN(rec)) return '—';
    const cooking = Number(row.cooking_time_min);
    const differs = !Number.isNaN(cooking) && Math.abs(cooking - rec) >= 5;
    const cls = differs ? 'rec-change' : 'rec-ok';
    const hint = differs
      ? ` <span style="color:#888;font-weight:500;font-size:.72rem">(зараз ${{fmtMin(cooking)}})</span>`
      : '';
    return `<span class="${{cls}}">${{rec}} хв</span>${{hint}}`;
  }}

  function setView(view) {{
    activeView = view;
    document.querySelectorAll('.view-tabs button').forEach(b => {{
      b.classList.toggle('active', b.dataset.view === view);
    }});
    $('filtersBad').style.display = view === 'bad' ? 'flex' : 'none';
    $('filtersPrep').style.display = view === 'prep' ? 'flex' : 'none';
    $('viewBad').style.display = view === 'bad' ? 'block' : 'none';
    $('viewPrep').style.display = view === 'prep' ? 'block' : 'none';
    if (view === 'prep') renderPrep();
  }}

  function initPrepFilters() {{
    const cities = [...new Set((PREP_TIME.rows || []).map(r => r.city_ua))].sort((a,b) => a.localeCompare(b,'uk'));
    const sel = $('selPrepCity');
    cities.forEach(c => {{
      const o = document.createElement('option');
      o.value = c; o.textContent = c;
      sel.appendChild(o);
    }});
    sel.addEventListener('change', renderPrep);
    $('prepSearch').addEventListener('input', renderPrep);
  }}

  function fmtPp(v) {{
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    const n = Number(v);
    const sign = n > 0 ? '+' : '';
    const cls = Math.abs(n) < 0.05 ? 'diff-zero' : (n > 0 ? 'diff-pos' : 'diff-neg');
    return `<span class="${{cls}}">${{sign}}${{n.toFixed(2)}} п.п.</span>`;
  }}

  function renderCookingImpact() {{
    const impact = COOKING_IMPACT || {{}};
    if (!impact.ready) {{
      return `<div class="portfolio-note" style="background:#FFF8E1;border-color:#FFE082;margin-top:20px">
        <strong>Вплив змін cooking time на Bad Orders</strong><br>
        ${{impact.message || 'Очікуємо дані наступного тижня.'}}
        ${{impact.before_week_label ? `<br>Базовий тиждень (до змін): <strong>${{impact.before_week_label}}</strong>` : ''}}
      </div>`;
    }}
    const rows = impact.rows || [];
    const table = rows.length ? `<div class="prep-table-wrap" style="margin-top:12px"><table>
      <thead><tr>
        <th>Provider name</th>
        <th>Provider ID</th>
        <th>Cooking до</th>
        <th>Cooking після</th>
        <th>Δ cooking</th>
        <th>Bad % до</th>
        <th>Bad % після</th>
        <th>Δ Bad Orders</th>
        <th>Prep-related % до</th>
        <th>Prep-related % після</th>
        <th>Δ Prep-related</th>
        <th>Місто</th>
      </tr></thead>
      <tbody>${{rows.map(r => `<tr>
        <td>${{r.provider_name || '—'}}</td>
        <td class="mono">${{r.provider_id}}</td>
        <td class="num">${{fmtMin(r.cooking_before)}}</td>
        <td class="num">${{fmtMin(r.cooking_after)}}</td>
        <td class="num">${{fmtDiff(r.delta_cooking)}}</td>
        <td class="num">${{r.bad_pct_before != null ? r.bad_pct_before.toFixed(2) + '%' : '—'}}</td>
        <td class="num">${{r.bad_pct_after != null ? r.bad_pct_after.toFixed(2) + '%' : '—'}}</td>
        <td class="num">${{fmtPp(r.delta_bad_pp)}}</td>
        <td class="num">${{r.prep_related_pct_before != null ? r.prep_related_pct_before.toFixed(2) + '%' : '—'}}</td>
        <td class="num">${{r.prep_related_pct_after != null ? r.prep_related_pct_after.toFixed(2) + '%' : '—'}}</td>
        <td class="num">${{fmtPp(r.delta_prep_related_pp)}}</td>
        <td>${{r.city_ua || '—'}}</td>
      </tr>`).join('')}}</tbody>
    </table></div>` : '<p class="empty">Не знайдено провайдерів зі зміною cooking time ≥ 5 хв.</p>';

    return `
      <h2 style="margin-top:28px">Вплив змін cooking time на Bad Orders</h2>
      <div class="portfolio-note">
        ${{impact.message || ''}}<br>
        До: <strong>${{impact.before_week_label || '—'}}</strong>
        · Після: <strong>${{impact.after_week_label || '—'}}</strong>
        ${{impact.baseline_saved_at ? `<br><span style="color:#666">Baseline cooking time: ${{impact.baseline_saved_at}}</span>` : ''}}
      </div>
      <div class="kpi-row">
        <div class="kpi"><div class="n">${{impact.changed_count || 0}}</div><div class="l">Змінили cooking time</div></div>
        <div class="kpi"><div class="n" style="color:#2E7D32">${{impact.improved_count || 0}}</div><div class="l">Bad Orders покращився</div></div>
        <div class="kpi bad"><div class="n">${{impact.worsened_count || 0}}</div><div class="l">Bad Orders погіршився</div></div>
      </div>
      <div class="prep-legend">
        <span>Δ Bad Orders у відсоткових пунктах (мінус = менше bad orders — добре)</span>
        <span>Prep-related — bad orders з причинами затримки / ETA приготування</span>
      </div>
      ${{table}}
    `;
  }}

  function renderPrep() {{
    const city = $('selPrepCity').value;
    const q = ($('prepSearch').value || '').trim().toLowerCase();
    let rows = [...(PREP_TIME.rows || [])];
    if (city) rows = rows.filter(r => r.city_ua === city);
    if (q) {{
      rows = rows.filter(r =>
        String(r.provider_name || '').toLowerCase().includes(q) ||
        String(r.provider_id || '').includes(q) ||
        String(r.brand_name || '').toLowerCase().includes(q)
      );
    }}

    const total = rows.length;
    const slower = rows.filter(r => (r.diff_min || 0) > 2).length;
    const faster = rows.filter(r => (r.diff_min || 0) < -2).length;
    const avgDiff = total
      ? (rows.reduce((s, r) => s + (r.diff_min || 0), 0) / total)
      : 0;

    const tableRows = rows.map(r => `<tr>
      <td>${{r.provider_name || '—'}}</td>
      <td class="mono">${{r.provider_id}}</td>
      <td class="num">${{fmtMin(r.cooking_time_min)}}</td>
      <td class="num">${{fmtMin(r.actual_prep_min)}}</td>
      <td class="num">${{fmtMin(r.estimated_prep_min)}}</td>
      <td class="num">${{fmtDiff(r.diff_min)}}</td>
      <td class="num">${{fmtRecommended(r)}}</td>
      <td class="num">${{r.orders || 0}}</td>
      <td>${{r.city_ua || r.city_name || '—'}}</td>
    </tr>`).join('');

    $('viewPrep').innerHTML = `
      <div class="portfolio-note">
        <strong>Preparation time</strong> · період <strong>${{PREP_TIME.label || '—'}}</strong>
        (останні 2 повні тижні).<br>
        Cooking time — налаштований час провайдера.
        Actual — фактичний час приготування.
        Estimated — оцінка системи.
        Різниця = Actual − Estimated (плюс = довше за оцінку системи).<br>
        <strong>Рекомендовано виставити</strong> — actual, округлений до 5 хв (макс. 90), щоб партнер підлаштував cooking time у системі під реальний час.
      </div>
      <div class="kpi-row">
        <div class="kpi"><div class="n">${{total}}</div><div class="l">Провайдерів</div></div>
        <div class="kpi bad"><div class="n">${{slower}}</div><div class="l">Повільніше за систему (&gt; +2 хв)</div></div>
        <div class="kpi"><div class="n" style="color:#2E7D32">${{faster}}</div><div class="l">Швидше за систему (&lt; −2 хв)</div></div>
        <div class="kpi"><div class="n">${{avgDiff.toFixed(1)}}</div><div class="l">Середня різниця, хв</div></div>
      </div>
      <div class="prep-legend">
        <span><span class="diff-pos">+x хв</span> актуальний час довший за систему</span>
        <span><span class="diff-neg">−x хв</span> актуальний час коротший за систему</span>
        <span><span class="rec-change">N хв</span> рекомендуємо змінити cooking time</span>
        <span><span class="rec-ok">N хв</span> вже близьке до факту</span>
      </div>
      <div class="prep-table-wrap">
        ${{rows.length ? `<table>
          <thead><tr>
            <th>Provider name</th>
            <th>Provider ID</th>
            <th>Cooking time, хв</th>
            <th>Actual preparation, хв</th>
            <th>Estimated preparation, хв</th>
            <th>Різниця</th>
            <th>Рекомендовано виставити, хв</th>
            <th>Замовлень</th>
            <th>Місто</th>
          </tr></thead>
          <tbody>${{tableRows}}</tbody>
        </table>` : '<p class="empty">Немає даних за обраним фільтром.</p>'}}
      </div>
      ${{renderCookingImpact()}}
    `;
  }}

  function initFilters() {{
    const weeks = Object.keys(REPORT_WEEKS).sort().reverse();
    const selW = $('selWeek');
    weeks.forEach(wk => {{
      const o = document.createElement('option');
      o.value = wk;
      o.textContent = REPORT_WEEKS[wk].label;
      selW.appendChild(o);
    }});
    if ('{default_week}') selW.value = '{default_week}';
    selW.addEventListener('change', onFilterChange);
    $('selCity').addEventListener('change', onCityChange);
    $('selBrand').addEventListener('change', render);
    onFilterChange();
  }}

  function currentWeek() {{
    return REPORT_WEEKS[$('selWeek').value];
  }}

  function onFilterChange() {{
    const wk = currentWeek();
    if (!wk) return;
    const selCity = $('selCity');
    const prev = selCity.value;
    selCity.innerHTML = '<option value="">— Усі міста —</option>';
    wk.cities.forEach(c => {{
      const o = document.createElement('option');
      o.value = c; o.textContent = c;
      selCity.appendChild(o);
    }});
    if ([...selCity.options].some(o => o.value === prev)) selCity.value = prev;
    onCityChange();
  }}

  function onCityChange() {{
    const wk = currentWeek();
    const city = $('selCity').value;
    const selBrand = $('selBrand');
    selBrand.innerHTML = '<option value="">— Оберіть партнера —</option>';
    if (!wk) return;
    const brands = new Set();
    Object.values(wk.partners).forEach(p => {{
      if (!city || p.city_ua === city) brands.add(p.brand);
    }});
    [...brands].sort((a,b) => a.localeCompare(b,'uk')).forEach(b => {{
      const o = document.createElement('option');
      o.value = b; o.textContent = b;
      selBrand.appendChild(o);
    }});
    render();
  }}

  function barRows(obj, total) {{
    if (!total) return '<p class="empty" style="padding:12px">Немає даних</p>';
    return Object.entries(obj).sort((a,b) => b[1]-a[1]).map(([label, cnt]) => {{
      const pct = (cnt/total*100).toFixed(1);
      const cls = FAULT_CLASS[label] || 'client';
      return `<div class="bar-row">
        <span class="bar-label">${{label}}</span>
        <div class="bar-track"><div class="bar-fill ${{cls}}" style="width:${{pct}}%"></div></div>
        <span class="bar-val">${{cnt}}</span>
      </div>`;
    }}).join('');
  }}

  function respBars(obj, total) {{
    if (!total) return '<p class="empty" style="padding:12px">Немає даних</p>';
    const entries = RESP_ORDER
      .filter(k => (obj || {{}})[k])
      .map(k => [k, obj[k]]);
    if (!entries.length) return '<p class="empty" style="padding:12px">Немає даних</p>';
    return entries.map(([key, cnt]) => {{
      const pct = (cnt/total*100).toFixed(1);
      return `<div class="bar-row">
        <span class="bar-label">${{RESP_UA[key] || key}}</span>
        <div class="bar-track"><div class="bar-fill ${{RESP_CLASS[key] || 'client'}}" style="width:${{pct}}%"></div></div>
        <span class="bar-val">${{cnt}}</span>
        <span style="min-width:52px;text-align:right;color:var(--muted);font-size:.78rem">${{pct}}%</span>
      </div>`;
    }}).join('');
  }}

  function platformBlock(partner) {{
    const groups = partner.bad_by_platform || {{}};
    const total = Object.values(groups).reduce((a,b)=>a+b,0);
    if (!total) return '';
    const rows = Object.entries(groups).sort((a,b) => b[1]-a[1]).map(([key, cnt]) => `<tr>
      <td><strong>${{PLATFORM_GROUP_UA[key] || key}}</strong></td>
      <td class="num">${{cnt}}</td>
      <td class="num">${{(cnt/total*100).toFixed(1)}}%</td>
      <td style="color:var(--muted)">${{PLATFORM_GROUP_HINT[key] || ''}}</td>
    </tr>`).join('');
    return `
      <h2 style="border-left-color:var(--bolt)">Платформа Bolt — з чого саме складається</h2>
      <div class="portfolio-note" style="background:#EFF6FF;border-color:#BFDBFE">
        Це <strong>не</strong> вина закладу і не вина кур'єра. Тут зібрані технічні та логістичні причини
        на стороні Bolt: помилки алгоритму прогнозу часу, відсутність вільних кур'єрів у зоні,
        робота диспетчеризації та рішення підтримки. Такі замовлення не варто ескалювати партнеру —
        з ними йдемо до логістики та підтримки.
      </div>
      <div class="prep-table-wrap">
        <table>
          <thead><tr>
            <th>Причина на стороні Bolt</th>
            <th>Замовлень</th>
            <th>Частка</th>
            <th>Що це означає</th>
          </tr></thead>
          <tbody>${{rows}}</tbody>
        </table>
      </div>
    `;
  }}

  function reasonList(obj) {{
    const entries = Object.entries(obj).sort((a,b) => b[1]-a[1]);
    if (!entries.length) return '<p class="empty" style="padding:12px">Немає даних</p>';
    return '<ul class="reason-list">' + entries.map(([r,c]) =>
      `<li><span>${{r}}</span><strong>${{c}}</strong></li>`
    ).join('') + '</ul>';
  }}

  function orderTable(orders) {{
    if (!orders.length) return '<p class="empty">Немає замовлень за обраним фільтром.</p>';
    const rows = orders.map(o => {{
      const tag = o.state === 'rejected'
        ? '<span class="tag tag-rejected">rejected</span>'
        : o.state === 'failed'
        ? '<span class="tag tag-failed">failed</span>'
        : '<span class="tag tag-failed">bad</span>';
      const respCls = RESP_CLASS[o.resp] || 'client';
      const resp = `<span class="tag tag-${{respCls}}">${{o.resp_ua || o.culprit_ua || '—'}}</span>`
        + (o.derived ? `<span class="derived-note">${{o.derived}}</span>` : '')
        + (o.platform_group_ua ? `<span class="derived-note">${{o.platform_group_ua}}</span>` : '');
      return `<tr>
        <td class="mono">${{o.order_id}}</td>
        <td class="mono">${{o.order_ref || '—'}}</td>
        <td>${{o.location}}</td>
        <td>${{resp}}</td>
        <td>${{o.reason_ua || '—'}}</td>
        <td>${{o.comment || '—'}}</td>
        <td>${{tag}}</td>
        <td class="mono">${{(o.created||'').slice(0,16)}}</td>
      </tr>`;
    }}).join('');
    const head = '<th>№ замовлення</th><th>Order ref</th><th>Локація</th><th>Зона відповідальності</th><th>Причина</th><th>Деталі</th><th>Статус</th><th>Час</th>';
    return `<table><thead><tr>${{head}}</tr></thead><tbody>${{rows}}</tbody></table>`;
  }}

  let detailOrders = {{ failed: [], bad: [] }};
  let activeDetailTab = 'failed';
  let currentPartnerCtx = null;

  function isProviderFault(o) {{
    return o.resp === 'provider';
  }}

  function providerBadOrders(partner) {{
    return (partner.bad_orders || []).filter(isProviderFault);
  }}

  function ukOrdersWord(n) {{
    const abs = Math.abs(n) % 100;
    const d = abs % 10;
    if (abs > 10 && abs < 20) return 'замовлень';
    if (d === 1) return 'замовлення';
    if (d >= 2 && d <= 4) return 'замовлення';
    return 'замовлень';
  }}

  function cleanReasonForPartner(reason) {{
    let r = (reason || '—').trim();
    r = r.replace(/^Заклад:\\s*/i, '');
    return r || '—';
  }}

  function buildPartnerMessage(partner, weekLabel, brand, cityLabel) {{
    const orders = providerBadOrders(partner);
    if (!orders.length) return '';

    const lines = [
      'Вітаю!',
      '',
      `Звертаю вашу увагу на невдалі замовлення за період ${{weekLabel}} у закладі ${{brand}} (${{cityLabel}}).`,
      '',
      `За результатами тижневого аналізу, ${{orders.length}} ${{ukOrdersWord(orders.length)}} мають причини, пов'язані з роботою закладу. Загальний показник поганих замовлень за тиждень: ${{partner.bad_pct}}% (${{partner.bad_count}} з ${{partner.delivered}} доставлених).`,
      '',
      'Деталі по замовленнях:',
      '',
    ];

    orders.forEach(o => {{
      lines.push(`Номер замовлення: ${{o.order_ref || '—'}}`);
      lines.push(`Заклад: ${{o.location || '—'}}`);
      lines.push(`Причина: ${{cleanReasonForPartner(o.reason_ua)}}`);
      lines.push('');
    }});

    lines.push(
      'Прошу перевірити:',
      '1. Наявність позицій у меню та своєчасне оновлення стоп-листу',
      '2. Якість пакування та правильність комплектації замовлень',
      '3. Час приготування та передачу замовлень курʼєру',
      '4. Стабільність прийняття замовлень протягом робочого дня',
      '',
      'Зниження частки поганих замовлень позитивно вплине на рейтинг закладу, задоволеність клієнтів та обсяг замовлень, і відповідно на ваш прибуток.',
      '',
      'Якщо потрібна допомога - рада буду допомогти.',
    );
    return lines.join('\\n');
  }}

  function openPartnerModal() {{
    if (!currentPartnerCtx) return;
    const {{ partner, weekLabel, brand, cityLabel }} = currentPartnerCtx;
    const text = buildPartnerMessage(partner, weekLabel, brand, cityLabel);
    $('partnerMessageText').value = text;
    $('partnerModal').classList.add('open');
  }}

  function closePartnerModal() {{
    $('partnerModal').classList.remove('open');
  }}

  function copyPartnerMessage() {{
    const ta = $('partnerMessageText');
    ta.select();
    ta.setSelectionRange(0, 99999);
    navigator.clipboard.writeText(ta.value).then(() => {{
      const btn = $('btnCopyPartnerMsg');
      const prev = btn.textContent;
      btn.textContent = 'Скопійовано!';
      setTimeout(() => {{ btn.textContent = prev; }}, 2000);
    }}).catch(() => document.execCommand('copy'));
  }}

  function updateCulpritFilter() {{
    const sel = $('selCulprit');
    if (!sel) return;
    const prev = sel.value;
    const orders = detailOrders[activeDetailTab] || [];
    const culprits = [...new Set(orders.map(o => o.culprit_ua).filter(Boolean))].sort((a,b) => a.localeCompare(b,'uk'));
    sel.innerHTML = '<option value="">— Усі зони —</option>';
    culprits.forEach(c => {{
      const o = document.createElement('option');
      o.value = c; o.textContent = c;
      sel.appendChild(o);
    }});
    if ([...sel.options].some(o => o.value === prev)) sel.value = prev;
    else sel.value = '';
  }}

  function renderDetailTable() {{
    const filter = $('selCulprit')?.value || '';
    const orders = (detailOrders[activeDetailTab] || []).filter(o => !filter || o.culprit_ua === filter);
    const host = activeDetailTab === 'failed' ? $('tabFailed') : $('tabBad');
    if (host) host.innerHTML = orderTable(orders);
    const cnt = $('detailCount');
    if (cnt) cnt.textContent = filter
      ? `Показано ${{orders.length}} з ${{detailOrders[activeDetailTab].length}}`
      : `Всього: ${{orders.length}}`;
  }}

  function initDetailPanel(partner) {{
    detailOrders.failed = (partner.failed_orders || []).map(o => ({{
      ...o,
      order_ref: o.order_ref || '—',
      resp_ua: o.resp_ua || o.culprit_ua || '—',
      culprit_ua: o.resp_ua || o.culprit_ua || '—',
      reason_ua: o.reason_ua || '—',
      comment: o.comment || o.detail || '—',
    }}));
    detailOrders.bad = (partner.bad_orders || []).map(o => ({{
      ...o,
      order_ref: o.order_ref || '—',
      resp_ua: o.resp_ua || o.culprit_ua || '—',
      culprit_ua: o.resp_ua || o.culprit_ua || '—',
      reason_ua: o.reason_ua || '—',
      comment: o.comment || '—',
    }}));
    activeDetailTab = 'failed';
    updateCulpritFilter();
    renderDetailTable();
  }}

  function mergePartners(list) {{
    if (!list.length) return null;
    if (list.length === 1) return list[0];
    const out = {{
      brand: list[0].brand,
      city_ua: 'Усі міста',
      delivered: 0, bad_count: 0, failed_count: 0,
      failed_by_fault: {{}}, bad_by_actor: {{}}, bad_by_reason: {{}},
      bad_by_resp: {{}}, bad_by_platform: {{}},
      failed_orders: [], bad_orders: []
    }};
    list.forEach(p => {{
      out.delivered += p.delivered || 0;
      out.bad_count += p.bad_count || 0;
      out.failed_count += p.failed_count || 0;
      ['failed_by_fault','bad_by_actor','bad_by_reason','bad_by_resp','bad_by_platform'].forEach(k => {{
        Object.entries(p[k] || {{}}).forEach(([a, c]) => {{
          out[k][a] = (out[k][a] || 0) + c;
        }});
      }});
      out.failed_orders = out.failed_orders.concat(p.failed_orders || []);
      out.bad_orders = out.bad_orders.concat(p.bad_orders || []);
    }});
    out.bad_pct = out.delivered ? +(out.bad_count / out.delivered * 100).toFixed(2) : 0;
    out.failed_pct = out.delivered ? +(out.failed_count / out.delivered * 100).toFixed(2) : 0;
    return out;
  }}

  function topBadBrands(wk, cityFilter, limit = 30) {{
    const byBrand = {{}};
    Object.values(wk.partners || {{}}).forEach(p => {{
      if (cityFilter && p.city_ua !== cityFilter) return;
      const key = p.brand;
      if (!byBrand[key]) {{
        byBrand[key] = {{
          brand: p.brand,
          delivered: 0,
          bad_count: 0,
          failed_count: 0,
          cities: new Set(),
        }};
      }}
      byBrand[key].delivered += p.delivered || 0;
      byBrand[key].bad_count += p.bad_count || 0;
      byBrand[key].failed_count += p.failed_count || 0;
      if (p.city_ua) byBrand[key].cities.add(p.city_ua);
    }});
    return Object.values(byBrand)
      .filter(b => b.delivered > 0 && b.bad_count > 0)
      .map(b => ({{
        ...b,
        bad_pct: +(b.bad_count / b.delivered * 100).toFixed(2),
        cities_label: [...b.cities].sort((a, c) => a.localeCompare(c, 'uk')).join(', '),
      }}))
      .sort((a, b) => b.bad_pct - a.bad_pct || b.bad_count - a.bad_count)
      .slice(0, limit);
  }}

  function renderPortfolioKpis(wk) {{
    const p = wk.portfolio || {{}};
    const city = $('selCity').value;
    const top = topBadBrands(wk, city, 30);
    const topRows = top.map((b, i) => `<tr>
      <td class="num">${{i + 1}}</td>
      <td><button type="button" class="link-brand" data-brand="${{b.brand.replace(/"/g, '&quot;')}}" style="background:none;border:none;color:var(--provider);font-weight:700;cursor:pointer;padding:0;text-align:left;font-size:inherit">${{b.brand}}</button></td>
      <td>${{b.cities_label || '—'}}</td>
      <td class="num"><strong style="color:var(--fail)">${{b.bad_pct}}%</strong></td>
      <td class="num">${{b.bad_count}}</td>
      <td class="num">${{b.delivered}}</td>
      <td class="num">${{b.failed_count}}</td>
    </tr>`).join('');

    $('portfolioKpis').innerHTML = `
      <h2 style="margin-top:0">Портфоліо · ${{wk.label}}</h2>
      <div class="kpi-row">
        <div class="kpi bad">
          <div class="n">${{p.bad_pct != null ? p.bad_pct : '—'}}%</div>
          <div class="l">Bad Orders (портфоліо)</div>
          <div style="font-size:.78rem;color:var(--muted);margin-top:6px">
            ${{p.bad_count || 0}} з ${{p.delivered || 0}} доставлених
          </div>
        </div>
        <div class="kpi fail">
          <div class="n">${{p.failed_pct != null ? p.failed_pct : '—'}}%</div>
          <div class="l">Failed Orders (портфоліо)</div>
          <div style="font-size:.78rem;color:var(--muted);margin-top:6px">
            ${{p.failed_count || 0}} невдалих замовлень
          </div>
        </div>
        <div class="kpi">
          <div class="n">${{p.delivered || 0}}</div>
          <div class="l">Доставлено</div>
        </div>
        <div class="kpi">
          <div class="n">${{p.bad_count || 0}}</div>
          <div class="l">Поганих (шт.)</div>
        </div>
        <div class="kpi">
          <div class="n">${{p.failed_count || 0}}</div>
          <div class="l">Невдалих (шт.)</div>
        </div>
      </div>

      <h2 class="bad-h">Зона відповідальності по всьому портфоліо</h2>
      <div class="grid-2">
        <div class="card">${{respBars(p.bad_by_resp || {{}}, Object.values(p.bad_by_resp || {{}}).reduce((a,b)=>a+b,0))}}</div>
        <div class="card">
          <p style="font-size:.82rem;color:var(--muted);margin-bottom:8px"><strong>Як читати звіт:</strong></p>
          <ul style="font-size:.82rem;color:var(--muted);padding-left:18px">
            <li>Кожне погане замовлення віднесене до однієї зони: <strong>Заклад</strong>, <strong>Кур'єр</strong>, <strong>Платформа Bolt</strong> або <strong>Клієнт</strong>.</li>
            <li>Спочатку беремо атрибуцію моделі Bolt. Якщо її немає — визначаємо за етапом, на якому зірвалось замовлення, за кодом причини або за оцінкою їжі.</li>
            <li>Такі випадки підписані сірим текстом у деталях, щоб було видно, що це наш розрахунок, а не готова атрибуція.</li>
            <li><strong>Не атрибутовано</strong> лишається тільки там, де жоден сигнал не дає відповіді — наприклад система бачить затримку на доставці, але не визначила причину.</li>
            <li>Партнеру надсилаємо тільки замовлення з зони <strong>Заклад</strong>.</li>
          </ul>
        </div>
      </div>
      ${{platformBlock({{ bad_by_platform: p.bad_by_platform || {{}} }})}}

      <h2 class="bad-h">ТОП-30 брендів за Bad Orders %</h2>
      <p style="font-size:.82rem;color:var(--muted);margin:-4px 0 12px">
        ${{city ? `Фільтр міста: <strong>${{city}}</strong>. ` : 'По всьому портфоліо. '}}
        Натисніть на бренд, щоб відкрити деталі.
      </p>
      <div class="prep-table-wrap">
        ${{top.length ? `<table>
          <thead><tr>
            <th>#</th>
            <th>Бренд</th>
            <th>Місто</th>
            <th>Bad Orders %</th>
            <th>Поганих</th>
            <th>Доставлено</th>
            <th>Failed</th>
          </tr></thead>
          <tbody>${{topRows}}</tbody>
        </table>` : '<p class="empty" style="padding:20px">Немає даних для ТОП-30.</p>'}}
      </div>
    `;

    document.querySelectorAll('.link-brand').forEach(btn => {{
      btn.addEventListener('click', () => {{
        const brand = btn.dataset.brand;
        const selBrand = $('selBrand');
        if (![...selBrand.options].some(o => o.value === brand)) {{
          const o = document.createElement('option');
          o.value = brand; o.textContent = brand;
          selBrand.appendChild(o);
        }}
        selBrand.value = brand;
        render();
        $('content').scrollIntoView({{ behavior: 'smooth', block: 'start' }});
      }});
    }});
  }}

  function render() {{
    const wk = currentWeek();
    const city = $('selCity').value;
    const brand = $('selBrand').value;
    const el = $('content');

    if (!wk) {{
      $('portfolioKpis').innerHTML = '';
      el.innerHTML = '<div class="empty">Немає даних.</div>';
      return;
    }}

    renderPortfolioKpis(wk);

    if (!brand) {{
      el.innerHTML = '<div class="empty">Оберіть бренд (партнера) для детального аналізу.</div>';
      return;
    }}

    const matches = Object.values(wk.partners).filter(p =>
      p.brand === brand && (!city || p.city_ua === city)
    );
    const partner = mergePartners(matches);

    if (!partner) {{
      el.innerHTML = '<div class="empty">Немає даних для обраного партнера.</div>';
      return;
    }}

    const failedTotal = Object.values(partner.failed_by_fault).reduce((a,b)=>a+b,0);
    const badTotal = Object.values(partner.bad_by_resp || {{}}).reduce((a,b)=>a+b,0);
    const titleCity = city || partner.city_ua;
    const providerBadCount = providerBadOrders(partner).length;
    currentPartnerCtx = {{
      partner,
      weekLabel: wk.label,
      brand,
      cityLabel: titleCity,
    }};

    el.innerHTML = `
      <h2 style="border:none;padding:0;margin-bottom:8px;font-size:1.15rem">
        ${{partner.brand}} · ${{titleCity}}
      </h2>
      <div class="kpi-row">
        <div class="kpi bad"><div class="n">${{partner.bad_pct}}%</div><div class="l">Bad Orders (партнер)</div></div>
        <div class="kpi fail"><div class="n">${{partner.failed_pct}}%</div><div class="l">Failed Orders (партнер)</div></div>
        <div class="kpi"><div class="n">${{partner.bad_count}}</div><div class="l">Поганих замовлень</div></div>
        <div class="kpi"><div class="n">${{partner.failed_count}}</div><div class="l">Невдалих замовлень</div></div>
        <div class="kpi"><div class="n">${{partner.delivered}}</div><div class="l">Доставлено</div></div>
      </div>

      <h2 class="fail-h">Failed Orders — зона відповідальності</h2>
      <div class="grid-2">
        <div class="card">
          <p style="font-size:.82rem;color:var(--muted);margin-bottom:10px">
            Зірвані (failed) та відхилені (rejected) замовлення за етапом, на якому зупинилось замовлення
          </p>
          ${{respBars(partner.failed_by_fault, failedTotal)}}
        </div>
        <div class="card">
          <p style="font-size:.82rem;color:var(--muted);margin-bottom:8px"><strong>Як визначається:</strong></p>
          <ul style="font-size:.82rem;color:var(--muted);padding-left:18px">
            <li><strong>Заклад</strong> — відхилив замовлення або не прийняв та не почав готувати вчасно</li>
            <li><strong>Кур'єр</strong> — були відмови кур'єрів від замовлення</li>
            <li><strong>Клієнт</strong> — є звернення клієнта про скасування</li>
            <li><strong>Платформа Bolt</strong> — кур'єра так і не знайшли або зрив уже на етапі доставки</li>
          </ul>
        </div>
      </div>

      <h2 class="bad-h">Bad Orders — зона відповідальності</h2>
      <div class="grid-2">
        <div class="card">${{respBars(partner.bad_by_resp, badTotal)}}</div>
        <div class="card">
          <p style="font-size:.82rem;color:var(--muted);margin-bottom:8px"><strong>Що входить у кожну зону:</strong></p>
          <ul style="font-size:.82rem;color:var(--muted);padding-left:18px">
            <li><strong>Заклад</strong> — не прийняв замовлення, затримка приготування, відсутні позиції, комплектація та якість страв</li>
            <li><strong>Кур'єр</strong> — спізнення до закладу чи клієнта, затримка на видачі, поведінка, недоставлене замовлення</li>
            <li><strong>Платформа Bolt</strong> — алгоритми, дефіцит кур'єрів, скасування підтримкою (розбивка нижче)</li>
            <li><strong>Клієнт</strong> — скасування клієнтом, а також скарги, які модель Bolt не підтвердила або віднесла до дій самого клієнта</li>
            <li><strong>Не атрибутовано</strong> — сигналів недостатньо навіть після перевірки етапу зриву та коду причини</li>
          </ul>
        </div>
      </div>

      ${{platformBlock(partner)}}

      <h2 class="bad-h">Bad Orders — причини</h2>
      <div class="card">${{reasonList(partner.bad_by_reason)}}</div>

      <div style="display:flex;flex-wrap:wrap;align-items:center;gap:4px">
        <button class="btn-detail" id="btnDetail" type="button">Детально — номери замовлень</button>
        ${{providerBadCount > 0 ? `<button class="btn-partner" id="btnPartnerMsg" type="button">Надіслати інформацію партнеру (${{providerBadCount}})</button>` : ''}}
      </div>
      <div class="detail-panel" id="detailPanel">
        <div class="detail-tabs">
          <button type="button" class="active" data-tab="failed">Failed Orders (${{partner.failed_orders.length}})</button>
          <button type="button" data-tab="bad">Bad Orders (${{partner.bad_orders.length}})</button>
        </div>
        <div class="detail-toolbar">
          <div>
            <label for="selCulprit">Зона відповідальності</label>
            <select id="selCulprit"><option value="">— Усі зони —</option></select>
          </div>
          <span class="detail-count" id="detailCount"></span>
        </div>
        <div id="tabFailed" style="overflow:auto"></div>
        <div id="tabBad" style="display:none;overflow:auto"></div>
      </div>
    `;

    initDetailPanel(partner);

    $('btnDetail').addEventListener('click', () => {{
      $('detailPanel').classList.toggle('open');
    }});
    const btnPartner = $('btnPartnerMsg');
    if (btnPartner) btnPartner.addEventListener('click', openPartnerModal);
    $('selCulprit').addEventListener('change', renderDetailTable);
    document.querySelectorAll('.detail-tabs button').forEach(btn => {{
      btn.addEventListener('click', () => {{
        document.querySelectorAll('.detail-tabs button').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        activeDetailTab = btn.dataset.tab;
        $('tabFailed').style.display = activeDetailTab === 'failed' ? 'block' : 'none';
        $('tabBad').style.display = activeDetailTab === 'bad' ? 'block' : 'none';
        updateCulpritFilter();
        renderDetailTable();
      }});
    }});
  }}

  initFilters();
  initPrepFilters();
  document.querySelectorAll('.view-tabs button').forEach(btn => {{
    btn.addEventListener('click', () => setView(btn.dataset.view));
  }});
  $('btnClosePartnerModal').addEventListener('click', closePartnerModal);
  $('btnClosePartnerModal2').addEventListener('click', closePartnerModal);
  $('btnCopyPartnerMsg').addEventListener('click', copyPartnerMessage);
  $('partnerModal').addEventListener('click', e => {{
    if (e.target === $('partnerModal')) closePartnerModal();
  }});
  </script>
</body>
</html>
"""


def enrich_all_weeks(weeks: dict[str, dict]) -> dict[str, dict]:
    for key in weeks:
        weeks[key] = enrich_week_payload(weeks[key])
    return weeks


def weeks_to_fetch(existing: dict[str, dict]) -> list[date]:
    """Усі пропущені повні тижні (пн) від останнього в звіті до останнього завершеного."""
    if os.environ.get("BAD_ORDERS_WEEK_START"):
        return [date.fromisoformat(os.environ["BAD_ORDERS_WEEK_START"])]

    last_complete_start, _ = week_bounds()
    if not existing:
        return [last_complete_start]

    latest_in_report = max(date.fromisoformat(k) for k in existing)
    ws = latest_in_report + timedelta(days=7)
    weeks: list[date] = []
    while ws <= last_complete_start:
        weeks.append(ws)
        ws += timedelta(days=7)
    return weeks


def main() -> None:
    existing = load_existing_weeks(OUTPUT_HTML)
    prep_data = load_existing_prep(OUTPUT_HTML)
    cooking_impact = load_existing_cooking_impact(OUTPUT_HTML)

    if os.environ.get("BAD_ORDERS_HTML_ONLY"):
        if not existing:
            raise RuntimeError("No existing data in HTML for BAD_ORDERS_HTML_ONLY")
        # Keep waiting banner until after-week exists; don't wipe impact.
        if not cooking_impact.get("ready"):
            cooking_impact = empty_cooking_impact()
            baseline = load_cooking_baseline()
            if baseline:
                cooking_impact["baseline_saved_at"] = baseline.get("saved_at", "")
                cooking_impact["before_week_start"] = baseline.get("before_week_start", "")
                cooking_impact["before_week_label"] = baseline.get("before_week_label", "")
                cooking_impact["message"] = (
                    "Базовий cooking time зафіксовано "
                    f"({cooking_impact['before_week_label'] or 'до змін'}). "
                    "Повний аналіз впливу з’явиться в понеділок після оновлення тижня "
                    "13.07–19.07."
                )
        generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        OUTPUT_HTML.write_text(
            build_html(enrich_all_weeks(existing), prep_data, generated_at, cooking_impact),
            encoding="utf-8",
        )
        print(f"Rebuilt HTML only: {OUTPUT_HTML}")
        return

    print(f"Existing weeks in report: {list(existing.keys())}")

    to_fetch = weeks_to_fetch(existing)
    print(f"Weeks to fetch: {[w.isoformat() for w in to_fetch] if to_fetch else '[] (up to date)'}")

    conn = sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=get_token(),
    )
    try:
        for ws in to_fetch:
            key = ws.isoformat()
            if key in existing and not os.environ.get("BAD_ORDERS_FORCE_REFRESH"):
                print(f"  Week {key} already present — skip (set BAD_ORDERS_FORCE_REFRESH=1 to overwrite)")
                continue
            we = ws + timedelta(days=6)
            existing[key] = fetch_week_data(conn, ws, we)

        prep_data = fetch_prep_time_data(conn)
        print(f"  Preparation time rows: {len(prep_data.get('rows', []))}")

        if not COOKING_BASELINE_PATH.exists():
            ensure_cooking_baseline(prep_data)
        cooking_impact = build_cooking_impact(conn, prep_data, existing)
        print(
            f"  Cooking impact: ready={cooking_impact.get('ready')} "
            f"changed={cooking_impact.get('changed_count', 0)}"
        )
    finally:
        conn.close()

    generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    existing = enrich_all_weeks(existing)
    OUTPUT_HTML.write_text(
        build_html(existing, prep_data, generated_at, cooking_impact),
        encoding="utf-8",
    )
    print(f"Written {OUTPUT_HTML} ({len(existing)} week(s))")

    # Also write week-specific snapshot for weeks in report
    for key, data in existing.items():
        ws = date.fromisoformat(key)
        snap = SCRIPT_DIR / f"Bad Orders {ws:%d.%m}-{ws + timedelta(days=6):%d.%m.%Y}.html"
        if not snap.exists() or os.environ.get("BAD_ORDERS_FORCE_REFRESH"):
            single = {key: data}
            snap.write_text(
                build_html(single, prep_data, generated_at, cooking_impact),
                encoding="utf-8",
            )
            print(f"  Snapshot: {snap.name}")


if __name__ == "__main__":
    main()
