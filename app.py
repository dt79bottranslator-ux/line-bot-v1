# =========================================================
# NORMALIZE
# =========================================================
def normalize_id(value: Any) -> str:
    if value is None:
        return ""

    text = str(value)
    text = text.replace("\u200b", "")
    text = text.replace("\ufeff", "")
    text = text.replace("\u2060", "")
    text = text.replace("\xa0", " ")
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    return text.strip()


# =========================================================
# FIND USER (MATCH LAYER)
# =========================================================
def find_user_row_index(values: List[List[str]], user_id: str) -> Optional[int]:
    target_user_id = normalize_id(user_id)

    print(f"[FIND USER] target={repr(target_user_id)} total_rows={len(values)}")

    for idx, row in enumerate(values):
        if idx == 0:
            continue

        raw_sheet_id = row[COL_USER_ID] if len(row) > COL_USER_ID else ""
        row_user_id = normalize_id(raw_sheet_id)

        print(
            f"[COMPARE] row={idx + 1} "
            f"sheet_raw={repr(raw_sheet_id)} "
            f"sheet_norm={repr(row_user_id)} "
            f"target={repr(target_user_id)}"
        )

        if row_user_id == target_user_id:
            print(f"[MATCH FOUND] row_index={idx + 1}")
            return idx + 1

    print("[MATCH FAILED]")
    return None


# =========================================================
# GET USER ROLE (FIX ADMIN BUG)
# =========================================================
def get_user_role(user_id: str) -> str:
    row = get_user_row(user_id)
    if row is None:
        print("[ROLE DEBUG] row=None")
        return ""

    raw_role = get_row_value(row, COL_ROLE, "")
    role = normalize_id(raw_role).lower()

    print(f"[ROLE DEBUG] raw={repr(raw_role)} normalized={repr(role)}")

    return role


# =========================================================
# ADMIN CHECK
# =========================================================
def is_user_admin(user_id: str) -> bool:
    role = get_user_role(user_id)
    result = role == "admin"

    print(f"[ADMIN] user_id={normalize_id(user_id)} role={repr(role)} admin={result}")

    return result


# =========================================================
# SET PREMIUM
# =========================================================
def set_user_premium(user_id: str, premium: bool) -> bool:
    worksheet, values = get_user_lang_values()
    if worksheet is None or not values:
        print("[PREMIUM SET] USER_LANG_MAP unavailable")
        return False

    try:
        target_user_id = normalize_id(user_id)
        found_row_index = find_user_row_index(values, target_user_id)

        if not found_row_index:
            print(f"[PREMIUM SET] user_id not found: {target_user_id}")
            return False

        current_row = values[found_row_index - 1]

        target_lang = get_row_value(current_row, COL_TARGET_LANG, "en") or "en"
        usage_count = get_row_value(current_row, COL_USAGE_COUNT, "0") or "0"
        group_id = get_row_value(current_row, COL_GROUP_ID, "USER") or "USER"
        role = get_row_value(current_row, COL_ROLE, "")

        premium_text = "TRUE" if premium else "FALSE"

        new_row = [
            target_user_id,
            target_lang,
            now_iso(),
            premium_text,
            usage_count,
            group_id,
            role,
        ]

        worksheet.update(f"A{found_row_index}:G{found_row_index}", [new_row])

        print(f"[PREMIUM SET] user_id={target_user_id} premium={premium_text}")
        return True

    except Exception as exc:
        print(f"[PREMIUM SET ERROR] {str(exc)}")
        return False
