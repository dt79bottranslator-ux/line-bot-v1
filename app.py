def save_user_language(sheet, user_id, target_lang):
    """
    Lưu hoặc cập nhật ngôn ngữ của user vào Google Sheet.

    Cột kỳ vọng:
    A = user_id
    B = target_lang
    C = updated_at
    """
    try:
        records = sheet.get_all_records()

        # 1) Nếu user đã tồn tại -> update
        for idx, row in enumerate(records, start=2):  # start=2 vì dòng 1 là header
            if str(row.get("user_id", "")).strip() == str(user_id).strip():
                sheet.update_cell(idx, 2, target_lang)       # cột B
                sheet.update_cell(idx, 3, get_timestamp())   # cột C
                logger.info("Updated user_id=%s", user_id)
                return True

        # 2) Nếu user chưa tồn tại -> append dòng mới
        sheet.append_row([
            str(user_id).strip(),
            str(target_lang).strip(),
            get_timestamp()
        ])
        logger.info("Inserted new user_id=%s", user_id)
        return True

    except Exception as e:
        logger.exception("save_user_language failed: %s", e)
        return False
