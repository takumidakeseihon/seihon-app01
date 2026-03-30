import streamlit as st
import pandas as pd
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
import math
import unicodedata # 最強の文字照合用ライブラリ

# Firebaseライブラリをインポート
import firebase_admin
from firebase_admin import credentials, firestore
import json
import os

# 画像処理用ライブラリ（日報の写真アップロードで使用）
import base64
import io
from PIL import Image

# --- 共通テキストクリーンアップ関数（絶対マッチさせる用） ---
def clean_text(text):
    if pd.isna(text): return ""
    text = str(text)
    # NFKC正規化で全角半角（ＡとAなど）を統一
    text = unicodedata.normalize('NFKC', text)
    # すべてのスペースを完全に除去
    return text.strip().replace(' ', '').replace('　', '')

# --- 定数設定 ---
SCHEDULE_FILE = "schedule.csv"
PROCESS_OPTIONS = ["", "断裁", "折", "中綴じ", "無線綴じ", "ミシン・スジ", "角丸", "貼込", "糸かがり", "綴じ（カレンダー）", "丁合（カレンダー）", "穴明け", "梱包", "区分け", "手作業"]
NAIRE_PROCESS_OPTIONS = ["", "断裁", "丁合", "綴じ", "綴じ+梱包", "メクレルト", "梱包"]
FOLD_OPTIONS = ["", "4p", "6p", "8p", "16p", "その他"]

SCHEDULE_COL_PAGE_COUNT = "ページ数"
SCHEDULE_COL_TOTAL_QUANTITY = "総数"
SCHEDULE_COL_REMARKS = ["作業予定表備考1", "作業予定表備考2"]
SCHEDULE_COL_LOCATION_CODE = "拠点コード"
SCHEDULE_COL_DETAILS = "適用"
SCHEDULE_COL_DUE_DATE = "納期日付"
SCHEDULE_COL_DELIVERY_METHOD = "納品方法"
SCHEDULE_COL_DELIVERY_TIME = "納期時間"
SCHEDULE_COL_AMOUNT = "金額" # R列

# 機械リストの定義
ASAHIKAWA_MACHINES = {
    "断裁": ["", "断裁１号機", "断裁２号機", "断裁３号機", "断裁４号機"],
    "折": ["折機１号機", "折機２号機", "折機３号機", "折機４号機", "折機５号機", "折機６号機", "折機７号機", "折機８号機", "折機９号機", "折機１０号機"],
    "中綴じ": ["", "中綴１号機", "中綴２号機", "中綴３号機", "中綴４号機", "中綴５号機"],
    "無線綴じ": ["", "ボレロ"],
    "貼込": ["", "貼込１号機", "貼込２号機"],
    "ミシン・スジ": ["", "ミシン・スジ１号機"],
    "丁合（カレンダー）": ["", "丁合機"],
    "綴じ（カレンダー）": ["", "タンザック620", "タンザック520"],
}
SAPPORO_MACHINES = {
    "断裁": ["", "断裁１号機"],
    "中綴じ": ["", "中綴じ１号機", "中綴じ２号機", "中綴じ３号機", "中綴じ４号機", "中綴じ５号機"],
    "折": ["折り機１号機", "折り機２号機", "折り機３号機", "折り機４号機", "折り機５号機", "折り機６号機"],
    "ミシン・スジ": ["", "ミシン・スジ機"],
    "貼込": ["", "貼込み１号機"],
    "糸かがり": ["", "糸かがり１号機"],
}

# 従業員リスト
WORKER_NAMES = [
    "赤松 浩明", "浅野 央詞", "小松 宣彦", "小山 輝義", "佐々木 善直", "藤井 康彰",
    "荒田 朋子", "川井 千代宝", "木原 裕治", "蟹谷 和豊", "高橋 誠", "大文字 俊幸",
    "青塚 知代", "早川 健太", "石井 美津枝", "山下 泉", "小島 広勝", "菅原 加奈",
    "神馬 妃那", "ディアン ファトクローマン", "インドラ アデ カマルディン", "ムハマド ユヌス", "岳　匠",
    "立川　悠依", 
    "家常 貴史", "藤田 祐司", "田中 二郎", "内田 進", "若杉 瑞樹", "小柄 浩二",
    "蓬畑 皓一", "藤井 翔太", "佐々木 輝", "ノヴィ アナ", "カロマー ユニシャ",
    "モニカ ジュリヤニ", "岳 司郎",
]

# 従業員と拠点の対応表
ASAHIKAWA_MEMBERS = [
    "赤松 浩明", "浅野 央詞", "小松 宣彦", "小山 輝義", "佐々木 善直", "藤井 康彰",
    "荒田 朋子", "川井 千代宝", "木原 裕治", "蟹谷 和豊", "高橋 誠", "大文字 俊幸",
    "青塚 知代", "早川 健太", "石井 美津枝", "山下 泉", "小島 広勝", "菅原 加奈",
    "神馬 妃那", "ディアン ファトクローマン", "インドラ アデ カマルディン", "ムハマド ユヌス", "岳　匠",
    "立川　悠依", 
]
SAPPORO_MEMBERS = [
    "家常 貴史", "藤田 祐司", "田中 二郎", "内田 進", "若杉 瑞樹", "小柄 浩二",
    "蓬畑 皓一", "藤井 翔太", "佐々木 輝", "ノヴィ アナ", "カロマー ユニシャ",
    "モニカ ジュリヤニ", "岳 司郎",
]

WORKER_TO_LOCATION = {name: "旭川" for name in ASAHIKAWA_MEMBERS}
WORKER_TO_LOCATION.update({name: "札幌" for name in SAPPORO_MEMBERS})

# --- 従業員IDマッピング（URL文字化け対策） ---
WORKER_ID_MAP = {
    "赤松 浩明": "A01", "浅野 央詞": "A02", "小松 宣彦": "A03", "小山 輝義": "A04",
    "佐々木 善直": "A05", "藤井 康彰": "A06", "荒田 朋子": "A07", "川井 千代宝": "A08",
    "木原 裕治": "A09", "蟹谷 和豊": "A10", "高橋 誠": "A11", "大文字 俊幸": "A12",
    "青塚 知代": "A13", "早川 健太": "A14", "石井 美津枝": "A15", "山下 泉": "A16",
    "小島 広勝": "A17", "菅原 加奈": "A18", "神馬 妃那": "A19",
    "ディアン ファトクローマン": "A20", "インドラ アデ カマルディン": "A21",
    "ムハマド ユヌス": "A22", "岳　匠": "A23", "立川　悠依": "A24",
    "家常 貴史": "S01", "藤田 祐司": "S02", "田中 二郎": "S03", "内田 進": "S04",
    "若杉 瑞樹": "S05", "小柄 浩二": "S06", "蓬畑 皓一": "S07", "藤井 翔太": "S08",
    "佐々木 輝": "S09", "ノヴィ アナ": "S10", "カロマー ユニシャ": "S11",
    "モニカ ジュリヤニ": "S12", "岳 司郎": "S13"
}
ID_TO_WORKER = {v: k for k, v in WORKER_ID_MAP.items()}

# --- データ読み込み・認証関数 ---
@st.cache_data(ttl=3600)
def load_csv_data(file_path):
    if file_path == SCHEDULE_FILE and 'manual_schedule_df' in st.session_state:
        return st.session_state.manual_schedule_df

    if file_path == SCHEDULE_FILE and "SCHEDULE_CSV_URL" in st.secrets:
        url = st.secrets["SCHEDULE_CSV_URL"]
        if url:
            try:
                return pd.read_csv(url, encoding="utf-8-sig")
            except Exception as e:
                pass 

    try:
        df = pd.read_csv(file_path, encoding="utf-8-sig")
        return df
    except FileNotFoundError:
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ {file_path}の読み込み中にエラーが発生: {e}")
        return pd.DataFrame()

@st.cache_resource
def init_firebase():
    try:
        if not firebase_admin._apps:
            if os.environ.get("FIREBASE_KEY_JSON"):
                key_dict = json.loads(os.environ.get("FIREBASE_KEY_JSON"))
                cred = credentials.Certificate(key_dict)
                firebase_admin.initialize_app(cred)
            elif "FIREBASE_KEY_JSON" in st.secrets:
                key_dict = json.loads(st.secrets["FIREBASE_KEY_JSON"])
                cred = credentials.Certificate(key_dict)
                firebase_admin.initialize_app(cred)
            else:
                cred = credentials.Certificate("firebase_key.json")
                firebase_admin.initialize_app(cred)
            return firestore.client()
        else:
            return firestore.client()
    except Exception as e:
        st.error(f"データベース接続エラー: {e}")
        return None

@st.cache_data(ttl=600)
def load_from_firestore(_db, collection_name, active_only=False, days_limit=None):
    if not _db: return pd.DataFrame()
    try:
        query = _db.collection(collection_name)
        if days_limit:
            try:
                docs = query.order_by("作成日時", direction=firestore.Query.DESCENDING).limit(days_limit).stream()
            except:
                docs = query.limit(days_limit).stream()
        else:
            docs = query.stream()
            
        records = [doc.to_dict() | {'id': doc.id} for doc in docs]
        
        if not records:
            return pd.DataFrame()
            
        df = pd.DataFrame(records)
        
        if active_only:
            if "完了ステータス" in df.columns:
                df = df[df["完了ステータス"] != "出荷待ち"]
        
        return df
    except Exception as e:
        st.error(f"❌ {collection_name} のデータ読み込み中にエラーが発生しました: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def load_tasks_for_customer(_db, customer_name):
    if not _db or not customer_name:
        return pd.DataFrame()
    
    all_tasks = []
    for collection in ["in_progress", "completed"]:
        try:
            docs = _db.collection(collection).stream()
            records = [doc.to_dict() | {'id': doc.id} for doc in docs]
            if records:
                df = pd.DataFrame(records)
                if "製品名" in df.columns:
                    filtered_df = df[df["製品名"] == customer_name]
                    if not filtered_df.empty:
                        all_tasks.extend(filtered_df.to_dict('records'))
        except Exception as e:
            st.warning(f"⚠️ {collection}からのデータ取得中に問題が発生しました: {e}")

    return pd.DataFrame(all_tasks) if all_tasks else pd.DataFrame()

# =======================================================================
# フォームやハンドラ関数
# =======================================================================
def process_form(is_edit_mode=False, default_data=None):
    if default_data is None: default_data = {}
    product_name = default_data.get('製品名', st.session_state.get('selected_product', ''))
    process_name = default_data.get('工程名', st.session_state.get('selected_process', ''))
    
    st.markdown(f"<h2 style='font-size: clamp(0.9rem, 3.5vw, 1.6rem); margin-bottom: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;' title='Step 2: 「{product_name}」の作業内容を記録'>Step 2: 「{product_name}」の作業内容を記録</h2>", unsafe_allow_html=True)
    st.markdown(f"<h3 style='font-size: clamp(0.8rem, 3vw, 1.2rem); color: #555; margin-top: 5px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;'>工程: <b>{process_name}</b></h3>", unsafe_allow_html=True)
    
    schedule_df = load_csv_data(SCHEDULE_FILE)
    if not schedule_df.empty and '品名' in schedule_df.columns:
        clean_target = clean_text(product_name)
        schedule_df['clean_品名_for_match'] = schedule_df['品名'].apply(clean_text)
        product_row = schedule_df[schedule_df['clean_品名_for_match'] == clean_target]
        
        if not product_row.empty:
            info = product_row.iloc[0]
            info_lines = ["**予定表の情報（参照用）**"]
            
            amount_val = info.get(SCHEDULE_COL_AMOUNT, 0)
            amount_str = f"{int(amount_val):,}円" if pd.notna(amount_val) else ""
            
            remarks_list = [str(info[col]) for col in SCHEDULE_COL_REMARKS if col in info and pd.notna(info[col])]
            remarks_combined = " | ".join(remarks_list) if remarks_list else ""
            
            display_data = {
                "総数": info.get(SCHEDULE_COL_TOTAL_QUANTITY, ""),
                "受注金額": amount_str,
                "適用": info.get(SCHEDULE_COL_DETAILS, ""),
                "納期日付": info.get(SCHEDULE_COL_DUE_DATE, ""),
                "納品方法": info.get(SCHEDULE_COL_DELIVERY_METHOD, ""),
                "納期時間": info.get(SCHEDULE_COL_DELIVERY_TIME, ""),
                "備考": remarks_combined
            }
            
            for key, value in display_data.items():
                if pd.notna(value) and str(value).strip() != "":
                    safe_value = str(value).replace('*', '\*')
                    info_lines.append(f"**{key}:** {safe_value}")
                    
            display_text = "\n\n".join(info_lines)
            st.info(display_text)

    def to_time_obj(time_str):
        if time_str and isinstance(time_str, str):
            try: return datetime.strptime(time_str, '%H:%M').time()
            except ValueError: return None
        return None

    with st.form(key='process_form'):
        user_location = st.session_state.get('user_location', "未設定")
        
        detail_value = default_data.get('詳細', '')
        start_time_obj_val = to_time_obj(default_data.get('開始時間'))
        end_time_obj_val = to_time_obj(default_data.get('終了時間'))
        work_time_minutes_input = 0
        
        setup_processes = ["中綴じ", "折", "無線綴じ", "糸かがり", "綴じ（カレンダー）", "丁合（カレンダー）"]
        rotation_processes = ["折", "中綴じ", "無線綴じ", "ミシン・スジ", "貼込", "綴じ（カレンダー）", "丁合（カレンダー）"]
        
        setup_workers, setup_time, rotation_speed, machine_selection = 0.0, 0, 0, ""

        st.subheader("機械情報")
        machine_options = []
        if user_location == "旭川" and process_name in ASAHIKAWA_MACHINES:
            machine_options = list(ASAHIKAWA_MACHINES[process_name])
        elif user_location == "札幌" and process_name in SAPPORO_MACHINES:
            machine_options = list(SAPPORO_MACHINES[process_name])
            
        default_machine = default_data.get('使用機械', None)

        if machine_options:
            if process_name == "折":
                default_selections = []
                if default_machine and isinstance(default_machine, str):
                    default_selections = [item.strip() for item in default_machine.split(',') if item.strip()]
                
                for item in default_selections:
                    if item not in machine_options:
                        machine_options.append(item)
                
                selected_machines = st.multiselect("使用した折機（複数選択可）", options=machine_options, default=default_selections)
                machine_selection = ", ".join(selected_machines)
            else:
                if default_machine and isinstance(default_machine, str) and default_machine not in machine_options:
                    machine_options.append(default_machine)
                
                default_index_machine = 0
                if default_machine and default_machine in machine_options:
                    default_index_machine = machine_options.index(default_machine)
                machine_selection = st.selectbox("使用した機械", options=machine_options, index=default_index_machine)
        else:
             if default_machine:
                 st.info(f"記録された機械: {default_machine}")
                 machine_selection = default_machine
             else:
                 machine_selection = ""

        if process_name in setup_processes:
            setup_col1, setup_col2 = st.columns(2)
            with setup_col1:
                raw_setup_workers = default_data.get('セット人数', 0.0)
                setup_workers = st.number_input("セット人数", min_value=0.0, step=0.5, value=float(raw_setup_workers) if pd.notna(raw_setup_workers) else 0.0, format="%.1f")
            with setup_col2:
                raw_setup_time = default_data.get('セット時間_分', 0)
                setup_time = st.number_input("セット時間（分）", min_value=0, step=10, value=int(raw_setup_time) if pd.notna(raw_setup_time) else 0)

        if process_name in rotation_processes:
            raw_rotation_speed = default_data.get('回転数', 0)
            rotation_speed = st.number_input("機械回転数", min_value=0, step=100, value=int(raw_rotation_speed) if pd.notna(raw_rotation_speed) else 0)
        
        st.divider()
        st.subheader("作業実績")
        
        quantity = st.number_input("出来数", min_value=0, step=1, value=int(default_data.get('出来数', 0)))
        workers = st.number_input("作業人数（合計）", min_value=0.5, step=0.5, value=float(default_data.get('作業人数', 1.0)), format="%.1f")
        
        if user_location == "旭川":
            base_workers = ASAHIKAWA_MEMBERS
        elif user_location == "札幌":
            base_workers = SAPPORO_MEMBERS
        else:
            base_workers = WORKER_NAMES
            
        other_workers = [name for name in base_workers if name != st.session_state.logged_in_user and name != "（自分の名前を選択してください）"]

        raw_co_workers = default_data.get('共同作業者', [])
        
        if isinstance(raw_co_workers, list):
            safe_co_workers = raw_co_workers
        elif isinstance(raw_co_workers, str):
            safe_co_workers = [w.strip() for w in raw_co_workers.split(',')] if raw_co_workers else []
        else:
            safe_co_workers = []

        valid_default_co_workers = [w for w in safe_co_workers if w in other_workers]
        
        selected_co_workers = st.multiselect(
            "👤 一緒に作業したメンバー（任意・複数選択可）", 
            options=other_workers, 
            default=valid_default_co_workers,
            help="ここで選んだメンバーの「日報」にも、この作業履歴が自動的に追加されます！"
        )

        start_time_label = "開始時間/※セット時間は含まない" if process_name in setup_processes else "開始時間"

        if process_name == "断裁":
            time_options = [str(i * 10) for i in range(1, 73)]
            default_work_time = str(default_data.get('作業時間_分', 60))
            index = time_options.index(default_work_time) if default_work_time in time_options else 5
            work_time_str = st.selectbox("作業時間（分）", time_options, index=index)
            work_time_minutes_input = int(work_time_str)
            final_detail_value, start_time_obj, end_time_obj = f"{work_time_str}分", None, None
        
        elif process_name == "手作業":
            final_detail_value = st.text_input("手作業の内容", value=detail_value, placeholder="例: 封入、検品、シール貼りなど")
            start_time_obj = st.time_input("開始時間", step=600, value=start_time_obj_val)
            end_time_obj = st.time_input("終了時間", step=600, value=end_time_obj_val)

        elif process_name == "折":
            default_selections_pages = []
            if default_data.get('詳細'):
                default_selections_pages = [item.strip() for item in default_data.get('詳細').split(',')]
            
            selected_options = st.multiselect(
                "ページ数（複数選択可）",
                options=[opt for opt in FOLD_OPTIONS if opt],
                default=default_selections_pages
            )
            final_detail_value = ", ".join(selected_options)
            start_time_obj = st.time_input(start_time_label, step=600, value=start_time_obj_val)
            end_time_obj = st.time_input("終了時間", step=600, value=end_time_obj_val)
        elif process_name in ["中綴じ", "無線綴じ", "糸かがり", "綴じ（カレンダー）"]:
            default_pages = st.session_state.get('default_page_count', 0)
            if is_edit_mode:
                try: default_pages = int(default_data.get('詳細', 0))
                except (ValueError, TypeError): default_pages = 0
            page_count = st.number_input("ページ数／枚数", min_value=0, step=1, value=default_pages)
            final_detail_value = str(page_count)
            start_time_obj = st.time_input(start_time_label, step=600, value=start_time_obj_val)
            end_time_obj = st.time_input("終了時間", step=600, value=end_time_obj_val)
        elif process_name == "梱包":
            default_packing_type, default_items_per_pack, default_box_count = "", 0, 0
            if is_edit_mode and detail_value:
                details = detail_value.split(" | ")
                default_packing_type = details[0] if details else ""
                for item in details[1:]: 
                    if "個/包" in item:
                        try:
                            default_items_per_pack = int(item.replace("個/包", "").strip())
                        except ValueError:
                            default_items_per_pack = 0
                    elif "箱" in item:
                        try:
                            default_box_count = int(item.replace("箱", "").strip())
                        except ValueError:
                            default_box_count = 0
            packing_type = st.selectbox("作業内容", ["", "包装+箱", "包装のみ", "箱入れのみ", "結束"], index=["", "包装+箱", "包装のみ", "箱入れのみ", "結束"].index(default_packing_type) if default_packing_type in ["", "包装+箱", "包装のみ", "箱入れのみ", "結束"] else 0)
            items_per_pack, box_count = 0, 0
            if "包装" in packing_type or "結束" in packing_type: items_per_pack = st.number_input("一包みの入数", min_value=0, step=1, value=default_items_per_pack)
            if "箱" in packing_type: box_count = st.number_input("箱の数", min_value=0, step=1, value=default_box_count)
            details_list = [packing_type]
            if items_per_pack > 0: details_list.append(f"{items_per_pack}個/包")
            if box_count > 0: details_list.append(f"{box_count}箱")
            final_detail_value = " | ".join(d for d in details_list if d)
            start_time_obj = st.time_input("開始時間", step=600, value=start_time_obj_val)
            end_time_obj = st.time_input("終了時間", step=600, value=end_time_obj_val)
        else:
            final_detail_value = st.text_input("詳細（任意）", value=detail_value)
            start_time_obj = st.time_input(start_time_label, step=600, value=start_time_obj_val)
            end_time_obj = st.time_input("終了時間", step=600, value=end_time_obj_val)

        remarks = st.text_area("備考", value=default_data.get('備考', ''))
        
        col_btn1, col_btn2, col_btn3 = st.columns([1.2, 1.2, 2])
        if is_edit_mode:
            submit_button = col_btn1.form_submit_button("更新する", type="primary", use_container_width=True, on_click=disable_buttons, disabled=st.session_state.submit_disabled)
            complete_button = None
        else:
            submit_button = col_btn1.form_submit_button("作業中として追加", use_container_width=True, on_click=disable_buttons, disabled=st.session_state.submit_disabled)
            complete_button = col_btn2.form_submit_button("この内容で最終完了", type="primary", use_container_width=True, on_click=disable_buttons, disabled=st.session_state.submit_disabled)
        
        if col_btn3.form_submit_button("キャンセル"):
            st.session_state.submit_disabled = False
            st.session_state.sub_view = 'SELECT_PROCESS'
            st.rerun()

        def prepare_data_dict(status="作業中"):
            work_time_minutes = 0
            if process_name == "断裁":
                work_time_minutes = work_time_minutes_input
            elif start_time_obj and end_time_obj:
                if end_time_obj <= start_time_obj:
                    st.error("❌ 終了時間は開始時間よりも後の時刻を選択してください。")
                    return None
                delta = datetime.combine(datetime.today(), end_time_obj) - datetime.combine(datetime.today(), start_time_obj)
                work_time_minutes = delta.total_seconds() / 60
            elif process_name != "断裁" and (start_time_obj or end_time_obj):
                st.error("❌ 開始時間と終了時間を両方入力してください。")
                return None
            
            return {
                "入力者名": st.session_state.logged_in_user,
                "共同作業者": selected_co_workers,
                "拠点": user_location,
                "使用機械": machine_selection,
                "記録ID": default_data.get('記録ID', datetime.now().strftime("%Y%m%d%H%M%S%f")),
                "製品名": product_name, "工程名": process_name, "詳細": final_detail_value,
                "開始時間": start_time_obj.strftime('%H:%M') if start_time_obj else "",
                "終了時間": end_time_obj.strftime('%H:%M') if end_time_obj else "",
                "作業時間_分": int(work_time_minutes), "出来数": int(quantity),
                "作業人数": float(workers),
                "ステータス": status, "備考": remarks,
                "作成日時": firestore.SERVER_TIMESTAMP,
                "セット人数": float(setup_workers),
                "セット時間_分": int(setup_time),
                "回転数": int(rotation_speed)
            }

        def run_validation_and_submit(status):
            if quantity <= 0:
                st.error("❌ 出来数は1以上で入力してください。")
                st.session_state.submit_disabled = False 
                return

            final_data_dict = prepare_data_dict(status=status)
            if final_data_dict:
                handler = handle_completion if status == "完了" else (handle_update if is_edit_mode else handle_add_in_progress)
                args = (default_data.get('id'), final_data_dict) if is_edit_mode else (final_data_dict,)
                handler(*args)
            else:
                st.session_state.submit_disabled = False

        if submit_button: run_validation_and_submit("作業中")
        if complete_button: run_validation_and_submit("完了")

def handle_db_write(operation, success_message, error_message, rerun_on_success=True):
    try:
        with st.spinner("処理中..."):
            if not firebase_admin._apps:
                init_firebase()
            operation()
            st.session_state.success_msg = success_message
            st.session_state.sub_view = 'SELECT_PROCESS'
            if rerun_on_success:
                load_from_firestore.clear()
                load_tasks_for_customer.clear()
                st.rerun()
    except Exception as e:
        st.error(f"{error_message}: {e}")
    finally:
        st.session_state.submit_disabled = False

def handle_update(doc_id, data_dict):
    handle_db_write(lambda: firestore.client().collection("in_progress").document(doc_id).update(data_dict), "記録を更新しました。", "更新中にエラーが発生")

def handle_add_in_progress(data_dict):
    handle_db_write(lambda: firestore.client().collection("in_progress").add(data_dict), f"工程「{data_dict['工程名']}」を追加しました。", "追加処理中にエラーが発生")

def handle_completion(new_data_dict):
    def operation():
        if not firebase_admin._apps: init_firebase()
        db_batch = firestore.client()
        batch = db_batch.batch()
        
        product_name = new_data_dict['製品名']
        
        in_progress_df = st.session_state.get('in_progress_df', pd.DataFrame())
        if not in_progress_df.empty and "製品名" in in_progress_df.columns:
            docs_to_move = in_progress_df[in_progress_df["製品名"] == product_name]
            for index, row in docs_to_move.iterrows():
                doc_data = row.to_dict(); doc_data['ステータス'] = '完了'
                if '拠点' not in doc_data or pd.isna(doc_data.get('拠点')) or doc_data.get('拠点') == '未設定':
                    clean_name = doc_data.get('製品名', '').strip().replace(' ', '').replace('t', '')
                    doc_data['拠点'] = st.session_state.get('product_to_location', {}).get(clean_name, '未設定')
                batch.set(db_batch.collection("completed").document(), doc_data)
                batch.delete(db_batch.collection("in_progress").document(row['id']))

        batch.set(db_batch.collection("completed").document(), new_data_dict)
        batch.commit()
    handle_db_write(operation, f"✅ 「{new_data_dict['製品名']}」の記録を確定しました。", "完了処理中にエラーが発生")

def handle_product_completion(product_name):
    def operation():
        if not firebase_admin._apps: init_firebase()
        db_batch = firestore.client()
        batch = db_batch.batch()
        
        in_progress_df = st.session_state.get('in_progress_df', pd.DataFrame())
        moved_count = 0
        if not in_progress_df.empty and "製品名" in in_progress_df.columns:
            docs_to_move = in_progress_df[in_progress_df["製品名"] == product_name]
            moved_count = len(docs_to_move)
            for index, row in docs_to_move.iterrows():
                doc_data = row.to_dict(); doc_data['ステータス'] = '完了'
                if '拠点' not in doc_data or pd.isna(doc_data.get('拠点')) or doc_data.get('拠点') == '未設定':
                    clean_name = doc_data.get('製品名', '').strip().replace(' ', '').replace('S', '')
                    doc_data['拠点'] = st.session_state.get('product_to_location', {}).get(clean_name, '未設定')
                batch.set(db_batch.collection("completed").document(), doc_data)
                batch.delete(db_batch.collection("in_progress").document(row['id']))
        
        if moved_count == 0:
            st.warning(f"「{product_name}」に関する作業中の記録が見つかりません。")
            return
        batch.commit()
    
    handle_db_write(operation, f"✅ 「{product_name}」のすべての作業を完了しました。", "完了処理中にエラーが発生")

def login_screen():
    st.header("ようこそ！")
    st.subheader("はじめに、あなたの名前を選択してください。")
    
    def set_user_and_redirect(user):
        st.session_state.just_logged_in = True
        st.session_state.logged_in_user = user
        st.session_state.user_location = WORKER_TO_LOCATION.get(user, "すべて")

    cols = st.columns(4)
    actual_worker_names = [name for name in WORKER_NAMES if name != "（自分の名前を選択してください）"]
    for i, name in enumerate(actual_worker_names):
        with cols[i % 4]:
            if st.button(name, key=f"user_{name}", use_container_width=True):
                set_user_and_redirect(name)
                st.rerun()

def show_bookmark_page(user_name):
    st.success(f"**{user_name}** さんとしてログインしました！")
    st.header("📌 ホーム画面への追加（重要）")
    
    st.warning(
        "スマホの場合、今のまま「ホーム画面に追加」をすると、次回またログイン画面に戻ってしまうことがあります。\n\n"
        "確実にログイン状態を保存するために、**必ず以下の「青いボタン（専用リンク）」を一度タップ**してください。"
    )
    
    user_id = WORKER_ID_MAP.get(user_name, "")
    link_url = f"?uid={user_id}"
    
    st.markdown(f"""
        <a href="{link_url}" target="_blank" rel="noopener noreferrer" style="display: block; text-align: center; background-color: #3b82f6; color: white; padding: 15px; text-decoration: none; border-radius: 10px; font-weight: bold; font-size: 18px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.2);">
            👉 1. ここをタップして【新しいタブ】で開き直す
        </a>
    """, unsafe_allow_html=True)
    
    st.info("2. 新しい画面（タブ）が開いたら、その画面でブラウザの下のメニュー「↑」から **「ホーム画面に追加」** を行ってください。")
    st.caption("※ ホーム画面に追加できたら、今開いている古い画面は閉じてしまって大丈夫です。")
    
    st.divider()
    
    if st.button("すでに保存した / すぐに記録を開始する", use_container_width=True):
        del st.session_state.just_logged_in
        st.rerun()

# --- 日報機能の復活 ---
def show_daily_report():
    st.markdown("<h2 style='font-size: clamp(1.2rem, 5vw, 2rem); margin-bottom: 1rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;' title='📝 日報（退勤報告）'>📝 日報（退勤報告）</h2>", unsafe_allow_html=True)
    
    user = st.session_state.logged_in_user
    st.write(f"**{user}** さん、お疲れ様です！")
    
    with st.spinner("提出状況を確認しています..."):
        reports_df = load_from_firestore(db, "daily_reports")
        
    today = datetime.now(timezone(timedelta(hours=9))).date()
    
    st.markdown("<h5 style='font-size: clamp(0.9rem, 3.5vw, 1.1rem); margin-bottom: 10px;'>📅 直近1週間の提出状況</h5>", unsafe_allow_html=True)
    
    html_blocks = ['<div style="display: flex; justify-content: space-between; gap: 4px; margin-bottom: 20px;">']
    
    for i in range(7):
        d = today - timedelta(days=6-i)
        d_str = d.strftime('%Y-%m-%d')
        day_label = ["月", "火", "水", "木", "金", "土", "日"][d.weekday()]
        disp_date = f"{d.month}/{d.day}"
        
        is_submitted = False
        if not reports_df.empty and '提出者' in reports_df.columns and '日付' in reports_df.columns:
            if not reports_df[(reports_df['提出者'] == user) & (reports_df['日付'] == d_str)].empty:
                is_submitted = True
                
        if is_submitted:
            bg_color, text_color, border_color = "#d1fae5", "#065f46", "#34d399"
            status_text = "✅済"
        elif d == today:
            bg_color, text_color, border_color = "#fef3c7", "#92400e", "#fbbf24"
            status_text = "📝今日"
        else:
            bg_color, text_color, border_color = "#f3f4f6", "#6b7280", "#e5e7eb"
            status_text = "－"
            
        date_weight = "bold" if d == today else "normal"
        date_color = "#333" if d == today else "#666"
            
        block = f'''<div style="flex: 1; text-align: center; font-size: clamp(0.6rem, 2.5vw, 0.85rem);">
<div style="color: {date_color}; font-weight: {date_weight}; margin-bottom: 4px; line-height: 1.2;">{disp_date}<br>({day_label})</div>
<div style="background-color: {bg_color}; color: {text_color}; padding: 4px 0; border-radius: 6px; border: 1px solid {border_color}; font-weight: bold; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title="{status_text}">{status_text}</div>
</div>'''
        html_blocks.append(block)
        
    html_blocks.append('</div>')
    st.markdown("".join(html_blocks), unsafe_allow_html=True)
                
    st.divider()
    
    target_date = st.date_input(
        "📅 提出（または確認）する対象日を選択してください",
        value=today
    )
    target_date_str = target_date.strftime('%Y-%m-%d')

    is_target_submitted = False
    submitted_report = None
    if not reports_df.empty and '提出者' in reports_df.columns and '日付' in reports_df.columns:
        my_target_reports = reports_df[(reports_df['提出者'] == user) & (reports_df['日付'] == target_date_str)]
        if not my_target_reports.empty:
            is_target_submitted = True
            submitted_report = my_target_reports.iloc[0].to_dict()
            
    if is_target_submitted:
        leave_time = submitted_report.get("退勤時間", "不明")
        st.success(f"🎉 **この日の日報はすでに提出済みです！** (退勤記録: {leave_time})")
        with st.expander("提出した内容を確認する"):
            if submitted_report.get('漏れている作業'):
                st.write(f"- **追加申告した作業:** {submitted_report.get('漏れている作業', '')}")
            st.write(f"- **機械の調子:** {submitted_report.get('機械の調子', '')}")
            st.write(f"- **ヒヤリハット:** {submitted_report.get('ヒヤリハット', '')}")
            st.write(f"- **特記事項:** {submitted_report.get('特記事項', '')}")
            if submitted_report.get('写真データ'):
                st.write("- **添付写真:** あり（データベースに保存されています）")

    with st.spinner(f"{target_date.strftime('%Y年%m月%d日')} の作業履歴をまとめています..."):
        in_prog_df = load_from_firestore(db, "in_progress")
        comp_df = load_from_firestore(db, "completed", days_limit=30)
        
        if not in_prog_df.empty:
            in_prog_df['_collection'] = "in_progress"
        if not comp_df.empty:
            comp_df['_collection'] = "completed"
            
        all_df = pd.concat([in_prog_df, comp_df], ignore_index=True)
        
    today_tasks = pd.DataFrame()
    other_tasks = pd.DataFrame()
    
    if not all_df.empty and '作成日時' in all_df.columns:
        all_df['作成日時_dt'] = pd.to_datetime(all_df['作成日時'], utc=True).dt.tz_convert('Asia/Tokyo')
        today_df = all_df[all_df['作成日時_dt'].dt.date == target_date]
        
        def is_involved(row):
            if row.get('入力者名') == user: return True
            co_workers = row.get('共同作業者', [])
            if isinstance(co_workers, list) and user in co_workers: return True
            if isinstance(co_workers, str) and user in co_workers: return True
            return False
            
        if not today_df.empty:
            involved_mask = today_df.apply(is_involved, axis=1)
            today_tasks = today_df[involved_mask].sort_values('作成日時_dt')
            other_tasks = today_df[~involved_mask].sort_values('作成日時_dt')

    st.markdown(f"<h3 style='font-size: clamp(1rem, 4vw, 1.4rem); white-space: nowrap; overflow: hidden; text-overflow: ellipsis;' title='📋 {target_date.strftime('%m月%d日')} のあなたの作業履歴'>📋 {target_date.strftime('%m月%d日')} のあなたの作業履歴</h3>", unsafe_allow_html=True)
    
    if today_tasks.empty:
        st.info("この日の作業記録はまだありません。（※補助として参加した場合、機長が未入力の可能性があります）")
    else:
        for idx, row in today_tasks.iterrows():
            product = row.get('製品名', '名称不明')
            process = row.get('工程名', '工程不明')
            detail = row.get('詳細', '')
            qty = int(row.get('出来数', 0))
            status = row.get('ステータス', '')
            is_helper = row.get('入力者名') != user
            
            machine = row.get('使用機械', '')
            rotation = int(row.get('回転数', 0)) if pd.notna(row.get('回転数', 0)) else 0
            
            helper_text = "（👤補助として参加）" if is_helper else ""
            
            extra_info = f"{qty:,}個 / 詳細: {detail} / 状態: {status}"
            if machine:
                extra_info += f" / 機械: {machine}"
            if rotation > 0:
                extra_info += f" / 回転数: {rotation:,}"
                
            st.markdown(f"- **{product}** ＞ {process} {helper_text}  \n  └ {extra_info}")

    with st.expander("🔍 手伝ったのに上の履歴にない場合はここをクリック", expanded=False):
        st.markdown("他の人が入力した作業記録から、自分が手伝った作業を見つけて「共同作業者」として名前を追加できます。")
        
        if other_tasks.empty:
            st.info("※ この日に行われた他の作業記録は見つかりませんでした。（機長がまだ作業を入力していない可能性があります。その場合は下の「特記事項」にメモを残してください）")
        else:
            task_options = {}
            for idx, row in other_tasks.iterrows():
                time_str = row['作成日時_dt'].strftime('%H:%M')
                product = row.get('製品名', '名称不明')
                process = row.get('工程名', '工程不明')
                worker = row.get('入力者名', '不明')
                machine = row.get('使用機械', '')
                machine_str = f"[{machine}] " if machine else ""
                qty = int(row.get('出来数', 0))
                
                label = f"{time_str} {worker}さんが入力: {product} ＞ {process} {machine_str}({qty}個)"
                task_options[label] = row
                
            selected_task_label = st.selectbox("手伝った作業を選んでください", ["（ここから作業を選択）"] + list(task_options.keys()))
            
            if selected_task_label != "（ここから作業を選択）":
                target_row = task_options[selected_task_label]
                
                if st.button("🙋‍♂️ この作業の「共同作業者」に自分を追加する", type="primary"):
                    try:
                        with st.spinner("データベースを更新中..."):
                            raw_co_workers = target_row.get('共同作業者', [])
                            if isinstance(raw_co_workers, list):
                                new_co_workers = list(raw_co_workers)
                            elif isinstance(raw_co_workers, str):
                                new_co_workers = [w.strip() for w in raw_co_workers.split(',')] if raw_co_workers else []
                            else:
                                new_co_workers = []
                                
                            if user not in new_co_workers:
                                new_co_workers.append(user)
                                
                            collection_name = target_row.get('_collection')
                            doc_id = target_row.get('id')
                            
                            if collection_name and doc_id:
                                db.collection(collection_name).document(doc_id).update({
                                    '共同作業者': new_co_workers
                                })
                                st.success("✅ 作業履歴にあなたを追加しました！画面を更新します。")
                                load_from_firestore.clear()
                                st.rerun()
                            else:
                                st.error("データエラーにより追加できませんでした。")
                    except Exception as e:
                        st.error(f"追加中にエラーが発生しました: {e}")
        
        st.divider()
        st.markdown("<h4 style='font-size: clamp(0.9rem, 3.5vw, 1.2rem); margin-bottom: 0.5rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;'>✍️ 上のリストにも作業が見つからない場合</h4>", unsafe_allow_html=True)
        missing_work_val = submitted_report.get('漏れている作業', '') if is_target_submitted else ""
        missing_work = st.text_area(
            "機長がまだ入力していない作業などは、こちらに直接メモしてください", 
            value=missing_work_val, 
            placeholder="例: 13:00〜14:00 〇〇の折り作業を手伝いました",
            height=80
        )

    st.divider()
    
    with st.form("daily_report_form"):
        st.markdown("<h3 style='font-size: clamp(1rem, 4vw, 1.4rem); margin-bottom: 0.5rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;' title='⏰ 退勤時間の記録（残業申請）'>⏰ 退勤時間の記録（残業申請）</h3>", unsafe_allow_html=True)
        st.info("定時の17:30を超えて作業する場合（残業した場合）に退勤時間を選択してください。")
        
        time_options = ["残業なし（定時退社）"] + [f"{h:02d}:{m:02d}" for h in range(17, 24) for m in (0, 15, 30, 45) if (h == 17 and m >= 45) or h > 17]
        
        default_time_str = "残業なし（定時退社）"
        if is_target_submitted:
            default_time_str = submitted_report.get("退勤時間", "残業なし（定時退社）")
        elif target_date == today:
            now_dt = datetime.now(timezone(timedelta(hours=9)))
            if now_dt.hour >= 17:
                rounded_minute = (now_dt.minute // 15) * 15
                guess_time = f"{now_dt.hour:02d}:{rounded_minute:02d}"
                if guess_time in time_options:
                    default_time_str = guess_time
            
        default_index = time_options.index(default_time_str) if default_time_str in time_options else 0
        leave_time_str = st.selectbox("退勤時間", time_options, index=default_index)
        
        st.divider()
        st.markdown("<h3 style='font-size: clamp(1rem, 4vw, 1.4rem); margin-bottom: 0.5rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;' title='💡 報告事項'>💡 報告事項</h3>", unsafe_allow_html=True)
        
        mac_default = 0
        hiyari_default = 0
        report_text_val = ""
        
        if is_target_submitted:
            mac_val = submitted_report.get('機械の調子', '')
            if "ちょっと" in mac_val: mac_default = 1
            elif "修理" in mac_val: mac_default = 2
            elif "使っていない" in mac_val: mac_default = 3
            
            if "あり" in submitted_report.get('ヒヤリハット', ''): hiyari_default = 1
            
            report_text_val = submitted_report.get('特記事項', '')

        machine_cond = st.radio("機械の調子はどうでしたか？", ["✨ 絶好調", "🔧 ちょっと変な音がした", "⚠️ 修理が必要", "➖ 機械は使っていない"], index=mac_default)
            
        hiyari = st.radio("ヒヤリハット・ミスはありましたか？", ["なし", "あり（下の特記事項に記入してください）"], index=hiyari_default)
        
        report_text = st.text_area("特記事項（トラブル、気づき、明日の申し送りなど）", value=report_text_val, height=100)
        
        st.info("現場の状況を伝えるため、任意で写真を追加できます。（1枚のみ）")
        uploaded_photo = st.file_uploader("📷 写真を追加", type=["jpg", "jpeg", "png"])
        
        submit_btn_label = f"{target_date.strftime('%m/%d')} の日報を上書きして再提出" if is_target_submitted else f"{target_date.strftime('%m/%d')} の日報を送信する"
        submitted = st.form_submit_button(submit_btn_label, type="primary", use_container_width=True)
        
        if submitted:
            photo_base64 = ""
            if uploaded_photo:
                try:
                    img = Image.open(uploaded_photo)
                    if img.mode != 'RGB': img = img.convert('RGB')
                    img.thumbnail((800, 800))
                    buffered = io.BytesIO()
                    img.save(buffered, format="JPEG", quality=70)
                    photo_base64 = f"data:image/jpeg;base64,{base64.b64encode(buffered.getvalue()).decode()}"
                except Exception as e:
                    st.error(f"写真の処理に失敗しました。写真は保存されません: {e}")

            report_data = {
                "提出者": user,
                "日付": target_date.strftime('%Y-%m-%d'),
                "作成日時": firestore.SERVER_TIMESTAMP,
                "退勤時間": leave_time_str, 
                "漏れている作業": missing_work,
                "機械の調子": machine_cond,
                "ヒヤリハット": hiyari,
                "特記事項": report_text,
                "写真データ": photo_base64,
                "関連タスク数": len(today_tasks)
            }
            
            try:
                if is_target_submitted:
                    db.collection("daily_reports").document(submitted_report['id']).delete()
                    
                db.collection("daily_reports").add(report_data)
                
                msg_action = "再提出" if is_target_submitted else "送信"
                st.session_state.success_msg = f"🎉 {target_date.strftime('%m/%d')} の日報を{msg_action}しました！お疲れ様でした！\n(退勤記録: {leave_time_str})"
                
                load_from_firestore.clear()
                st.rerun()
            except Exception as e:
                st.error(f"日報の送信に失敗しました: {e}")

# --- 追加：Step1のフラグメント化（ロードのチラつき防止） ---
@st.fragment
def render_step1_fragment(schedule_df, display_df, selected_location, product_to_location):
    st.markdown(f"<h3 style='font-size: clamp(0.9rem, 3.5vw, 1.4rem); white-space: nowrap; overflow: hidden; text-overflow: ellipsis;'>Step 1: 新規工程を記録（{selected_location}）</h3>", unsafe_allow_html=True)
    
    filtered_schedule_df = schedule_df.copy()
    if selected_location != "すべて":
        if not filtered_schedule_df.empty and '拠点' in filtered_schedule_df.columns:
            filtered_schedule_df = filtered_schedule_df[filtered_schedule_df['拠点'] == selected_location]
    
    customer_names = []
    if not filtered_schedule_df.empty and '得意先名' in filtered_schedule_df.columns:
        customer_names = sorted(filtered_schedule_df['得意先名'].dropna().unique().tolist())
    
    # ▼ 得意先を選ぶと、ここから下だけが更新される（画面全体はチカチカしない！）
    selected_customer = st.selectbox("得意先名で絞り込み", ["すべての得意先"] + customer_names, key="customer_choice")
    
    with st.form(key="selection_form"):
        product_list_df = filtered_schedule_df.copy()
        if selected_customer != "すべての得意先":
            product_list_df = product_list_df[product_list_df['得意先名'] == selected_customer]

        schedule_products = []
        if not product_list_df.empty and '品名' in product_list_df.columns:
            schedule_products = product_list_df['品名'].dropna().unique().tolist()
            
        in_progress_products_in_location = []
        if not display_df.empty:
            if '得意先名' not in display_df.columns and '品名' in schedule_df.columns and '得意先名' in schedule_df.columns:
                product_to_customer_map = schedule_df.drop_duplicates(subset=['品名']).set_index('品名')['得意先名'].to_dict()
                display_df['得意先名'] = display_df['製品名'].map(product_to_customer_map)

            if selected_customer != "すべての得意先":
                if '得意先名' in display_df.columns:
                    in_progress_products_in_location = display_df[display_df['得意先名'] == selected_customer]['製品名'].unique().tolist()
            else:
                in_progress_products_in_location = display_df['製品名'].unique().tolist()
        
        product_list = sorted(list(set(schedule_products + in_progress_products_in_location)))
        
        options = [""] + product_list
        
        default_product_index = 0
        if 'product_choice_final' in st.session_state and st.session_state.product_choice_final in options:
            default_product_index = options.index(st.session_state.product_choice_final)

        selected_product = st.selectbox("製品を選択（リスト内で検索もできます）", options, index=default_product_index)
        
        if selected_product and selected_product != "" and not schedule_df.empty and '品名' in schedule_df.columns:
            clean_selected = clean_text(selected_product)
            if 'clean_品名_for_match' not in schedule_df.columns:
                schedule_df['clean_品名_for_match'] = schedule_df['品名'].apply(clean_text)
                
            preview_row = schedule_df[schedule_df['clean_品名_for_match'] == clean_selected]
            if not preview_row.empty:
                p_info = preview_row.iloc[0]
                p_qty = p_info.get(SCHEDULE_COL_TOTAL_QUANTITY, "ー")
                p_detail = p_info.get(SCHEDULE_COL_DETAILS, "ー")
                p_due = p_info.get(SCHEDULE_COL_DUE_DATE, "ー")
                
                remarks_list = [str(p_info[col]) for col in SCHEDULE_COL_REMARKS if col in p_info and pd.notna(p_info[col])]
                p_memo = " | ".join(remarks_list)
                
                preview_text = f"📦 **総数:** {p_qty}　📅 **納期:** {p_due}　📝 **適用:** {p_detail}"
                if p_memo:
                    preview_text += f"\n💡 **備考:** {p_memo}"
                
                st.info(preview_text)

        allow_manual_input = st.checkbox("リストにない製品を手入力する")
        product_name_manual = st.text_input("新しい製品名を入力")
        process_name = st.selectbox("記録する工程名", PROCESS_OPTIONS)
        
        submitted = st.form_submit_button("この工程の入力を開始する", type="primary")

        if submitted:
            product_name = product_name_manual if allow_manual_input and product_name_manual else selected_product
            if not product_name or not process_name:
                st.error("製品名と工程名を両方選択してください。")
            else:
                if 'default_page_count' in st.session_state: del st.session_state.default_page_count
                if 'schedule_info_display' in st.session_state: del st.session_state.schedule_info_display
                if 'auto_selected_location' in st.session_state: del st.session_state.auto_selected_location

                if not schedule_df.empty and '品名' in schedule_df.columns:
                    clean_target = clean_text(product_name)
                    if 'clean_品名_for_match' not in schedule_df.columns:
                        schedule_df['clean_品名_for_match'] = schedule_df['品名'].apply(clean_text)
                    product_row = schedule_df[schedule_df['clean_品名_for_match'] == clean_target]
                    
                    if not product_row.empty:
                        info = product_row.iloc[0]
                        st.session_state.auto_selected_location = product_to_location.get(clean_target, "未設定")
                        if process_name in ["中綴じ", "無線綴じ", "糸かがり", "綴じ（カレンダー）"] and SCHEDULE_COL_PAGE_COUNT in schedule_df.columns:
                            page_count_val = pd.to_numeric(info.get(SCHEDULE_COL_PAGE_COUNT), errors='coerce')
                            st.session_state.default_page_count = int(page_count_val) if pd.notna(page_count_val) else 0
                        
                st.session_state.selected_product = product_name
                st.session_state.selected_process = process_name
                st.session_state.sub_view = 'INPUT_FORM'
                # 入力開始ボタンを押した時は画面全体を遷移させる
                st.rerun()

def main_app():
    if 'product_to_select' in st.session_state:
        selected_product_name = st.session_state.product_to_select
        st.session_state.product_choice_final = selected_product_name
        
        schedule_df_for_lookup = load_csv_data(SCHEDULE_FILE)
        if not schedule_df_for_lookup.empty and '品名' in schedule_df_for_lookup.columns and '得意先名' in schedule_df_for_lookup.columns:
            customer_row = schedule_df_for_lookup[schedule_df_for_lookup['品名'] == selected_product_name]
            if not customer_row.empty:
                customer_name = customer_row['得意先名'].dropna().iloc[0]
                st.session_state.customer_choice = customer_name
            else:
                st.session_state.customer_choice = "すべての得意先"
        else:
            st.session_state.customer_choice = "すべての得意先"
            
        del st.session_state.product_to_select

    if 'success_msg' in st.session_state and st.session_state.success_msg:
        st.success(st.session_state.success_msg)
        del st.session_state.success_msg

    st.sidebar.success(f"ログイン中: **{st.session_state.logged_in_user}**")
    if st.sidebar.button("ログアウト"):
        for key in list(st.session_state.keys()):
            if key not in ['_sidebar_state', 'query_params']:
                del st.session_state[key]
        st.query_params.clear()
        load_from_firestore.clear()
        load_tasks_for_customer.clear()
        st.rerun()
    
    st.sidebar.divider()
    
    def clear_cache_and_rerun():
        load_from_firestore.clear()
        load_tasks_for_customer.clear()
        load_csv_data.clear()
        st.rerun()
        
    st.sidebar.button("データを更新", on_click=clear_cache_and_rerun, use_container_width=True)
    
    with st.sidebar.expander("🛠️ 管理者メニュー (CSV手動更新)"):
        st.info("朝の自動更新が失敗した場合などに、ここから今日の予定表を一時的に読み込ませることができます。")
        uploaded_file = st.file_uploader("予定表 (schedule.csv) をアップロード", type=['csv'])
        if uploaded_file is not None:
            try:
                df = pd.read_csv(uploaded_file, encoding="utf-8-sig")
                st.session_state.manual_schedule_df = df
                st.success("✅ 手動アップロードされたCSVを適用しました！")
                if st.button("画面を更新して反映する", use_container_width=True):
                    load_csv_data.clear()
                    st.rerun()
            except Exception as e:
                st.error(f"読み込みエラー: {e}")

    main_view = st.radio(
        "メニューを選択", 
        ["🔧 通常工程の記録", "📦 名入れ一括登録", "📝 日報（退勤報告）"], 
        horizontal=True,
        label_visibility="collapsed"
    )
    st.divider()
    
    if main_view == "🔧 通常工程の記録":
        # ▼ スピナーを外してロードのチカチカを防止
        in_progress_df = load_from_firestore(db, "in_progress")
        st.session_state.in_progress_df = in_progress_df

        if 'naire_parent_customers' not in st.session_state:
            naire_df = load_from_firestore(db, "naire_master", active_only=True)
            st.session_state.naire_df = naire_df
            if not naire_df.empty and '得意先名' in naire_df.columns:
                st.session_state.naire_parent_customers = naire_df['得意先名'].dropna().unique().tolist()
            else:
                st.session_state.naire_parent_customers = []

        if st.session_state.sub_view == 'SELECT_PROCESS':
            schedule_df = load_csv_data(SCHEDULE_FILE)
            location_options = ["すべて", "旭川", "札幌"]
            product_to_location = {}
            if not schedule_df.empty and SCHEDULE_COL_LOCATION_CODE in schedule_df.columns and '品名' in schedule_df.columns:
                location_code_map = {1: "旭川", 2: "札幌"}
                schedule_df[SCHEDULE_COL_LOCATION_CODE] = pd.to_numeric(schedule_df[SCHEDULE_COL_LOCATION_CODE], errors='coerce')
                schedule_df['拠点'] = schedule_df[SCHEDULE_COL_LOCATION_CODE].map(location_code_map)
                
                for loc in sorted(schedule_df['拠点'].dropna().unique().tolist()):
                    if loc not in location_options:
                        location_options.append(loc)
                
                schedule_df['clean_品名'] = schedule_df['品名'].apply(clean_text)
                product_to_location = schedule_df.drop_duplicates(subset=['clean_品名']).set_index('clean_品名')['拠点'].to_dict()
                st.session_state.product_to_location = product_to_location
            else:
                st.warning(f"'{SCHEDULE_FILE}'に'{SCHEDULE_COL_LOCATION_CODE}'列が見つからないため、拠点機能が限定されます。")

            default_location = st.session_state.get("user_location", "すべて")
            default_index = 0
            if default_location in location_options:
                default_index = location_options.index(default_location)
            
            selected_location = st.selectbox("拠点を選択して表示を絞り込み", location_options, index=default_index)

            display_df = in_progress_df.copy()
            if not display_df.empty and "製品名" in display_df.columns:
                if '拠点' not in display_df.columns:
                    display_df['拠点'] = "未設定"
                
                unassigned_mask = display_df['拠点'].isin(['', '未設定', None]) | display_df['拠点'].isna()
                if unassigned_mask.any():
                    display_df['clean_製品名'] = display_df['製品名'].astype(str).str.strip()
                    display_df.loc[unassigned_mask, '拠点'] = display_df.loc[unassigned_mask, '入力者名'].map(WORKER_TO_LOCATION)
                
                display_df['拠点'].fillna('未設定', inplace=True)
                display_df.drop(columns=['clean_製品名'], inplace=True, errors='ignore')

                if selected_location != "すべて":
                    display_df = display_df[display_df['拠点'] == selected_location]
            
            col_form, col_list = st.columns(2)
            with col_form:
                # ▼ 変更: 入力フォームをフラグメント（部分更新）として呼び出す
                render_step1_fragment(schedule_df, display_df, selected_location, product_to_location)

            with col_list:
                st.markdown(f"<h3 style='font-size: clamp(0.9rem, 3.5vw, 1.4rem); white-space: nowrap; overflow: hidden; text-overflow: ellipsis;'>進行中の作業一覧（{selected_location}）</h3>", unsafe_allow_html=True)
                
                view_filter = st.radio(
                    "表示フィルター",
                    ["すべて表示", "通常工程のみ", "名入れ作業のみ"],
                    index=1,
                    horizontal=True,
                    key="view_filter"
                )
                
                naire_parent_customers = st.session_state.get("naire_parent_customers", [])
                if view_filter == "通常工程のみ":
                    if not display_df.empty and '製品名' in display_df.columns:
                        display_df = display_df[~display_df['製品名'].isin(naire_parent_customers)]
                elif view_filter == "名入れ作業のみ":
                    if not display_df.empty and '製品名' in display_df.columns:
                        display_df = display_df[display_df['製品名'].isin(naire_parent_customers)]

                if display_df.empty:
                    st.info("現在、選択された条件で作業中の製品はありません。")
                else:
                    for product, group_df in display_df.groupby('製品名'):
                        location_name = group_df['拠点'].iloc[0] if '拠点' in group_df.columns else "未設定"
                        with st.expander(f"**{product}** (拠点: {location_name} | 工程数: {len(group_df)})"):
                            col_exp_1, col_exp_2 = st.columns(2)
                            if col_exp_1.button("この製品に工程を追加", key=f"add_to_{product}", use_container_width=True):
                                st.session_state.product_to_select = product
                                st.rerun()
                            if col_exp_2.button("この製品の作業を完了", key=f"complete_{product}", type="primary", use_container_width=True):
                                handle_product_completion(product)
                            st.divider()
                            for index, row in group_df.iterrows():
                                c1, c2 = st.columns([4, 1])
                                with c1:
                                    worker_name_display = row.get('入力者名', '不明')
                                    machine_display = row.get('使用機械', '')
                                    caption_text = f"工程: {row['工程名']} ({row['詳細']}) | 出来数: {row['出来数']}個 | 入力者: {worker_name_display}"
                                    if machine_display:
                                        caption_text += f" | 機械: {machine_display}"
                                    st.caption(caption_text)
                                if c2.button("編集", key=f"edit_{row['id']}", use_container_width=True):
                                    st.session_state.record_to_edit = row.to_dict()
                                    st.session_state.sub_view = 'EDIT_FORM'
                                    st.rerun()
                                with st.popover("削除"):
                                    st.markdown("本当にこの工程を削除しますか？")
                                    if st.button("はい、削除します", key=f"delete_confirm_{row['id']}", type="primary"):
                                        try:
                                            if not firebase_admin._apps:
                                                init_firebase()
                                            db_del = firestore.client()
                                            db_del.collection("in_progress").document(row['id']).delete()
                                            load_from_firestore.clear()
                                            st.success(f"作業記録 (ID: {row['id']}) を削除しました。")
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"削除中にエラーが発生しました: {e}")
                                st.divider()
        elif st.session_state.sub_view == 'INPUT_FORM':
            process_form(is_edit_mode=False)
        elif st.session_state.sub_view == 'EDIT_FORM':
            record_to_edit = st.session_state.get('record_to_edit')
            if record_to_edit:
                process_form(is_edit_mode=True, default_data=record_to_edit)
            else:
                st.error("編集対象が指定されていません。")
                st.session_state.sub_view = 'SELECT_PROCESS'

    elif main_view == "📦 名入れ一括登録":
        st.header("名入れ工程の進捗管理")
        with st.spinner("名入れマスタを読み込んでいます..."):
            naire_df = load_from_firestore(db, "naire_master", active_only=True)
            st.session_state.naire_df = naire_df
        
        if naire_df.empty:
            st.warning(f"名入れマスタデータが登録されていません。")
            st.info("管理者の方は、「名入れマスタ管理アプリ」から新しいデータを登録してください。")
        else:
            parent_customers = sorted(naire_df['得意先名'].dropna().unique())
            selected_parent_customer = st.selectbox("対象の得意先を選択してください", [""] + parent_customers)

            if selected_parent_customer:
                with st.spinner(f"「{selected_parent_customer}」の作業記録を読み込んでいます..."):
                    tasks_df = load_tasks_for_customer(db, selected_parent_customer)
                
                st.subheader("工程進捗ボード")
                board_processes = ["断裁", "丁合", "綴じ", "綴じ+梱包", "メクレルト", "梱包"]
                master_list_for_customer = naire_df[naire_df['得意先名'] == selected_parent_customer]
                
                uncompleted_master_list = pd.DataFrame()
                if '完了ステータス' in master_list_for_customer.columns:
                    uncompleted_master_list = master_list_for_customer[master_list_for_customer['完了ステータス'] != '出荷待ち'].copy()
                else:
                    st.warning("⚠️ 「完了ステータス」列がマスタデータに見つかりません。すべての項目を表示します。")
                    uncompleted_master_list = master_list_for_customer.copy()
                
                st.write("**進捗状況（完了した会社は一覧から消えます）**")
                all_companies = sorted(uncompleted_master_list['会社名'].dropna().unique())
                board_data = []
                for company in all_companies:
                    row_data = {"会社名": company}
                    for process in board_processes:
                        is_done = False
                        if not tasks_df.empty and "詳細" in tasks_df.columns and "工程名" in tasks_df.columns:
                            match = tasks_df[
                                (tasks_df['詳細'] == company) & 
                                (tasks_df['製品名'] == selected_parent_customer) &
                                (tasks_df['工程名'] == process)
                            ]
                            if not match.empty:
                                is_done = True
                        row_data[process] = "✅" if is_done else ""
                    board_data.append(row_data)

                if board_data:
                    st.dataframe(pd.DataFrame(board_data).set_index("会社名"), use_container_width=True)
                else:
                    st.info("この得意先のすべての名入れ工程は完了（出荷待ち）です。")

                if 'naire_reset_key' not in st.session_state:
                    st.session_state.naire_reset_key = 0

                with st.expander("新しい工程を一括登録・完了する", expanded=True):
                    target_list_df = uncompleted_master_list.copy()
                    st.write("**1. 登録/完了する会社をチェック**")
                    
                    task_status = {}
                    if not tasks_df.empty:
                        for _, row in tasks_df.iterrows():
                            company = row.get('詳細')
                            process = row.get('工程名')
                            if company and process:
                                if company not in task_status: task_status[company] = []
                                task_status[company].append(process)

                    checked_items = []
                    
                    if target_list_df.empty:
                        st.info("この得意先のすべての名入れ工程は完了（出荷待ち）です。")
                    else:
                        def get_check_key(row_id):
                            return f"check_{row_id}_{st.session_state.naire_reset_key}"

                        col1_select, col2_select, _ = st.columns([1,1,4])
                        if col1_select.button("すべて選択", key="select_all_btn"):
                            for index, row in target_list_df.iterrows():
                                st.session_state[get_check_key(row['id'])] = True
                            st.rerun()
                        if col2_select.button("すべて解除", key="deselect_all_btn"):
                            for index, row in target_list_df.iterrows():
                                st.session_state[get_check_key(row['id'])] = False
                            st.rerun()

                        for index, row in target_list_df.iterrows():
                            company_name = row.get('会社名', '名称なし')
                            quantity_raw = pd.to_numeric(row.get('数量', 0), errors='coerce')
                            quantity_val = 0 if pd.isna(quantity_raw) else int(quantity_raw)
                            delivery_date = row.get('納期', '未設定')
                            
                            done_processes = task_status.get(company_name, [])
                            done_badges = " ".join([f"`{p}`" for p in done_processes]) if done_processes else "未着手"
                            
                            label = f"**{company_name}**\n  📅 納期: {delivery_date} | 📦 部数: {quantity_val} | 📝 完了: {done_badges}"
                            
                            key = get_check_key(row['id'])
                            
                            c_check, c_edit = st.columns([0.85, 0.15])
                            with c_check:
                                if st.checkbox(label, key=key):
                                    checked_items.append(row)
                            with c_edit:
                                with st.popover("編集"):
                                    st.write(f"**{company_name}** を編集・削除")
                                    with st.form(key=f"edit_master_{row['id']}"):
                                        new_name = st.text_input("会社名", value=company_name)
                                        try:
                                            default_date = pd.to_datetime(delivery_date).date()
                                        except:
                                            default_date = None
                                        new_date = st.date_input("納期", value=default_date)
                                        new_qty = st.number_input("部数", value=quantity_val, step=1)
                                        
                                        if st.form_submit_button("変更を保存"):
                                            update_data = {
                                                "会社名": new_name,
                                                "数量": new_qty,
                                                "納期": new_date.strftime('%Y/%m/%d') if new_date else ""
                                            }
                                            if not firebase_admin._apps:
                                                init_firebase()
                                            db.collection("naire_master").document(row['id']).update(update_data)
                                            load_from_firestore.clear()
                                            st.session_state.success_msg = f"「{company_name}」の情報を更新しました。"
                                            st.rerun()
                                    
                                    st.divider()
                                    if st.button("削除する", key=f"del_master_{row['id']}", type="primary"):
                                        if not firebase_admin._apps:
                                            init_firebase()
                                        db.collection("naire_master").document(row['id']).delete()
                                        load_from_firestore.clear()
                                        st.session_state.success_msg = f"「{company_name}」を削除しました。"
                                        st.rerun()

                            st.divider()

                    st.write("**2. 登録する工程内容**")
                    process_name = st.selectbox("工程名", NAIRE_PROCESS_OPTIONS, key="bulk_process_name")

                    with st.form("bulk_form"):
                        current_process = st.session_state.get("bulk_process_name", "")
                        if current_process == '断裁':
                            work_time_input = st.selectbox("（チェックした全体の）合計作業時間（分）", [str(i * 10) for i in range(1, 73)])
                        elif not current_process:
                             st.info("まず上のメニューから工程を選択してください。")
                        else:
                            start_time_input = st.time_input("開始時間", step=600, value=time(9, 0))
                            end_time_input = st.time_input("終了時間", step=600, value=time(10, 0))
                        workers = st.number_input("作業人数", min_value=0.5, value=1.0, step=0.5, format="%.1f")
                        st.divider()
                        st.write("**3. 実行**")
                        col1, col2 = st.columns(2)
                        is_process_selected = current_process != ""
                        register_submitted = col1.form_submit_button("チェックした項目をまとめて登録", use_container_width=True, disabled=not is_process_selected)
                        complete_submitted = col2.form_submit_button("チェックした項目を完了にする (出荷待ち)", type="primary", use_container_width=True)

                        if register_submitted:
                            if not checked_items: st.warning("登録する項目がチェックされていません。"); st.stop()
                            checked_count = len(checked_items)
                            invalid_quantity_items = [item['会社名'] for item in checked_items if int(pd.to_numeric(item.get('数量', 0), errors='coerce')) <= 0]
                            if invalid_quantity_items: st.error(f"❌ 以下の項目は数量が0または無効: {', '.join(invalid_quantity_items)}"); st.stop()

                            total_work_time, start_time_str, end_time_str = 0, "", ""
                            if current_process == '断裁':
                                total_work_time = int(work_time_input)
                            else:
                                if not (start_time_input and end_time_input and end_time_input > start_time_input):
                                    st.error("終了時間は開始時間より後にしてください。"); st.stop()
                                delta = datetime.combine(datetime.today(), end_time_input) - datetime.combine(datetime.today(), start_time_input)
                                total_work_time = delta.total_seconds() / 60
                                start_time_str, end_time_str = start_time_input.strftime('%H:%M'), end_time_input.strftime('%H:%M')
                            
                            work_time_per_item = round(total_work_time / checked_count, 1)
                            batch = db.batch()
                            for item in checked_items:
                                new_record_data = {
                                    "入力者名": st.session_state.logged_in_user,
                                    "拠点": st.session_state.get('user_location', "未設定"),
                                    "記録ID": datetime.now().strftime("%Y%m%d%H%M%S%f") + f"_{item['id']}", 
                                    "製品名": selected_parent_customer, "工程名": current_process, "詳細": item.get('会社名', ''), 
                                    "開始時間": start_time_str, "終了時間": end_time_str, "作業時間_分": work_time_per_item,
                                    "出来数": int(pd.to_numeric(item.get('数量', 0), errors='coerce')), "作業人数": float(workers), 
                                    "ステータス": "作業中", "備考": item.get('備考', ''), "作成日時": firestore.SERVER_TIMESTAMP
                                }
                                batch.set(db.collection("in_progress").document(), new_record_data)
                            batch.commit()
                            
                            st.session_state.naire_reset_key += 1
                            st.session_state.success_msg = f"{len(checked_items)}件の記録を登録しました。"
                            st.rerun()

                        if complete_submitted:
                            if not checked_items: st.warning("完了にする項目がチェックされていません。"); st.stop()
                            batch = db.batch()
                            
                            company_names_to_complete = [item['会社名'] for item in checked_items]
                            in_progress_df = st.session_state.get('in_progress_df', pd.DataFrame())
                            
                            if not in_progress_df.empty:
                                docs_to_move = in_progress_df[
                                    (in_progress_df["製品名"] == selected_parent_customer) &
                                    (in_progress_df["詳細"].isin(company_names_to_complete))
                                ]
                                
                                for index, row in docs_to_move.iterrows():
                                    doc_data = row.to_dict(); doc_data['ステータス'] = '完了'
                                    if '拠点' not in doc_data or pd.isna(doc_data.get('拠点')) or doc_data.get('拠点') == '未設定':
                                        doc_data['拠点'] = st.session_state.get('user_location', "未設定")
                                    batch.set(db.collection("completed").document(), doc_data)
                                    batch.delete(db.collection("in_progress").document(row['id']))
                            
                                common_docs_to_move = in_progress_df[
                                    (in_progress_df["製品名"] == selected_parent_customer) &
                                    (in_progress_df["詳細"] == "")
                                ]
                                
                                remaining_companies = uncompleted_master_list[~uncompleted_master_list['会社名'].isin(company_names_to_complete)]
                                
                                if remaining_companies.empty:
                                    for index, common_row in common_docs_to_move.iterrows():
                                        doc_data = common_row.to_dict(); doc_data['ステータス'] = '完了'
                                        if '拠点' not in doc_data or pd.isna(doc_data.get('拠点')) or doc_data.get('拠点') == '未設定':
                                            doc_data['拠点'] = st.session_state.get('user_location', "未設定")
                                        batch.set(db.collection("completed").document(), doc_data)
                                        batch.delete(db.collection("in_progress").document(common_row['id']))

                            for item in checked_items:
                                batch.update(db.collection("naire_master").document(item['id']), {"完了ステータス": "出荷待ち"})
                            
                            batch.commit()
                            
                            load_from_firestore.clear()
                            load_tasks_for_customer.clear()
                            
                            st.session_state.naire_reset_key += 1
                            st.session_state.success_msg = f"{len(checked_items)}件を「出荷待ち」に更新し、関連する作業記録を「完了」に移動しました。"
                            st.rerun()

    elif main_view == "📝 日報（退勤報告）":
        show_daily_report()

st.set_page_config(layout="wide")

st.markdown("<h1 style='font-size: clamp(1.2rem, 5vw, 2.5rem); padding-top: 1rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;'>📘 製本記録アプリ</h1>", unsafe_allow_html=True)

if 'submit_disabled' not in st.session_state:
    st.session_state.submit_disabled = False

def disable_buttons():
    st.session_state.submit_disabled = True

if 'sub_view' not in st.session_state:
    st.session_state.sub_view = 'SELECT_PROCESS'

db = init_firebase()
if not db:
    st.stop()

params = st.query_params.to_dict()
url_uid = params.get("uid")

if 'logged_in_user' not in st.session_state:
    if url_uid and url_uid in ID_TO_WORKER:
        user_name = ID_TO_WORKER[url_uid]
        st.session_state.logged_in_user = user_name
        st.session_state.user_location = WORKER_TO_LOCATION.get(user_name, "すべて")

if 'logged_in_user' in st.session_state:
    if st.session_state.get("just_logged_in"):
        st.query_params.uid = WORKER_ID_MAP.get(st.session_state.logged_in_user, "")
        show_bookmark_page(st.session_state.logged_in_user)
    else:
        st.query_params.uid = WORKER_ID_MAP.get(st.session_state.logged_in_user, "")
        main_app()
else:
    login_screen()
