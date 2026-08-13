import streamlit as st
import pandas as pd
from datetime import datetime, time, timedelta, timezone
import unicodedata
import streamlit.components.v1 as components
import firebase_admin
from firebase_admin import credentials, firestore
import json
import os
import base64
import io
from PIL import Image

st.set_page_config(page_title="製本記録アプリ", layout="wide")

# 修正箇所: ここでCSSを一度だけ定義します。
st.markdown("""
<style>
input, textarea, select { font-size: 16px !important; }
/* スマホ画面（幅600px以下）でボタンを横並びにするためのクラス */
@media (max-width: 600px) {
    .button-container-row > div {
        display: flex;
        flex-direction: row !important;
        gap: 0.5rem;
    }
    .button-container-row > div > div {
        width: 33.33% !important;
    }
    .button-container-row button {
        width: 100% !important;
        padding: 0.25rem 0.5rem !important;
        font-size: 0.8rem !important;
        min-height: 0px !important;
    }
}
</style>
""", unsafe_allow_html=True)
components.html("""<script>const doc=window.parent.document; function d(){doc.querySelectorAll('div[data-baseweb="select"] input').forEach(i=>{if(i.getAttribute('inputmode')!=='none')i.setAttribute('inputmode','none');});} d(); new MutationObserver(d).observe(doc.body,{childList:true,subtree:true});</script>""", height=0, width=0)

def clean_text(text):
    if pd.isna(text): return ""
    return unicodedata.normalize('NFKC', str(text)).strip().replace(' ', '').replace('　', '')

SCHEDULE_FILE = "schedule.csv"
SCHEDULE_M_FILE = "schedule_m.csv"
PROCESS_OPTIONS = ["", "断裁", "折", "中綴じ", "無線綴じ", "ミシン・スジ", "角丸", "貼込", "糸かがり", "綴じ（カレンダー）", "丁合（カレンダー）", "穴明け", "梱包", "区分け", "手作業"]
NAIRE_PROCESS_OPTIONS = ["", "断裁", "丁合", "綴じ", "綴じ+梱包", "メクレルト", "梱包"]
CALENDAR_PROCESS_OPTIONS = ["", "断裁", "丁合", "綴じ", "綴じ+梱包", "梱包"]
FOLD_OPTIONS = ["", "4p", "6p", "8p", "16p", "その他"]

SCHEDULE_COL_PAGE_COUNT = "ページ数"
SCHEDULE_COL_TOTAL_QUANTITY = "総数"
SCHEDULE_COL_REMARKS = ["作業予定表備考1", "作業予定表備考2"]
SCHEDULE_COL_LOCATION_CODE = "拠点コード"
SCHEDULE_COL_DETAILS = "適用"
SCHEDULE_COL_DUE_DATE = "納期日付"
SCHEDULE_COL_DELIVERY_METHOD = "納品方法"
SCHEDULE_COL_DELIVERY_TIME = "納期時間"
SCHEDULE_COL_AMOUNT = "金額"

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

WORKER_NAMES = [
    "赤松 浩明", "浅野 央詞", "小松 宣彦", "小山 輝義", "佐々木 善直", "藤井 康彰", "荒田 朋子", "川井 千代宝", "木原 裕治", "蟹谷 和豊", "高橋 誠", "大文字 俊幸",
    "青塚 知代", "早川 健太", "石井 美津枝", "山下 泉", "小島 広勝", "菅原 加奈", "神馬 妃那", "ディアン ファトクローマン", "インドラ アデ カマルディン", "ムハマド ユヌス", "岳　匠", "立川　悠依", 
    "家常 貴史", "藤田 祐司", "田中 二郎", "内田 進", "若杉 瑞樹", "小柄 浩二", "蓬畑 皓一", "藤井 翔太", "佐々木 輝", "ノヴィ アナ", "カロマー ユニシャ", "モニカ ジュリヤニ", "岳 司郎", "福田 準也", "アンギ プラティウィ", "ナイシラ オクタヴィアニ", "チンタ フィトリヤニラマダニ"
]
ASAHIKAWA_MEMBERS = WORKER_NAMES[:24]
SAPPORO_MEMBERS = WORKER_NAMES[24:]
WORKER_TO_LOCATION = {name: "旭川" for name in ASAHIKAWA_MEMBERS}
WORKER_TO_LOCATION.update({name: "札幌" for name in SAPPORO_MEMBERS})
WORKER_ID_MAP = {name: f"A{i+1:02d}" if name in ASAHIKAWA_MEMBERS else f"S{i-23:02d}" for i, name in enumerate(WORKER_NAMES)}
ID_TO_WORKER = {v: k for k, v in WORKER_ID_MAP.items()}

@st.cache_resource
def init_firebase():
    try:
        if not firebase_admin._apps:
            if os.environ.get("FIREBASE_KEY_JSON"):
                cred = credentials.Certificate(json.loads(os.environ.get("FIREBASE_KEY_JSON")))
            elif "FIREBASE_KEY_JSON" in st.secrets:
                cred = credentials.Certificate(json.loads(st.secrets["FIREBASE_KEY_JSON"]))
            else:
                cred = credentials.Certificate("firebase_key.json")
            firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        st.error(f"DB接続エラー: {e}")
        return None

@st.cache_data(ttl=3600)
def load_csv_data(file_path):
    if file_path == SCHEDULE_FILE and 'manual_schedule_df' in st.session_state: return st.session_state.manual_schedule_df
    if file_path == SCHEDULE_M_FILE and 'manual_schedule_m_df' in st.session_state: return st.session_state.manual_schedule_m_df
    if file_path == SCHEDULE_FILE and "SCHEDULE_CSV_URL" in st.secrets and st.secrets["SCHEDULE_CSV_URL"]:
        try: return pd.read_csv(st.secrets["SCHEDULE_CSV_URL"], encoding="utf-8-sig")
        except: pass 
    try: return pd.read_csv(file_path, encoding="utf-8-sig")
    except: return pd.DataFrame()

@st.cache_data(ttl=600)
def load_from_firestore(_db, collection_name, active_only=False, days_limit=None):
    if not _db: return pd.DataFrame()
    try:
        query = _db.collection(collection_name)
        docs = query.order_by("`作成日時`", direction=firestore.Query.DESCENDING).limit(days_limit).stream() if days_limit else query.stream()
        records = [doc.to_dict() | {'id': doc.id} for doc in docs]
        df = pd.DataFrame(records) if records else pd.DataFrame()
        if active_only and not df.empty and "完了ステータス" in df.columns: df = df[df["完了ステータス"] != "出荷待ち"]
        return df
    except: return pd.DataFrame()

@st.cache_data(ttl=600)
def load_tasks_for_customer(_db, customer_name):
    if not _db or not customer_name: return pd.DataFrame()
    all_tasks = []
    for collection in ["in_progress", "completed"]:
        try:
            docs = _db.collection(collection).stream()
            records = [doc.to_dict() | {'id': doc.id} for doc in docs]
            if records:
                df = pd.DataFrame(records)
                if "製品名" in df.columns:
                    f_df = df[df["製品名"] == customer_name]
                    if not f_df.empty:
                        all_tasks.append(f_df)
        except Exception:
            pass
    return pd.concat(all_tasks, ignore_index=True) if all_tasks else pd.DataFrame()

def handle_db_write(operation, success_message, error_message, rerun_on_success=True, view_key='sub_view'):
    try:
        with st.spinner("処理中..."):
            if not firebase_admin._apps: init_firebase()
            operation()
            st.session_state.success_msg = success_message
            st.session_state[view_key] = 'SELECT_PROCESS' if view_key == 'sub_view' else 'SELECT'
            st.session_state.pop('record_to_copy', None)
            st.session_state.pop('cal_record_to_copy', None)
            st.session_state.pop('cal_bulk_items', None)
            if rerun_on_success:
                load_from_firestore.clear(); load_tasks_for_customer.clear(); st.rerun()
    except Exception as e: st.error(f"{error_message}: {e}")

def handle_update(doc_id, data_dict, view_key='sub_view'): handle_db_write(lambda: firestore.client().collection("in_progress").document(doc_id).update(data_dict), "記録を更新しました。", "更新中にエラー", view_key=view_key)
def handle_add_in_progress(data_dict, view_key='sub_view'): handle_db_write(lambda: firestore.client().collection("in_progress").add(data_dict), f"工程「{data_dict['工程名']}」を追加しました。", "追加中にエラー", view_key=view_key)

def handle_completion(new_data_dict, view_key='sub_view'):
    def op():
        db_b = firestore.client().batch()
        ip_df = st.session_state.get('in_progress_df', pd.DataFrame())
        if not ip_df.empty and "製品名" in ip_df.columns:
            for _, row in ip_df[ip_df["製品名"] == new_data_dict['製品名']].iterrows():
                d = row.to_dict(); d['ステータス'], d['完了日時'] = '完了', firestore.SERVER_TIMESTAMP
                if '拠点' not in d or pd.isna(d.get('拠点')) or d.get('拠点') == '未設定': d['拠点'] = st.session_state.get('product_to_location', {}).get(clean_text(d.get('製品名', '')), '未設定')
                db_b.set(firestore.client().collection("completed").document(), d)
                db_b.delete(firestore.client().collection("in_progress").document(row['id']))
        new_data_dict['完了日時'] = firestore.SERVER_TIMESTAMP
        db_b.set(firestore.client().collection("completed").document(), new_data_dict)
        db_b.commit()
    handle_db_write(op, f"✅ 「{new_data_dict['製品名']}」を確定しました。", "完了処理中にエラー", view_key=view_key)

def handle_product_completion(product_name, view_key='sub_view'):
    def op():
        db_b = firestore.client().batch()
        ip_df = st.session_state.get('in_progress_df', pd.DataFrame())
        mc = 0
        if not ip_df.empty and "製品名" in ip_df.columns:
            for _, row in ip_df[ip_df["製品名"] == product_name].iterrows():
                d = row.to_dict(); d['ステータス'], d['完了日時'] = '完了', firestore.SERVER_TIMESTAMP
                if '拠点' not in d or pd.isna(d.get('拠点')) or d.get('拠点') == '未設定': d['拠点'] = st.session_state.get('product_to_location', {}).get(clean_text(d.get('製品名', '')), '未設定')
                db_b.set(firestore.client().collection("completed").document(), d)
                db_b.delete(firestore.client().collection("in_progress").document(row['id']))
                mc += 1
        if mc == 0: return st.warning("記録が見つかりません。")
        db_b.commit()
    handle_db_write(op, f"✅ 「{product_name}」を作業完了にしました。", "完了処理中にエラー", view_key=view_key)

def process_form(is_edit_mode=False, default_data=None, view_key='sub_view', is_calendar=False, bulk_items=None):
    default_data = default_data or {}
    product_name = default_data.get('製品名', st.session_state.get('selected_product', ''))
    process_name = default_data.get('工程名', st.session_state.get('selected_process', ''))
    
    is_bulk = bulk_items is not None and len(bulk_items) > 0
    title_suffix = f" （一括 {len(bulk_items)}件）" if is_bulk else ""
    st.markdown(f"<h2 style='font-size: clamp(0.9rem, 3.5vw, 1.6rem); margin-bottom: 0;'>Step 2: 「{product_name}」の作業内容を記録{title_suffix}</h2>", unsafe_allow_html=True)
    st.markdown(f"<h3 style='font-size: clamp(0.8rem, 3vw, 1.2rem); color: #555; margin-top: 5px;'>工程: <b>{process_name}</b></h3>", unsafe_allow_html=True)
    
    schedule_df = load_csv_data(SCHEDULE_FILE)
    if not schedule_df.empty and '品名' in schedule_df.columns:
        schedule_df['clean_品名_for_match'] = schedule_df['品名'].apply(clean_text)
        product_row = schedule_df[schedule_df['clean_品名_for_match'] == clean_text(product_name)]
        if not product_row.empty:
            info = product_row.iloc[0]
            amt = info.get(SCHEDULE_COL_AMOUNT, 0)
            rmks = " | ".join([str(info[c]) for c in SCHEDULE_COL_REMARKS if c in info and pd.notna(info[c])])
            st.info("\n\n".join([f"**{k}:** {str(v).replace('*', '\*')}" for k, v in {"総数": info.get(SCHEDULE_COL_TOTAL_QUANTITY, ""), "受注金額": f"{int(amt):,}円" if pd.notna(amt) else "", "適用": info.get(SCHEDULE_COL_DETAILS, ""), "納期日付": info.get(SCHEDULE_COL_DUE_DATE, ""), "納期時間": info.get(SCHEDULE_COL_DELIVERY_TIME, ""), "備考": rmks}.items() if pd.notna(v) and str(v).strip() != ""]))
    
    def to_time_obj(t_str):
        try: return datetime.strptime(t_str, '%H:%M').time() if t_str else None
        except: return None
    with st.form(key='process_form'):
        user_loc = st.session_state.get('user_location', "未設定")
        detail_val = default_data.get('詳細', '')
        st_time_obj = to_time_obj(default_data.get('開始時間'))
        en_time_obj = to_time_obj(default_data.get('終了時間'))
        work_mins_in = 0
        setup_procs, rot_procs = ["中綴じ", "折", "無線綴じ", "糸かがり", "綴じ（カレンダー）", "丁合（カレンダー）"], ["折", "中綴じ", "無線綴じ", "ミシン・スジ", "貼込", "綴じ（カレンダー）", "丁合（カレンダー）"]
        set_w, set_t, rot_s, mach_sel = 0.0, 0, 0, ""
        st.subheader("機械情報")
        is_setup_only = st.checkbox("🔧 セット作業のみ", value=(is_edit_mode and int(default_data.get('出来数', 1)) == 0))
        
        mach_opts = list(ASAHIKAWA_MACHINES.get(process_name, [])) if user_loc == "旭川" else list(SAPPORO_MACHINES.get(process_name, [])) if user_loc == "札幌" else []
        def_mach = default_data.get('使用機械', None)
        if mach_opts:
            if process_name == "折":
                def_sels = [i.strip() for i in def_mach.split(',') if i.strip()] if isinstance(def_mach, str) else []
                for i in def_sels: 
                    if i not in mach_opts: mach_opts.append(i)
                mach_sel = ", ".join(st.multiselect("使用した折機", mach_opts, default=def_sels))
            else:
                if isinstance(def_mach, str) and def_mach not in mach_opts: mach_opts.append(def_mach)
                mach_sel = st.selectbox("使用した機械", mach_opts, index=mach_opts.index(def_mach) if def_mach in mach_opts else 0)
        else:
             mach_sel = def_mach if def_mach else ""
             if def_mach: st.info(f"記録された機械: {def_mach}")
             
        if process_name in setup_procs:
            c1, c2 = st.columns(2)
            with c1: set_w = st.number_input("セット人数", min_value=0.0, step=0.5, value=float(default_data.get('セット人数', 0.0)) if pd.notna(default_data.get('セット人数', 0.0)) else 0.0, format="%.1f")
            with c2: set_t = st.number_input("セット時間（分）", min_value=0, step=10, value=int(default_data.get('セット時間_分', 0)) if pd.notna(default_data.get('セット時間_分', 0)) else 0)
        if process_name in rot_procs:
            rot_s = st.number_input("機械回転数", min_value=0, step=100, value=int(default_data.get('回転数', 0)) if pd.notna(default_data.get('回転数', 0)) else 0)
        
        st.divider()
        st.subheader("作業実績")
        
        if is_bulk:
            total_qty = sum(item.get('出来数', 0) for item in bulk_items)
            qty = st.number_input("出来数（チェックした項目の合計）", min_value=0, value=int(total_qty), disabled=True)
            st.info("※一括登録のため、出来数は各会社の合算値が表示されています。")
        else:
            qty = 0 if is_setup_only else st.number_input("出来数", min_value=0, step=1, value=int(default_data.get('出来数', 0)), disabled=is_setup_only)
        
        workers = st.number_input("作業人数（合計）", min_value=0.5, step=0.5, value=float(default_data.get('作業人数', 1.0)), format="%.1f")
        
        base_w = ASAHIKAWA_MEMBERS if user_loc == "旭川" else SAPPORO_MEMBERS if user_loc == "札幌" else WORKER_NAMES
        other_w = [n for n in base_w if n != st.session_state.logged_in_user and n != "（自分の名前を選択してください）"]
        raw_cw = default_data.get('共同作業者', [])
        safe_cw = raw_cw if isinstance(raw_cw, list) else [w.strip() for w in raw_cw.split(',')] if isinstance(raw_cw, str) and raw_cw else []
        sel_cw = st.multiselect("👤 共同作業者", other_w, default=[w for w in safe_cw if w in other_w])
        st_label = "開始時間/※セット時間は含まない" if process_name in setup_procs else "開始時間"
        fin_dtl, st_o, en_o = detail_val, st_time_obj, en_time_obj
        
        if process_name == "断裁":
            t_opts = [str(i * 10) for i in range(1, 73)]
            d_wt = str(default_data.get('作業時間_分', 60))
            wt_str = st.selectbox("作業時間（分）", t_opts, index=t_opts.index(d_wt) if d_wt in t_opts else 5, disabled=is_setup_only)
            work_mins_in = 0 if is_setup_only else int(wt_str)
            fin_dtl, st_o, en_o = ("セットのみ" if is_setup_only else f"{wt_str}分"), None, None
        elif process_name == "手作業":
            fin_dtl = st.text_input("手作業の内容", value=detail_val)
            st_o = st.time_input("開始時間", step=600, value=st_time_obj, disabled=is_setup_only)
            en_o = st.time_input("終了時間", step=600, value=en_time_obj, disabled=is_setup_only)
        elif process_name == "折":
            sel_opts = st.multiselect("ページ数", [o for o in FOLD_OPTIONS if o], default=[i.strip() for i in detail_val.split(',')] if detail_val else [])
            fin_dtl = ", ".join(sel_opts)
            st_o = st.time_input(st_label, step=600, value=st_time_obj, disabled=is_setup_only)
            en_o = st.time_input("終了時間", step=600, value=en_time_obj, disabled=is_setup_only)
        elif process_name in ["中綴じ", "無線綴じ", "糸かがり", "綴じ（カレンダー）"]:
            try: d_pgs = int(detail_val) if is_edit_mode else st.session_state.get('default_page_count', 0)
            except: d_pgs = 0
            fin_dtl = str(st.number_input("ページ数／枚数", min_value=0, step=1, value=d_pgs))
            st_o = st.time_input(st_label, step=600, value=st_time_obj, disabled=is_setup_only)
            en_o = st.time_input("終了時間", step=600, value=en_time_obj, disabled=is_setup_only)
        elif process_name == "梱包":
            d_pt, d_ip, d_bc = "", 0, 0
            if is_edit_mode and detail_val:
                dtls = detail_val.split(" | ")
                d_pt = dtls[0] if dtls else ""
                for i in dtls[1:]:
                    if "個/包" in i: d_ip = int(i.replace("個/包", "").strip())
                    elif "箱" in i: d_bc = int(i.replace("箱", "").strip())
            pt = st.selectbox("作業内容", ["", "包装+箱", "包装のみ", "箱入れのみ", "結束"], index=["", "包装+箱", "包装のみ", "箱入れのみ", "結束"].index(d_pt) if d_pt in ["", "包装+箱", "包装のみ", "箱入れのみ", "結束"] else 0)
            ip = st.number_input("一包みの入数", min_value=0, step=1, value=d_ip) if "包装" in pt or "結束" in pt else 0
            bc = st.number_input("箱の数", min_value=0, step=1, value=d_bc) if "箱" in pt else 0
            d_list = [pt]
            if ip > 0: d_list.append(f"{ip}個/包")
            if bc > 0: d_list.append(f"{bc}箱")
            fin_dtl = " | ".join(d for d in d_list if d)
            st_o = st.time_input("開始時間", step=600, value=st_time_obj, disabled=is_setup_only)
            en_o = st.time_input("終了時間", step=600, value=en_time_obj, disabled=is_setup_only)
        else:
            fin_dtl = st.text_input("詳細（任意）", value=detail_val)
            st_o = st.time_input(st_label, step=600, value=st_time_obj, disabled=is_setup_only)
            en_o = st.time_input("終了時間", step=600, value=en_time_obj, disabled=is_setup_only)
        rmks = st.text_area("備考", value=default_data.get('備考', ''))
        
        cb1, cb2, cb3 = st.columns([1.2, 1.2, 2])
        btn_sub = cb1.form_submit_button("更新する" if is_edit_mode else "作業中として追加", type="primary" if is_edit_mode else "secondary", use_container_width=True)
        btn_com = None if is_edit_mode else cb2.form_submit_button("この内容で最終完了", type="primary", use_container_width=True)
        if cb3.form_submit_button("キャンセル"):
            st.session_state[view_key] = 'SELECT_PROCESS' if view_key == 'sub_view' else 'SELECT'
            st.session_state.pop('record_to_copy', None)
            st.session_state.pop('cal_record_to_copy', None)
            st.session_state.pop('cal_bulk_items', None)
            st.rerun()
            
        def submit_data(status):
            if not is_setup_only and qty <= 0 and not is_bulk: return st.error("❌ 出来数は1以上で入力してください。")
            wm, st_str, en_str = 0, "", ""
            if process_name == "断裁": wm = work_mins_in
            elif not is_setup_only:
                if not st_o or not en_o: return st.error("❌ 開始時間と終了時間は必須です。")
                if en_o <= st_o: return st.error("❌ 終了時間は開始時間より後にしてください。")
                wm = (datetime.combine(datetime.today(), en_o) - datetime.combine(datetime.today(), st_o)).total_seconds() / 60
                st_str, en_str = st_o.strftime('%H:%M'), en_o.strftime('%H:%M')
            
            base_f_data = {
                "入力者名": st.session_state.logged_in_user, "共同作業者": sel_cw, "拠点": user_loc, "使用機械": mach_sel,
                "工程名": process_name, "開始時間": st_str, "終了時間": en_str,
                "作業人数": float(workers), "ステータス": status, "備考": rmks,
                "作成日時": firestore.SERVER_TIMESTAMP, "セット人数": float(set_w), "セット時間_分": int(set_t), "回転数": int(rot_s),
                "is_calendar": is_calendar
            }
            
            if is_bulk:
                wm_per = int(wm / len(bulk_items)) if wm > 0 else 0
                def op():
                    b = firestore.client().batch()
                    for i, item in enumerate(bulk_items):
                        f = base_f_data.copy()
                        f.update({
                            "記録ID": f"{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{i}",
                            "製品名": product_name, "詳細": item.get('詳細', ''), "作業時間_分": wm_per, "出来数": int(item.get('出来数', 0))
                        })
                        if status == "完了":
                            f['完了日時'] = firestore.SERVER_TIMESTAMP
                            b.set(firestore.client().collection("completed").document(), f)
                        else:
                            b.set(firestore.client().collection("in_progress").document(), f)
                    b.commit()
                handle_db_write(op, f"✅ {len(bulk_items)}件を一括で{status}として登録しました。", "一括登録中にエラー", view_key=view_key)
            else:
                f_data = base_f_data.copy()
                f_data.update({
                    "記録ID": default_data.get('記録ID', datetime.now().strftime("%Y%m%d%H%M%S%f")),
                    "製品名": product_name, "詳細": fin_dtl, "作業時間_分": int(wm), "出来数": int(qty)
                })
                if status == "完了": handle_completion(f_data, view_key=view_key)
                elif is_edit_mode: handle_update(default_data.get('id'), f_data, view_key=view_key)
                else: handle_add_in_progress(f_data, view_key=view_key)
        
        if btn_sub: submit_data("作業中")
        if btn_com: submit_data("完了")

def login_screen():
    st.header("ようこそ！")
    st.subheader("はじめに、あなたの名前を選択してください。")
    cols = st.columns(4)
    for i, name in enumerate([n for n in WORKER_NAMES if n != "（自分の名前を選択してください）"]):
        if cols[i % 4].button(name, key=f"u_{name}", use_container_width=True):
            st.session_state.just_logged_in, st.session_state.logged_in_user, st.session_state.user_location = True, name, WORKER_TO_LOCATION.get(name, "すべて")
            st.rerun()

def show_bookmark_page(user_name):
    st.success(f"**{user_name}** さんとしてログインしました！")
    st.header("📌 ホーム画面への追加（重要）")
    st.markdown(f'<a href="?uid={WORKER_ID_MAP.get(user_name, "")}" target="_blank" style="display: block; text-align: center; background-color: #3b82f6; color: white; padding: 15px; text-decoration: none; border-radius: 10px; font-weight: bold; margin-bottom: 20px;">👉 1. ここをタップして【新しいタブ】で開き直す</a>', unsafe_allow_html=True)
    st.info("2. 新しい画面が開いたら、ブラウザのメニューから **「ホーム画面に追加」** を行ってください。")
    if st.button("すぐに記録を開始する", use_container_width=True):
        del st.session_state.just_logged_in
        st.rerun()

def show_daily_report():
    st.markdown("<h2 style='font-size: clamp(1.2rem, 5vw, 2rem); margin-bottom: 1rem;'>📝 日報（退勤報告）</h2>", unsafe_allow_html=True)
    user = st.session_state.logged_in_user
    c1, c2 = st.columns([6, 4])
    c1.write(f"**{user}** さん、お疲れ様です！")
    if c2.button("🔄 最新に更新", use_container_width=True): load_from_firestore.clear(); load_tasks_for_customer.clear(); st.rerun()
    
    reports_df = load_from_firestore(db, "daily_reports")
    today = datetime.now(timezone(timedelta(hours=9))).date()
    st.markdown("<h5>📅 直近1週間の状況（タップで日付移動）</h5>", unsafe_allow_html=True)
    
    in_prog_df = load_from_firestore(db, "in_progress")
    comp_df = load_from_firestore(db, "completed", days_limit=3000)
    all_df = pd.concat([in_prog_df, comp_df], ignore_index=True) if not in_prog_df.empty or not comp_df.empty else pd.DataFrame()
    if not all_df.empty and '作成日時' in all_df.columns: all_df['作成日時_dt'] = pd.to_datetime(all_df['作成日時'], utc=True).dt.tz_convert('Asia/Tokyo')
    cols = st.columns(7)
    
    for i in range(7):
        d = today - timedelta(days=6-i)
        is_sub = not reports_df.empty and '提出者' in reports_df.columns and not reports_df[(reports_df['提出者'] == user) & (reports_df['日付'] == d.strftime('%Y-%m-%d'))].empty
        has_w = False
        if not all_df.empty and '作成日時_dt' in all_df.columns:
            d_df = all_df[all_df['作成日時_dt'].dt.date == d]
            if not d_df.empty:
                has_w = any(
                    r.get('入力者名') == user or 
                    (isinstance(r.get('共同作業者'), list) and user in r.get('共同作業者')) or 
                    (isinstance(r.get('共同作業者'), str) and user in r.get('共同作業者'))
                    for _, r in d_df.iterrows()
                )
        
        stat = "✅済" if is_sub else ("📝今日" if d == today else ("⚠️未提出" if has_w else "－"))
        btn_type = "primary" if st.session_state.get('sel_d_date', today) == d else "secondary"
        with cols[i]:
            if stat == "⚠️未提出" and st.session_state.get('sel_d_date', today) != d:
                st.markdown(f"""<div style="background-color:#fee2e2; border:1px solid #ef4444; border-radius:5px; text-align:center; padding:5px 0; cursor:pointer;" onclick="document.getElementById('hdn_{i}').click();"><span style="font-size:0.8rem; color:#991b1b;">{d.month}/{d.day}</span><br><span style="font-size:0.8rem; font-weight:bold; color:#991b1b;">⚠️未提出</span></div>""", unsafe_allow_html=True)
                if st.button(" ", key=f"hdn_{i}"): st.session_state.sel_d_date = d; st.rerun()
            else:
                if st.button(f"{d.month}/{d.day}\n{stat}", key=f"db_{i}", use_container_width=True, type=btn_type): st.session_state.sel_d_date = d; st.rerun()
    st.divider()
    
    t_date = st.session_state.get('sel_d_date', today)
    t_str = t_date.strftime('%Y-%m-%d')
    st.markdown(f"#### 📅 選択中の日付: {t_date.strftime('%Y年%m月%d日')}")
    is_t_sub = False; sub_rep = {}
    if not reports_df.empty and '提出者' in reports_df.columns:
        my_t = reports_df[(reports_df['提出者'] == user) & (reports_df['日付'] == t_str)]
        if not my_t.empty: is_t_sub, sub_rep = True, my_t.iloc[0].to_dict()
    if is_t_sub:
        st.success(f"🎉 日報は提出済みです！ (出勤: {sub_rep.get('出勤時間','')} / 退勤: {sub_rep.get('退勤時間','')})")
        with st.expander("提出内容を確認"):
            st.write(f"- 機械: {sub_rep.get('機械の調子','')}\n- ヒヤリ: {sub_rep.get('ヒヤリハット','')}\n- 特記: {sub_rep.get('特記事項','')}")
    
    # ここが今回のエラー修正箇所です。NaN(float)を安全に除外して判定します。
    t_tasks = pd.DataFrame()
    if not all_df.empty and '作成日時_dt' in all_df.columns:
        d_df = all_df[all_df['作成日時_dt'].dt.date == t_date]
        if not d_df.empty: t_tasks = d_df[d_df.apply(lambda r: r.get('入力者名') == user or (isinstance(r.get('共同作業者'), list) and user in r.get('共同作業者')) or (isinstance(r.get('共同作業者'), str) and user in r.get('共同作業者')), axis=1)].sort_values('作成日時_dt')
    st.markdown(f"### 📋 作業履歴")
    if t_tasks.empty: st.info("この日の作業記録はありません。")
    else:
        for _, r in t_tasks.iterrows():
            try:
                wt_raw = r.get('作業時間_分', 0)
                wt = int(float(wt_raw)) if pd.notna(wt_raw) and str(wt_raw).strip() != "" else 0
            except:
                wt = 0
            w_str = f"{wt//60}時間{wt%60}分" if wt>0 else ""
            st.markdown(f"- `{r.get('開始時間','')}~ {w_str}` **{r.get('製品名','')}** > {r.get('工程名','')} [{r.get('使用機械','')}] ({r.get('出来数',0)}個) / {r.get('詳細','')}")
            
    with st.form("daily_rep"):
        c1, c2 = st.columns(2)
        arr_opts = ["通常出勤"] + [f"{h:02d}:{m:02d}" for h in range(5, 10) for m in (0, 15, 30, 45)]
        lev_opts = ["定時退社"] + [f"{h:02d}:{m:02d}" for h in range(17, 24) for m in (0, 15, 30, 45) if (h==17 and m>=45) or h>17]
        a_val = sub_rep.get("出勤時間", "通常出勤") if is_t_sub else "通常出勤"
        l_val = sub_rep.get("退勤時間", "定時退社") if is_t_sub else "定時退社"
        arr = c1.selectbox("出勤時間", arr_opts, index=arr_opts.index(a_val) if a_val in arr_opts else 0)
        lev = c2.selectbox("退勤時間", lev_opts, index=lev_opts.index(l_val) if l_val in lev_opts else 0)
        mac = st.radio("機械の調子", ["✨ 絶好調", "🔧 変な音がした", "⚠️ 修理が必要", "➖ 使っていない"], index=0)
        hiy = st.radio("ヒヤリハット", ["なし", "あり（特記事項に記入）"], index=0)
        note = st.text_area("特記事項", value=sub_rep.get('特記事項',''))
        
        if st.form_submit_button("日報を送信", type="primary", use_container_width=True):
            data = {"提出者": user, "日付": t_str, "作成日時": firestore.SERVER_TIMESTAMP, "出勤時間": arr, "退勤時間": lev, "機械の調子": mac, "ヒヤリハット": hiy, "特記事項": note}
            if is_t_sub: db.collection("daily_reports").document(sub_rep['id']).delete()
            db.collection("daily_reports").add(data)
            st.session_state.success_msg = f"日報を送信しました！"
            load_from_firestore.clear(); st.rerun()
            
    st.divider()
    st.markdown("### 📂 過去の日報履歴（通信量ゼロ表示）")
    with st.expander("過去30件の履歴を見る"):
        if not reports_df.empty and '提出者' in reports_df.columns:
            my_r = reports_df[reports_df['提出者'] == user].sort_values('日付', ascending=False).head(30)
            if my_r.empty: st.info("履歴がありません")
            else:
                for _, r in my_r.iterrows(): st.markdown(f"**{r.get('日付','')}** | 出勤:{r.get('出勤時間','')} 退勤:{r.get('退勤時間','')} | {r.get('機械の調子','')}\n> {r.get('特記事項','')}")

def show_admin_dashboard():
    st.markdown("<h2 style='font-size: clamp(1.2rem, 5vw, 2rem); margin-bottom: 1rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;' title='👑 管理者ダッシュボード'>👑 管理者ダッシュボード</h2>", unsafe_allow_html=True)
    
    current_user = st.session_state.get('logged_in_user', '')
    is_admin = current_user in ["岳　匠", "福田 準也"]
    
    if not st.session_state.get('admin_authenticated', False) and not is_admin:
        st.info("この画面は日報を確認する管理者専用の画面です。パスワードを入力してください。")
        password = st.text_input("パスワード", type="password")
        correct_password = st.secrets.get("ADMIN_PASSWORD", "admin1234") 
        if st.button("ログイン", type="primary"):
            if password == correct_password:
                st.session_state.admin_authenticated = True
                st.rerun()
            else:
                st.error("❌ パスワードが違います。")
        return
    if is_admin:
        st.success(f"✅ 管理者（{current_user}）としてログイン中")
    else:
        st.success("✅ 管理者としてログイン中")
        if st.button("管理者画面からログアウト"):
            st.session_state.admin_authenticated = False
            st.rerun()
        
    st.divider()
    
    with st.spinner("データベースから日報と作業記録を取得中..."):
        reports_df = load_from_firestore(db, "daily_reports")
        in_prog_df = load_from_firestore(db, "in_progress")
        comp_df = load_from_firestore(db, "completed", days_limit=3000)
        
        if not in_prog_df.empty:
            in_prog_df['_collection'] = "in_progress"
        if not comp_df.empty:
            comp_df['_collection'] = "completed"
            
        all_tasks_df = pd.concat([in_prog_df, comp_df], ignore_index=True) if not in_prog_df.empty or not comp_df.empty else pd.DataFrame()
        today_tasks_df = pd.DataFrame()
        
        if not all_tasks_df.empty and '作成日時' in all_tasks_df.columns:
            all_tasks_df['作成日時_dt'] = pd.to_datetime(all_tasks_df['作成日時'], utc=True).dt.tz_convert('Asia/Tokyo')
    admin_tab = st.radio("メニュー", ["📊 日報・作業記録の確認", "🛠️ 未照合データの一括修正"], horizontal=True, key="adm_tab")
    
    if admin_tab == "📊 日報・作業記録の確認":
        col1, col2, col3 = st.columns([1.5, 2, 1.5])
        with col1:
            target_date = st.date_input("📅 表示する日付", value=datetime.now(timezone(timedelta(hours=9))).date())
        with col2:
            default_loc = "すべて"
            if current_user == "岳　匠": default_loc = "旭川"
            elif current_user == "福田 準也": default_loc = "札幌"
            
            loc_options = ["すべて", "旭川", "札幌"]
            default_idx = loc_options.index(default_loc)
            location_filter = st.radio("🏢 表示する拠点", loc_options, index=default_idx, horizontal=True)
        with col3:
            st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
            if st.button("🔄 最新の状況に更新", key="refresh_report", use_container_width=True):
                load_from_firestore.clear()
                load_tasks_for_customer.clear()
                st.rerun()
                
        if not all_tasks_df.empty and '作成日時_dt' in all_tasks_df.columns:
            today_tasks_df = all_tasks_df[all_tasks_df['作成日時_dt'].dt.date == target_date]
            
        if location_filter == "旭川":
            target_members = ASAHIKAWA_MEMBERS
        elif location_filter == "札幌":
            target_members = SAPPORO_MEMBERS
        else:
            target_members = WORKER_NAMES
            
        target_date_str = target_date.strftime('%Y-%m-%d')
        
        filtered_df = pd.DataFrame()
        if not reports_df.empty:
            filtered_df = reports_df[reports_df['日付'] == target_date_str].copy()
            if not filtered_df.empty:
                filtered_df['拠点'] = filtered_df['提出者'].map(WORKER_TO_LOCATION).fillna("未設定")
                if location_filter != "すべて":
                    filtered_df = filtered_df[filtered_df['拠点'] == location_filter]
        worked_members = set()
        if not today_tasks_df.empty:
            for _, row in today_tasks_df.iterrows():
                worker = row.get('入力者名')
                if pd.notna(worker) and worker in target_members:
                    worked_members.add(worker)
                
                co_workers = row.get('共同作業者', [])
                if isinstance(co_workers, list):
                    for cw in co_workers:
                        if cw in target_members: worked_members.add(cw)
                elif isinstance(co_workers, str) and co_workers:
                    for cw in [w.strip() for w in co_workers.split(',')]:
                        if cw in target_members: worked_members.add(cw)
        submitted_members = filtered_df['提出者'].tolist() if not filtered_df.empty else []
        missing_members = sorted(list(worked_members - set(submitted_members)))
        
        st.markdown(f"<h3 style='font-size: clamp(1rem, 4vw, 1.4rem);'>🚨 未提出者 ({len(missing_members)}名)</h3>", unsafe_allow_html=True)
        if missing_members:
            st.error("、 ".join(missing_members))
            st.caption("※今日システムに作業記録があるにも関わらず、日報が未提出の方です。（休みの人は表示されません）")
        else:
            if worked_members:
                st.success("今日作業記録がある方は全員提出済みです！素晴らしい！🎉")
            else:
                st.info("この日の作業記録はまだありません。")
            
        st.divider()
            
        st.markdown(f"<h3 style='font-size: clamp(1rem, 4vw, 1.4rem);'>📊 提出済み日報 ({len(submitted_members)}件)</h3>", unsafe_allow_html=True)
        
        if filtered_df.empty:
            st.info(f"{location_filter}拠点の {target_date_str} の日報はまだ提出されていません。")
        else:
            display_cols = ['提出者', '拠点', '出勤時間', '退勤時間', '機械の調子', 'ヒヤリハット']
            existing_cols = [c for c in display_cols if c in filtered_df.columns]
            st.dataframe(filtered_df[existing_cols], use_container_width=True)
            
            export_cols = ['日付', '提出者', '拠点', '出勤時間', '退勤時間', '機械の調子', 'ヒヤリハット', '漏れている作業', '特記事項']
            export_existing_cols = [c for c in export_cols if c in filtered_df.columns]
            csv_data = filtered_df[export_existing_cols].to_csv(index=False).encode('utf-8-sig')
            
            st.download_button(
                label="📥 表示中の日報をCSV（エクセル用）でダウンロード",
                data=csv_data,
                file_name=f"日報一覧_{target_date_str}_{location_filter}.csv",
                mime="text/csv",
                type="primary",
                use_container_width=True
            )
            
            st.divider()
            st.subheader("📝 詳細な報告内容（タップで展開）")
            
            for idx, row in filtered_df.iterrows():
                worker = row.get('提出者', '不明')
                loc = row.get('拠点', '')
                arrive_time = row.get('出勤時間', '早出なし')
                leave_time = row.get('退勤時間', '残業なし')
                
                arrive_time = "早出なし" if pd.isna(arrive_time) else str(arrive_time)
                leave_time = "残業なし" if pd.isna(leave_time) else str(leave_time)
                
                arr_disp = arrive_time if "なし" not in arrive_time else "通常"
                lev_disp = leave_time if "なし" not in leave_time else "定時"
                
                with st.expander(f"👤 {worker} ({loc}) - 出勤: {arr_disp} / 退勤: {lev_disp}"):
                    worker_tasks = pd.DataFrame()
                    if not today_tasks_df.empty:
                        def is_worker_involved(task_row):
                            if task_row.get('入力者名') == worker: return True
                            cw = task_row.get('共同作業者', [])
                            if isinstance(cw, list) and worker in cw: return True
                            if isinstance(cw, str) and worker in cw: return True
                            return False
                        
                        involved_mask = today_tasks_df.apply(is_worker_involved, axis=1)
                        worker_tasks = today_tasks_df[involved_mask].sort_values('作成日時_dt')
                    st.markdown("##### 📋 今日の作業内容")
                    if worker_tasks.empty:
                        st.write("システムの作業記録はありません。")
                    else:
                        for _, t_row in worker_tasks.iterrows():
                            product = t_row.get('製品名', '名称不明')
                            process = t_row.get('工程名', '工程不明')
                            detail = t_row.get('詳細', '')
                            qty = int(t_row.get('出来数', 0))
                            machine = t_row.get('使用機械', '')
                            is_helper = t_row.get('入力者名') != worker
                            
                            qty_str = f"{qty:,}個"
                            setup_badge = " 🔧セットのみ" if qty == 0 else ""
                            machine_str = f"[{machine}]" if machine else ""
                            
                            if is_helper:
                                input_user = t_row.get('入力者名', '不明')
                                helper_badge = f"👤補助 (機長:{input_user})"
                            else:
                                helper_badge = "👑機長"
                                
                            start_t = t_row.get('開始時間', '')
                            try:
                                work_m_raw = t_row.get('作業時間_分', 0)
                                work_m = int(float(work_m_raw)) if pd.notna(work_m_raw) and str(work_m_raw).strip() != "" else 0
                            except:
                                work_m = 0
                            
                            if work_m > 0:
                                h = work_m // 60
                                m = work_m % 60
                                wt_str = f"{h}時間{m}分" if h > 0 and m > 0 else (f"{h}時間" if h > 0 else f"{m}分")
                                time_str = f"{start_t}開始 ({wt_str})" if start_t else f"計{wt_str}"
                            else:
                                time_str = f"{start_t}開始" if start_t else "時間記録なし"
                            
                            st.markdown(f"- `{time_str}` `{helper_badge}` **{product}** ＞ {process} {machine_str}{setup_badge} ({qty_str}) / 詳細: {detail}")
                    st.divider()
                    st.markdown(f"**🔧 機械の調子:** {row.get('機械の調子', '未記入')}")
                    
                    hiyari = row.get('ヒヤリハット', '未記入')
                    if "あり" in hiyari:
                        st.markdown(f"**⚠️ ヒヤリハット:** <span style='color:red;'>{hiyari}</span>", unsafe_allow_html=True)
                    else:
                        st.markdown(f"**⚠️ ヒヤリハット:** {hiyari}")
                    
                    missing = row.get('漏れている作業', '')
                    if missing:
                        st.markdown(f"**✍️ 追加申告作業:**\n> {missing}")
                        
                    note = row.get('特記事項', '')
                    if note:
                        st.markdown(f"**💡 特記事項:**\n> {note}")
                    else:
                        st.markdown("**💡 特記事項:** なし")
                        
                    photo = row.get('写真データ', '')
                    if photo and isinstance(photo, str) and photo.startswith('data:image'):
                        st.image(photo, caption=f"{worker}さんからの添付写真", use_container_width=True)
                        
    elif admin_tab == "🛠️ 未照合データの一括修正":
        st.markdown("現場が「仮の名前」で入力した過去の作業記録を、予定表の「正式な名前」に一括で書き換えます。")
        
        st.markdown("##### Step 1: 検索条件と予定表データの設定")
        col_date1, col_date2 = st.columns(2)
        with col_date1:
            fix_start_date = st.date_input("検索開始日", value=datetime.now(timezone(timedelta(hours=9))).date() - timedelta(days=7), key="fix_start")
        with col_date2:
            fix_end_date = st.date_input("検索終了日", value=datetime.now(timezone(timedelta(hours=9))).date(), key="fix_end")
            
        st.info("💡 過去の品名に紐付ける場合は、当時の予定表(CSV)をここにアップロードしてください。現場の入力画面には影響しません。")
        uploaded_past_csv = st.file_uploader("過去の予定表 (schedule.csv) ※任意", type=['csv'], key="past_schedule_upload")
        
        target_tasks_df = pd.DataFrame()
        if not all_tasks_df.empty and '作成日時_dt' in all_tasks_df.columns:
            mask = (all_tasks_df['作成日時_dt'].dt.date >= fix_start_date) & (all_tasks_df['作成日時_dt'].dt.date <= fix_end_date)
            target_tasks_df = all_tasks_df[mask].copy()
        existing_products = []
        if not target_tasks_df.empty and '製品名' in target_tasks_df.columns:
            existing_products = sorted(target_tasks_df['製品名'].dropna().astype(str).unique().tolist())
            
        if uploaded_past_csv is not None:
            try:
                schedule_df_for_fix = pd.read_csv(uploaded_past_csv, encoding="utf-8-sig")
                st.success("専用の過去予定表を読み込みました！")
            except Exception as e:
                st.error(f"CSVの読み込みに失敗しました: {e}")
                schedule_df_for_fix = pd.DataFrame()
        else:
            schedule_df_for_fix = load_csv_data(SCHEDULE_FILE)
        official_products = []
        if not schedule_df_for_fix.empty and '品名' in schedule_df_for_fix.columns:
            official_products = sorted(schedule_df_for_fix['品名'].dropna().astype(str).unique().tolist())
            
        unmatched_products = [p for p in existing_products if p not in official_products]
        
        st.divider()
        st.markdown("##### Step 2: 修正対象の選択と詳細確認")
        
        col_from, col_to = st.columns(2)
        with col_from:
            st.write("**変更したい（間違っている）品名**")
            if unmatched_products:
                source_options = [""] + unmatched_products
            else:
                source_options = ["（指定期間内の未照合はありません）"]
            
            source_product = st.selectbox("Firebaseに登録されている品名 (未照合のみ)", source_options)
            
        with col_to:
            st.write("**変更後の（正しい）品名**")
            target_product = st.selectbox("予定表(CSV)の品名", [""] + official_products)
            manual_target = st.text_input("または、手動で正しい品名を入力", help="プルダウンに無い場合はこちらに入力してください")
            
        final_target = manual_target if manual_target else target_product
        
        if source_product and not source_product.startswith("（"):
            st.markdown(f"**🔍 「{source_product}」の作業履歴（推測の手がかり）**")
            details_df = target_tasks_df[target_tasks_df['製品名'] == source_product].sort_values('作成日時_dt')
            
            for _, r in details_df.iterrows():
                work_date = r['作成日時_dt'].strftime('%Y/%m/%d %H:%M')
                worker = r.get('入力者名', '不明')
                process = r.get('工程名', '')
                detail = r.get('詳細', '')
                qty = r.get('出来数', 0)
                st.caption(f"・ {work_date} | 👤 {worker} | 🔧 {process} ({detail}) | 📦 {qty}個")
        st.divider()
        
        st.markdown("##### Step 3: 一括書き換えの実行")
        if st.button("この品名を一括で書き換える", type="primary"):
            if not source_product or source_product.startswith("（"):
                st.error("変更元の品名を正しく選択してください。")
            elif not final_target:
                st.error("変更先の品名を入力または選択してください。")
            elif source_product == final_target:
                st.error("変更元と変更先が同じです。")
            else:
                with st.spinner(f"「{source_product}」を「{final_target}」に変更中..."):
                    try:
                        target_rows = all_tasks_df[all_tasks_df['製品名'] == source_product]
                        if target_rows.empty:
                            st.warning("該当する品名のデータが見つかりませんでした。")
                        else:
                            db_batch = firestore.client()
                            batch = db_batch.batch()
                            update_count = 0
                            
                            for _, row in target_rows.iterrows():
                                doc_id = row.get('id')
                                col_name = row.get('_collection')
                                if col_name and doc_id:
                                    doc_ref = db_batch.collection(col_name).document(doc_id)
                                    batch.update(doc_ref, {"製品名": final_target})
                                    update_count += 1
                                    
                            if update_count > 0:
                                batch.commit()
                                st.session_state.success_msg = f"✅ {update_count}件の作業記録を「{final_target}」に書き換えました！"
                                load_from_firestore.clear()
                                load_tasks_for_customer.clear()
                                st.rerun()
                    except Exception as e:
                        st.error(f"更新中にエラーが発生しました: {e}")
def render_step1(schedule_df, display_df, selected_location, product_to_location):
    st.markdown(f"<h3>Step 1: 新規工程を記録（{selected_location}）</h3>", unsafe_allow_html=True)
    f_sch = schedule_df[schedule_df['拠点'] == selected_location] if selected_location != "すべて" and not schedule_df.empty and '拠点' in schedule_df.columns else schedule_df.copy()
    c_names = sorted(f_sch['得意先名'].dropna().unique().tolist()) if not f_sch.empty and '得意先名' in f_sch.columns else []
    
    # 選択する得意先の初期値を決定
    default_customer = "すべての得意先"
    preselected_product = st.session_state.get('product_to_select', "")
    if preselected_product and not f_sch.empty and '品名' in f_sch.columns:
        match = f_sch[f_sch['品名'] == preselected_product]
        if not match.empty:
            customer = match.iloc[0].get('得意先名')
            if pd.notna(customer) and customer in c_names:
                default_customer = customer

    sel_c = st.selectbox("得意先名で絞り込み", ["すべての得意先"] + c_names, index=(["すべての得意先"] + c_names).index(default_customer))
    with st.form("selection_form"):
        p_df = f_sch[f_sch['得意先名'] == sel_c] if sel_c != "すべての得意先" else f_sch.copy()
        s_prods = p_df['品名'].dropna().unique().tolist() if not p_df.empty and '品名' in p_df.columns else []
        i_prods = display_df['製品名'].unique().tolist() if not display_df.empty and '製品名' in display_df.columns else []
        opts = [""] + sorted(list(set(s_prods + i_prods)))
        
        # product_to_select があれば初期値にセット
        default_index = opts.index(preselected_product) if preselected_product in opts else 0
        sel_p = st.selectbox("製品を選択", opts, index=default_index)
        
        man_in = st.checkbox("リストにない製品を手入力")
        man_p = st.text_input("新しい製品名")
        sel_proc = st.selectbox("工程名", PROCESS_OPTIONS)
        
        if st.form_submit_button("入力を開始する", type="primary"):
            fin_p = man_p if man_in and man_p else sel_p
            if not fin_p or not sel_proc: st.error("製品と工程を選択してください")
            else:
                st.session_state.selected_product = fin_p
                st.session_state.selected_process = sel_proc
                st.session_state.sub_view = 'INPUT_FORM'
                # 処理が終わったらクリア
                if 'product_to_select' in st.session_state: del st.session_state.product_to_select
                st.rerun()

def main_app():
    # 修正: メニュー切り替え時などに product_to_select を消さないようにここでの clear は削除
    # if 'product_to_select' in st.session_state: del st.session_state.product_to_select
    if 'success_msg' in st.session_state: st.success(st.session_state.pop('success_msg'))
    
    st.sidebar.success(f"ログイン: **{st.session_state.logged_in_user}**")
    if st.sidebar.button("ログアウト"): st.session_state.clear(); st.rerun()
    st.sidebar.button("データ更新", on_click=lambda: (load_from_firestore.clear(), load_tasks_for_customer.clear()), use_container_width=True)
    
    with st.sidebar.expander("🛠️ 管理者メニュー"):
        st.markdown("**■ 予定表の手動アップロード**")
        st.info("朝の自動更新が失敗した際のフェイルセーフです。")
        uploaded_file = st.file_uploader("予定表 (schedule.csv)", type=['csv'])
        uploaded_m_file = st.file_uploader("明細 (schedule_m.csv) ※カレンダー用", type=['csv'])
        
        if st.button("CSVを適用する", use_container_width=True):
            success_count = 0
            if uploaded_file is not None:
                try:
                    st.session_state.manual_schedule_df = pd.read_csv(uploaded_file, encoding="utf-8-sig")
                    success_count += 1
                except Exception as e:
                    st.error(f"予定表読み込みエラー: {e}")
            if uploaded_m_file is not None:
                try:
                    st.session_state.manual_schedule_m_df = pd.read_csv(uploaded_m_file, encoding="utf-8-sig")
                    success_count += 1
                except Exception as e:
                    st.error(f"明細読み込みエラー: {e}")
                    
            if success_count > 0:
                st.success(f"✅ {success_count}個のファイルを適用しました！")
                load_csv_data.clear()
                st.rerun()
        st.divider()
        st.markdown("**■ 日報データの抽出 (CSV)**")
        dl_start = st.date_input("開始日", value=datetime.now(timezone(timedelta(hours=9))).date())
        dl_end = st.date_input("終了日", value=datetime.now(timezone(timedelta(hours=9))).date())
        
        r_df = load_from_firestore(db, "daily_reports")
        if not r_df.empty and '日付' in r_df.columns:
            mask = (r_df['日付'] >= dl_start.strftime('%Y-%m-%d')) & (r_df['日付'] <= dl_end.strftime('%Y-%m-%d'))
            filtered_reports = r_df[mask].copy()
            
            if not filtered_reports.empty:
                if '提出者' in filtered_reports.columns:
                    filtered_reports['拠点'] = filtered_reports['提出者'].map(WORKER_TO_LOCATION).fillna('未設定')
                if '写真データ' in filtered_reports.columns:
                    filtered_reports['写真添付'] = filtered_reports['写真データ'].apply(lambda x: "あり" if str(x).startswith("data:image") else "なし")
                    filtered_reports = filtered_reports.drop(columns=['写真データ'])
                
                cols_order = ['日付', '拠点', '提出者', '出勤時間', '退勤時間', '機械の調子', 'ヒヤリハット', '漏れている作業', '特記事項', '関連タスク数', '写真添付', '作成日時']
                final_cols = [c for c in cols_order if c in filtered_reports.columns] + [c for c in filtered_reports.columns if c not in cols_order]
                filtered_reports = filtered_reports[final_cols]
                filtered_reports = filtered_reports.sort_values(by=['日付', '拠点', '提出者'])
                
                csv_data = filtered_reports.to_csv(index=False).encode('utf-8-sig')
                
                st.download_button(
                    label="📥 CSVダウンロード",
                    data=csv_data,
                    file_name=f"日報データ_{dl_start.strftime('%Y%m%d')}-{dl_end.strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    type="primary",
                    use_container_width=True
                )
            else:
                st.caption("指定された期間の日報はありません。")
        else:
            st.caption("日報データがまだ登録されていません。")
            
    main_view = st.radio("メニュー", ["🔧 通常工程の記録", "📅 カレンダー一括管理", "📦 名入れ一括登録", "📝 日報（退勤報告）", "👑 管理者画面"], horizontal=True, label_visibility="collapsed")
    st.divider()
    
    if main_view == "🔧 通常工程の記録":
        in_progress_df = load_from_firestore(db, "in_progress")
        st.session_state.in_progress_df = in_progress_df
        
        sub_view = st.session_state.get('sub_view', 'SELECT_PROCESS')
        
        if sub_view == 'INPUT_FORM': 
            process_form(is_edit_mode=False, default_data=st.session_state.get('record_to_copy'))
        elif sub_view == 'EDIT_FORM': 
            process_form(is_edit_mode=True, default_data=st.session_state.get('record_to_edit'))
        else:
            st.session_state.sub_view = 'SELECT_PROCESS'
            
            schedule_df = load_csv_data(SCHEDULE_FILE)
            loc_opts = ["すべて", "旭川", "札幌"]
            p2l = {}
            if not schedule_df.empty and SCHEDULE_COL_LOCATION_CODE in schedule_df.columns and '品名' in schedule_df.columns:
                schedule_df['拠点'] = pd.to_numeric(schedule_df[SCHEDULE_COL_LOCATION_CODE], errors='coerce').map({1: "旭川", 2: "札幌"}).fillna('未設定')
                schedule_df['clean_品名'] = schedule_df['品名'].apply(clean_text)
                p2l = schedule_df.drop_duplicates(subset=['clean_品名']).set_index('clean_品名')['拠点'].to_dict()
            sel_loc = st.selectbox("拠点", loc_opts, index=loc_opts.index(st.session_state.get("user_location", "すべて")) if st.session_state.get("user_location", "すべて") in loc_opts else 0)
            d_df = in_progress_df.copy()
            if not d_df.empty:
                if 'is_calendar' in d_df.columns:
                    d_df = d_df[d_df['is_calendar'] != True]
                if "製品名" in d_df.columns:
                    if '拠点' not in d_df.columns: d_df['拠点'] = '未設定'
                    if sel_loc != "すべて": d_df = d_df[d_df['拠点'] == sel_loc]
            c_f, c_l = st.columns(2)
            with c_f: 
                render_step1(schedule_df, d_df, sel_loc, p2l)
            with c_l:
                st.markdown("<h3>進行中一覧</h3>", unsafe_allow_html=True)
                if d_df.empty: 
                    st.info("作業中の製品はありません。")
                else:
                    schedule_lookup = {}
                    if not schedule_df.empty and '品名' in schedule_df.columns:
                        schedule_df['clean_品名_lookup'] = schedule_df['品名'].apply(clean_text)
                        for _, row in schedule_df.iterrows():
                            schedule_lookup[row['clean_品名_lookup']] = row.get(SCHEDULE_COL_DUE_DATE, "")

                    for p, g in d_df.groupby('製品名'):
                        due_date = schedule_lookup.get(clean_text(p), "")
                        due_badge = f" 📅 納期:{due_date}" if pd.notna(due_date) and str(due_date).strip() != "" else ""

                        with st.expander(f"**{p}**{due_badge}"):
                            c1, c2 = st.columns(2)
                            if c1.button("工程追加", key=f"a_{p}"): st.session_state.product_to_select, st.session_state.scroll_to_top = p, True; st.rerun()
                            if c2.button("完了", key=f"c_{p}", type="primary"): handle_product_completion(p)
                            for _, r in g.iterrows():
                                work_date_str = ""
                                if '作成日時' in r and pd.notna(r['作成日時']):
                                    try:
                                        dt = pd.to_datetime(r['作成日時']).tz_convert('Asia/Tokyo')
                                        work_date_str = dt.strftime('%m/%d')
                                    except:
                                        pass
                                start_t = r.get('開始時間', '')
                                time_badge = f"🕒 {work_date_str} {start_t}~" if start_t else f"🕒 {work_date_str}"

                                st.caption(f"{time_badge} | {r['工程名']} / 出来数: {r['出来数']}個 / 入力: {r.get('入力者名','')}")
                                
                                # 修正箇所: ここでループ内にCSSを埋め込むのをやめ、シンプルにクラスだけ適用します。
                                st.markdown('<div class="button-container-row">', unsafe_allow_html=True)
                                cx, cy, cz = st.columns([1, 1, 1])
                                if cx.button("編集", key=f"e_{r['id']}", use_container_width=True): st.session_state.record_to_edit, st.session_state.sub_view = r.to_dict(), 'EDIT_FORM'; st.rerun()
                                if cy.button("続き", key=f"cp_{r['id']}", use_container_width=True): 
                                    d = r.to_dict(); d['開始時間'] = d['終了時間'] = ""; d['出来数'] = 0; d.pop('id', None)
                                    st.session_state.record_to_copy, st.session_state.sub_view = d, 'INPUT_FORM'; st.rerun()
                                if cz.button("削除", key=f"d_{r['id']}", use_container_width=True): db.collection("in_progress").document(r['id']).delete(); load_from_firestore.clear(); st.rerun()
                                st.markdown('</div>', unsafe_allow_html=True)
                                
                                st.divider()
    elif main_view == "📅 カレンダー一括管理":
        in_progress_df = load_from_firestore(db, "in_progress")
        st.session_state.in_progress_df = in_progress_df
        cal_sub_view = st.session_state.get('cal_sub_view', 'SELECT')
        
        if cal_sub_view == 'INPUT_FORM':
            process_form(default_data=st.session_state.get('cal_record_to_copy'), view_key='cal_sub_view', is_calendar=True)
        elif cal_sub_view == 'EDIT_FORM':
            process_form(is_edit_mode=True, default_data=st.session_state.get('cal_record_to_edit'), view_key='cal_sub_view', is_calendar=True)
        elif cal_sub_view == 'INPUT_FORM_BULK':
            process_form(default_data=st.session_state.get('cal_record_to_copy'), view_key='cal_sub_view', is_calendar=True, bulk_items=st.session_state.get('cal_bulk_items'))
        else:
            st.session_state.cal_sub_view = 'SELECT'
            st.header("カレンダー一括管理")
            sch = load_csv_data(SCHEDULE_FILE)
            sch_m = load_csv_data(SCHEDULE_M_FILE)
            
            c_left, c_right = st.columns([1.3, 1])
            with c_left:
                if sch.empty or sch_m.empty: 
                    st.warning("予定表CSV または schedule_m.csv が読み込めません。（サイドバーからアップロードしてください）")
                else:
                    cal_sch = sch[sch[SCHEDULE_COL_DETAILS].astype(str).str.contains('カレンダー', na=False)] if SCHEDULE_COL_DETAILS in sch.columns else pd.DataFrame()
                    if cal_sch.empty: 
                        st.info("予定表に「カレンダー」指定の案件がありません。")
                    else:
                        c_names = sorted(cal_sch['得意先名'].dropna().unique().tolist()) if '得意先名' in cal_sch.columns else []
                        sel_c = st.selectbox("得意先名で絞り込み", ["すべての得意先"] + c_names, key="cal_customer_sel")
                        
                        f_cal_sch = cal_sch[cal_sch['得意先名'] == sel_c] if sel_c != "すべての得意先" else cal_sch
                        p_prod = st.selectbox("カレンダーの品名を選択", [""] + sorted(f_cal_sch['品名'].dropna().unique().tolist()))
                        
                        if p_prod:
                            parent_row = cal_sch[cal_sch['品名']==p_prod].iloc[0]
                            denpyo_col = next((col for col in sch.columns if '伝票' in col), None)
                            denpyo_m_col = next((col for col in sch_m.columns if '伝票' in col), None)
                            
                            t_m = pd.DataFrame()
                            if denpyo_col and denpyo_m_col:
                                denpyo_val = parent_row.get(denpyo_col)
                                if pd.notna(denpyo_val):
                                    t_m = sch_m[sch_m[denpyo_m_col] == denpyo_val]
                            else:
                                c_code = parent_row.get('得意先コード')
                                if pd.notna(c_code) and '得意先コード' in sch_m.columns:
                                    t_m = sch_m[sch_m['得意先コード'] == c_code]
                            
                            exclude_words = ["区分け", "包代", "包装", "パレット", "箱代", "PUR", "送料", "運賃", "ダンボール", "段ボール", "値引き", "手数料"]
                            target_items = {}
                            if not t_m.empty:
                                for _, row in t_m.iterrows():
                                    content_val = str(row.get('内容', '')).strip()
                                    if content_val == 'nan' or not content_val:
                                        content_val = str(row.get('納品書明細', '')).strip()
                                    if content_val == 'nan' or not content_val:
                                        continue 
                                    is_excluded = any(word in content_val for word in exclude_words)
                                    
                                    if not is_excluded:
                                        qty_val = row.get('数量', 0)
                                        try:
                                            qty = int(float(qty_val)) if pd.notna(qty_val) else 0
                                        except:
                                            qty = 0
                                        if content_val in target_items:
                                            target_items[content_val]['数量'] += qty
                                        else:
                                            target_items[content_val] = {'会社名': content_val, '数量': qty}
                            
                            if not target_items:
                                st.markdown("### 🔘 単体で登録（名入れがない場合）")
                                st.info("このカレンダーには名入れが見つかりません。単体として登録します。")
                                with st.form("single_cal_form"):
                                    c_proc, c_btn = st.columns([2, 1])
                                    sel_single = c_proc.selectbox("工程", CALENDAR_PROCESS_OPTIONS, key="single_proc_top")
                                    if c_btn.form_submit_button("入力を開始する", type="primary", use_container_width=True):
                                        if not sel_single: st.error("工程を選択してください。")
                                        else:
                                            qty = parent_row.get(SCHEDULE_COL_TOTAL_QUANTITY, 0)
                                            st.session_state.cal_record_to_copy = {
                                                '製品名': p_prod, '工程名': sel_single, '詳細': "", '出来数': int(qty) if pd.notna(qty) else 0
                                            }
                                            st.session_state.cal_sub_view = 'INPUT_FORM'
                                            st.rerun()
                            else:
                                st.markdown("### 📑 複数名入れの一括登録")
                                st.success(f"{len(target_items)}件の名入れ先（費用項目を除外済）が見つかりました。")
                                
                                st.write("対象会社リスト（チェックして一括処理）")
                                checked_comps = []
                                is_single_item = (len(target_items) == 1)
                                for comp, info in target_items.items():
                                    qty_str = f" （{info['数量']:,}部）" if info['数量'] > 0 else ""
                                    if st.checkbox(f"{comp}{qty_str}", key=f"cal_chk_{comp}", value=is_single_item):
                                        checked_comps.append(info)
                                
                                sel_proc = st.selectbox("一括登録する工程", CALENDAR_PROCESS_OPTIONS)
                                c1, c2 = st.columns(2)
                                if c1.button("一括入力を開始する", type="primary"):
                                    if not checked_comps or not sel_proc: st.error("会社と工程を選択してください。")
                                    else:
                                        st.session_state.cal_record_to_copy = {'製品名': p_prod, '工程名': sel_proc}
                                        st.session_state.cal_bulk_items = [{'詳細': item['会社名'], '出来数': item['数量']} for item in checked_comps]
                                        st.session_state.cal_sub_view = 'INPUT_FORM_BULK'
                                        st.rerun()
                                        
                                st.divider()
                                st.write("💡 【手動追加】リストに無い宛先を1件だけ追加する")
                                with st.form("manual_add_form"):
                                    m_comp = st.text_input("追加する名入れ会社名")
                                    m_qty = st.number_input("部数", min_value=0, step=1, value=0)
                                    m_proc = st.selectbox("工程", CALENDAR_PROCESS_OPTIONS)
                                    if st.form_submit_button("この1件の入力を開始", type="secondary"):
                                        if not m_proc: st.error("工程を選択してください。")
                                        else:
                                            st.session_state.cal_record_to_copy = {
                                                '製品名': p_prod, '工程名': m_proc, '詳細': m_comp.strip(), '出来数': m_qty
                                            }
                                            st.session_state.cal_sub_view = 'INPUT_FORM'
                                            st.rerun()
            with c_right:
                st.markdown("<h3>カレンダー進行中一覧</h3>", unsafe_allow_html=True)
                cal_d_df = in_progress_df.copy()
                if not cal_d_df.empty and 'is_calendar' in cal_d_df.columns:
                    cal_d_df = cal_d_df[cal_d_df['is_calendar'] == True]
                else:
                    cal_d_df = pd.DataFrame() 
                    
                if cal_d_df.empty: 
                    st.info("作業中のカレンダーはありません。")
                else:
                    for p, g in cal_d_df.groupby('製品名'):
                        with st.expander(f"**{p}**", expanded=True):
                            c_btn = st.button("親ごと完了", key=f"c_cal_{p}", type="primary")
                            if c_btn: handle_product_completion(p, view_key='cal_sub_view')
                            for _, r in g.iterrows():
                                dtls = r.get('詳細', '')
                                dtl_str = f" - {dtls}" if dtls else ""
                                st.caption(f"{r['工程名']}{dtl_str} / 出来数: {r['出来数']}個 / 入力: {r.get('入力者名','')}")
                                
                                # 修正箇所: ここも同様にシンプルにクラスだけ適用します。
                                st.markdown('<div class="button-container-row">', unsafe_allow_html=True)
                                cx, cy, cz = st.columns(3)
                                if cx.button("編集", key=f"e_cal_{r['id']}", use_container_width=True): st.session_state.cal_record_to_edit, st.session_state.cal_sub_view = r.to_dict(), 'EDIT_FORM'; st.rerun()
                                if cy.button("続き", key=f"cp_cal_{r['id']}", use_container_width=True): 
                                    d = r.to_dict(); d['開始時間'] = d['終了時間'] = ""; d['出来数'] = 0; d.pop('id', None)
                                    st.session_state.cal_record_to_copy, st.session_state.cal_sub_view = d, 'INPUT_FORM'; st.rerun()
                                if cz.button("削除", key=f"d_cal_{r['id']}", use_container_width=True): db.collection("in_progress").document(r['id']).delete(); load_from_firestore.clear(); st.rerun()
                                st.markdown('</div>', unsafe_allow_html=True)
                                st.divider()
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
                                    doc_data['完了日時'] = firestore.SERVER_TIMESTAMP
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
                                        doc_data['完了日時'] = firestore.SERVER_TIMESTAMP
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
    elif main_view == "👑 管理者画面":
        show_admin_dashboard()

st.markdown("<h1>📘 製本記録アプリ</h1>", unsafe_allow_html=True)

# 修正箇所: window.parent.scrollTo を使用して、スマホの画面全体を確実に一番上までスムーズにスクロールさせます。
if st.session_state.get('scroll_to_top'):
    components.html("<script>window.parent.scrollTo({top: 0, behavior: 'smooth'});</script>", height=0, width=0)
    st.session_state.scroll_to_top = False

db = init_firebase()
if not db: st.stop()
if 'logged_in_user' not in st.session_state:
    if hasattr(st, 'query_params') and st.query_params.get("uid") in ID_TO_WORKER:
        st.session_state.logged_in_user = ID_TO_WORKER[st.query_params.get("uid")]
        st.session_state.user_location = WORKER_TO_LOCATION.get(st.session_state.logged_in_user, "すべて")
if 'logged_in_user' in st.session_state:
    if st.session_state.get("just_logged_in"): show_bookmark_page(st.session_state.logged_in_user)
    else: main_app()
else:
    login_screen()
