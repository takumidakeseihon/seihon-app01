import streamlit as st
import pandas as pd
from datetime import datetime, time, timedelta, timezone
import unicodedata
import streamlit.components.v1 as components
import firebase_admin
from firebase_admin import credentials, firestore
import json
import os
import re
import base64
import io
from PIL import Image
import urllib.request
from urllib.error import URLError, HTTPError

st.set_page_config(page_title="製本記録アプリ", layout="wide")

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

def convert_gdrive_url(url):
    """Googleドライブの閲覧用URLを直接ダウンロード用URLに変換する"""
    if url and "drive.google.com" in url:
        match = re.search(r'/d/([a-zA-Z0-9_-]+)', url)
        if match:
            file_id = match.group(1)
            return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url

@st.cache_resource
def init_firebase():
    if not firebase_admin._apps:
        try:
            firebase_secrets = st.secrets["FIREBASE_KEY_JSON"]
            cred_dict = json.loads(firebase_secrets)
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
        except Exception as e:
            st.error(f"Firebaseの初期化に失敗しました: {e}")
    return firestore.client()

db = init_firebase()

@st.cache_data(ttl=300)
def load_schedule():
    local_path = "schedule.csv"
    try:
        if "SCHEDULE_CSV_URL" in st.secrets:
            raw_url = st.secrets["SCHEDULE_CSV_URL"]
            url = convert_gdrive_url(raw_url)
            return pd.read_csv(url, encoding='cp932', encoding_errors='replace')
        elif os.path.exists(local_path):
            return pd.read_csv(local_path, encoding='cp932', encoding_errors='replace')
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"予定表(schedule.csv)の読み込みに失敗しました: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_schedule_m():
    local_path = "schedule_m.csv"
    error_msg = ""
    try:
        # 手動アップロードされたファイルがあれば優先
        if 'uploaded_sch_m' in st.session_state and st.session_state.uploaded_sch_m is not None:
            return pd.read_csv(st.session_state.uploaded_sch_m, encoding='cp932', encoding_errors='replace'), ""
            
        if "SCHEDULE_M_CSV_URL" in st.secrets:
            raw_url = st.secrets["SCHEDULE_M_CSV_URL"]
            url = convert_gdrive_url(raw_url)
            try:
                # HTTPリクエストヘッダーを追加してアクセス拒否を回避
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req) as response:
                    return pd.read_csv(response, encoding='cp932', encoding_errors='replace'), ""
            except HTTPError as e:
                error_msg = f"URLからの取得に失敗しました (HTTP {e.code}: 共有設定を確認してください): {url}"
            except URLError as e:
                error_msg = f"URLにアクセスできません ({e.reason}): {url}"
            except Exception as e:
                error_msg = f"URLからのCSV読み込みエラー: {e}"
                
        if os.path.exists(local_path):
            try:
                return pd.read_csv(local_path, encoding='cp932', encoding_errors='replace'), ""
            except Exception as e:
                error_msg = f"ローカルファイルのCSV読み込みエラー: {e}"
        else:
            error_msg = f"ローカルにファイルが見つかりません: {os.path.abspath(local_path)} （ファイル名が合っているか確認してください）"
            
        return pd.DataFrame(), error_msg
    except Exception as e:
        return pd.DataFrame(), f"予期せぬエラーが発生しました: {e}"

@st.cache_data(ttl=60)
def load_from_firestore(target_date):
    start_dt = datetime.combine(target_date, time.min).replace(tzinfo=timezone(timedelta(hours=9)))
    end_dt = start_dt + timedelta(days=1)
    
    collections = ['tasks', 'tasks_s']
    all_data = []
    
    for col in collections:
        docs = db.collection(col).where('日時', '>=', start_dt).where('日時', '<', end_dt).stream()
        for doc in docs:
            d = doc.to_dict()
            d['id'] = doc.id
            d['_collection'] = col
            all_data.append(d)
            
    if all_data:
        df = pd.DataFrame(all_data)
        # JSTの時刻に変換して文字列化
        df['日時'] = pd.to_datetime(df['日時']).dt.tz_convert('Asia/Tokyo').dt.strftime('%H:%M')
        return df
    return pd.DataFrame()

@st.cache_data(ttl=60)
def load_all_tasks_for_period(start_date, end_date):
    start_dt = datetime.combine(start_date, time.min).replace(tzinfo=timezone(timedelta(hours=9)))
    end_dt = datetime.combine(end_date, time.max).replace(tzinfo=timezone(timedelta(hours=9)))
    
    collections = ['tasks', 'tasks_s']
    all_data = []
    
    for col in collections:
        docs = db.collection(col).where('日時', '>=', start_dt).where('日時', '<=', end_dt).stream()
        for doc in docs:
            d = doc.to_dict()
            d['id'] = doc.id
            d['_collection'] = col
            all_data.append(d)
            
    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

if 'success_msg' not in st.session_state:
    st.session_state.success_msg = ""
if 'date' not in st.session_state:
    st.session_state.date = datetime.now(timezone(timedelta(hours=9))).date()

st.title("製本記録アプリ")

# 成功メッセージの表示
if st.session_state.success_msg:
    st.success(st.session_state.success_msg)
    st.session_state.success_msg = ""

# サイドバー: 共通設定
with st.sidebar:
    st.header("設定")
    st.session_state.date = st.date_input("日付", st.session_state.date)
    
    # 担当者リスト（カスタマイズ可能）
    worker_list = ["", "作業者A", "作業者B", "作業者C", "作業者D", "作業者E"]
    worker = st.selectbox("担当者", worker_list)
    
    if st.button("🔄 データを最新に更新"):
        load_from_firestore.clear()
        load_schedule.clear()
        load_schedule_m.clear()
        st.rerun()

    with st.expander("🛠️ 管理者メニュー"):
        st.markdown("##### CSV手動アップロード")
        uploaded_file = st.file_uploader("予定表 (schedule.csv)", type="csv")
        if uploaded_file is not None:
            # 一時的にローカル保存して読み込ませる
            with open("schedule.csv", "wb") as f:
                f.write(uploaded_file.getbuffer())
            load_schedule.clear()
            st.success("予定表を手動で適用しました")
            
        uploaded_m_file = st.file_uploader("明細 (schedule_m.csv) ※カレンダー用", type="csv")
        if uploaded_m_file is not None:
            st.session_state.uploaded_sch_m = io.BytesIO(uploaded_m_file.getvalue())
            load_schedule_m.clear()
            st.success("明細ファイルを手動で適用しました")

sch = load_schedule()
sch_m, sch_m_error = load_schedule_m()

if not sch.empty:
    sch['得意先名'] = sch['得意先名'].apply(clean_text)
    sch['品名'] = sch['品名'].apply(clean_text)

tasks_df_all = load_from_firestore(st.session_state.date)
tasks_df = pd.DataFrame()
done_tasks_df = pd.DataFrame()
if not tasks_df_all.empty:
    # 状態が「完了」のものを分離
    done_tasks_df = tasks_df_all[tasks_df_all['状態'] == '完了']
    # それ以外（進行中、保留など）
    tasks_df = tasks_df_all[tasks_df_all['状態'] != '完了']

tab1, tab2, tab3, tab4 = st.tabs(["📋 日報", "🏭 通常工程の記録", "📅 カレンダー一括管理", "⚙️ 未照合データ一括修正"])

# ==========================================
# TAB 1: 日報 (閲覧メイン)
# ==========================================
with tab1:
    st.header(f"📋 {st.session_state.date.strftime('%Y/%m/%d')} の作業日報")
    
    if tasks_df_all.empty:
        st.info("この日の作業記録はありません。")
    else:
        # 完了した作業と進行中の作業を分けて表示
        st.subheader("✅ 完了した作業")
        if not done_tasks_df.empty:
            st.dataframe(
                done_tasks_df[['日時', '状態', '作業者', '工程', '得意先', '製品名', '詳細', '出来数', '時間']],
                use_container_width=True, hide_index=True
            )
        else:
            st.write("完了した作業はありません。")
            
        st.subheader("▶️ 進行中・保留の作業")
        if not tasks_df.empty:
            st.dataframe(
                tasks_df[['日時', '状態', '作業者', '工程', '得意先', '製品名', '詳細', '出来数', '時間']],
                use_container_width=True, hide_index=True
            )
        else:
            st.write("進行中の作業はありません。")

# ==========================================
# TAB 2: 通常工程の記録
# ==========================================
with tab2:
    st.header("🏭 通常工程の記録")
    
    # 進行中の作業一覧（自分用）
    if not tasks_df.empty and worker:
        # カレンダーとして判定されたものを除外 (カレンダー一括管理で扱うため)
        my_tasks = tasks_df[(tasks_df['作業者'] == worker) & (tasks_df.get('is_calendar', False) != True)]
        # CSVの適用欄に「カレンダー」と書かれている品名も除外
        calendar_products = []
        if not sch.empty and '適用' in sch.columns:
            calendar_products = sch[sch['適用'].astype(str).str.contains('カレンダー', na=False)]['品名'].tolist()
        my_tasks = my_tasks[~my_tasks['製品名'].isin(calendar_products)]
        
        if not my_tasks.empty:
            st.subheader(f"▶️ {worker} さんの進行中・保留の作業")
            for _, row in my_tasks.iterrows():
                with st.container():
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.markdown(f"**{row.get('製品名', '')}** / {row.get('工程', '')} / {row.get('詳細', '')}")
                        st.caption(f"状態: {row.get('状態', '')} | 開始: {row.get('日時', '')} | 現在の出来数: {row.get('出来数', 0)}")
                    with col2:
                        # 終了・更新ボタンなど
                        if st.button("更新/完了", key=f"btn_upd_{row['id']}"):
                            st.session_state.update_target = row.to_dict()
                            
                    # 更新用フォームをインライン表示
                    if 'update_target' in st.session_state and st.session_state.update_target.get('id') == row['id']:
                        with st.form(key=f"form_{row['id']}"):
                            u_qty = st.number_input("追加の出来数", min_value=0, value=0)
                            u_time = st.number_input("掛かった時間(分)", min_value=0, value=0)
                            u_state = st.selectbox("状態の変更", ["進行中", "保留", "完了"], index=["進行中", "保留", "完了"].index(row.get('状態', '進行中')))
                            
                            if st.form_submit_button("記録を保存"):
                                try:
                                    doc_ref = db.collection(row['_collection']).document(row['id'])
                                    new_qty = row.get('出来数', 0) + u_qty
                                    new_time = row.get('時間', 0) + u_time
                                    doc_ref.update({
                                        '出来数': new_qty,
                                        '時間': new_time,
                                        '状態': u_state,
                                        '更新日時': firestore.SERVER_TIMESTAMP
                                    })
                                    st.session_state.success_msg = f"作業「{row.get('製品名')}」を更新しました。"
                                    del st.session_state.update_target
                                    load_from_firestore.clear()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"更新エラー: {e}")
            st.divider()

    # 新規登録フォーム
    st.subheader("➕ 新規作業の記録")
    
    if not worker:
        st.warning("サイドバーで担当者を選択してください。")
    else:
        with st.container():
            # CSVがある場合は得意先で絞り込み
            customers = ["（手入力）"]
            if not sch.empty:
                customers.extend(sorted(sch['得意先名'].dropna().unique()))
                
            selected_customer = st.selectbox("得意先", customers)
            
            # 品名の選択リスト作成
            products = ["（手入力）"]
            
            # 手入力以外の得意先が選ばれた場合、その得意先の製品だけを抽出
            if selected_customer != "（手入力）" and not sch.empty:
                filtered_sch = sch[sch['得意先名'] == selected_customer]
                products.extend(filtered_sch['品名'].dropna().unique())
            
            selected_product = st.selectbox("製品名", products)
            
            manual_product = ""
            if selected_product == "（手入力）":
                manual_product = st.text_input("製品名を手入力")
                
            process = st.selectbox("工程", ["断裁", "折", "丁合", "綴じ", "梱包", "その他"])
            detail = st.text_input("詳細 (仕様や名入れ先など)")
            qty = st.number_input("出来数", min_value=0, value=0)
            work_time = st.number_input("掛かった時間(分)", min_value=0, value=0)
            state = st.selectbox("状態", ["完了", "進行中", "保留"])
            
            if st.button("この作業を記録する", type="primary"):
                final_product = manual_product if selected_product == "（手入力）" else selected_product
                final_customer = "" if selected_customer == "（手入力）" else selected_customer
                
                if not final_product:
                    st.error("製品名を入力してください。")
                else:
                    try:
                        # 予定表から該当する行を探す (単一作業の場合は先頭行を利用)
                        matched_row = {}
                        if not sch.empty and selected_product != "（手入力）":
                            matches = sch[sch['品名'] == final_product]
                            if not matches.empty:
                                matched_row = matches.iloc[0].to_dict()
                        
                        # 適用欄に「カレンダー」が含まれているかチェック
                        is_calendar = False
                        if '適用' in matched_row and pd.notna(matched_row['適用']):
                            if 'カレンダー' in str(matched_row['適用']):
                                is_calendar = True
                        
                        doc_data = {
                            '日時': firestore.SERVER_TIMESTAMP,
                            '作業者': worker,
                            '得意先': final_customer,
                            '製品名': final_product,
                            '工程': process,
                            '詳細': detail,
                            '出来数': qty,
                            '時間': work_time,
                            '状態': state,
                            'is_calendar': is_calendar # カレンダーフラグを保存
                        }
                        
                        db.collection('tasks').add(doc_data)
                        st.session_state.success_msg = f"新規作業「{final_product}」を記録しました。"
                        load_from_firestore.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"保存エラー: {e}")

# ==========================================
# TAB 3: カレンダー一括管理
# ==========================================
with tab3:
    st.header("📅 カレンダー一括管理")
    
    if sch_m_error:
        st.markdown(f"""
        <div style="background-color: #ffebee; border-left: 6px solid #f44336; padding: 10px; margin-bottom: 15px; border-radius: 4px;">
            <strong style="color: #d32f2f;">⚠️ 【警告】明細データ(schedule_m.csv) が読み込めていません！</strong><br>
            そのため名入れを検索できず単体登録になります。<br>
            <span style="font-size: 0.9em; color: #555;">【詳細な原因】 {sch_m_error}</span><br>
            <span style="font-size: 0.9em;">※サイドバーの「🛠️ 管理者メニュー」から手動アップロードを試すか、設定を確認してください。</span>
        </div>
        """, unsafe_allow_html=True)
    
    if sch.empty:
        st.warning("予定表(schedule.csv)が読み込まれていません。サイドバーから手動アップロードするか設定を確認してください。")
    else:
        # カレンダーとして指定された製品を抽出
        calendar_sch = sch[sch['適用'].astype(str).str.contains('カレンダー', na=False)]
        if calendar_sch.empty:
            st.info("予定表に「カレンダー」が含まれる製品がありません。")
        else:
            customers = sorted(calendar_sch['得意先名'].dropna().unique())
            selected_group_customer = st.selectbox("得意先を選択してください (カレンダー)", [""] + list(customers))
            
            if selected_group_customer:
                group_products = calendar_sch[calendar_sch['得意先名'] == selected_group_customer]['品名'].dropna().unique()
                selected_group_product = st.selectbox("カレンダー品名を選択", [""] + list(group_products))
                
                if selected_group_product:
                    st.markdown(f"#### 📅 {selected_group_product}")
                    parent_row = sch[sch['品名'] == selected_group_product].iloc[0] if not sch[sch['品名'] == selected_group_product].empty else {}
                    
                    # --- 1. 明細(名入れ)の取得処理 ---
                    t_m = pd.DataFrame()
                    if not sch_m.empty:
                        # アプローチ1: A列の完全一致
                        sch_a = sch.iloc[:, 0].astype(str).str.strip().str.replace('.0', '', regex=False)
                        sch_m_a = sch_m.iloc[:, 0].astype(str).str.strip().str.replace('.0', '', regex=False)
                        
                        d_val = ""
                        if not sch[sch['品名'] == selected_group_product].empty:
                            target_idx = sch[sch['品名'] == selected_group_product].index[0]
                            d_val = sch_a.iloc[target_idx]
                        
                        if d_val:
                            t_m = sch_m[sch_m_a == d_val]
                        
                        if t_m.empty:
                            # アプローチ2: 品名から逆引き
                            matched_m = sch_m[sch_m.astype(str).apply(lambda x: x.str.contains(selected_group_product, na=False)).any(axis=1)]
                            if not matched_m.empty:
                                rev_d_val = sch_m_a.iloc[matched_m.index[0]]
                                t_m = sch_m[sch_m_a == rev_d_val]
                                
                        if t_m.empty and '得意先名' in parent_row:
                            # アプローチ3: 得意先から抽出
                            tokuisaki = str(parent_row['得意先名']).strip()
                            if tokuisaki:
                                t_m = sch_m[sch_m.astype(str).apply(lambda x: x.str.contains(tokuisaki, na=False)).any(axis=1)]
                    
                    target_items = {}
                    if not t_m.empty:
                        if '内容コード' in t_m.columns:
                            # 内容コード「9」のものを抽出
                            naire_df = t_m[(t_m['内容コード'].astype(str).str.strip().str.replace('.0', '', regex=False) == '9') | (t_m['内容コード'] == 9)]
                            for _, m_row in naire_df.iterrows():
                                m_name = m_row.get('内容', '')
                                m_qty = m_row.get('数量', 0)
                                if pd.notna(m_name) and m_name != "":
                                    target_items[m_name] = m_qty
                    
                    # --- 2. 進捗ダッシュボードの表示 ---
                    proc_cols = ["断裁", "丁合", "綴じ", "梱包"]
                    cols = st.columns(len(proc_cols))
                    
                    for idx, proc_name in enumerate(proc_cols):
                        # 完了したデータと進行中のデータを合算
                        c_done = done_tasks_df[(done_tasks_df['製品名'] == selected_group_product) & (done_tasks_df['工程'] == proc_name)]
                        c_prog = tasks_df[(tasks_df['製品名'] == selected_group_product) & (tasks_df['工程'] == proc_name)]
                        
                        c_qty = c_done['出来数'].sum() + c_prog['出来数'].sum()
                        
                        is_done = False
                        total_qty = t_m['数量'].sum() if (not t_m.empty and '数量' in t_m.columns) else 0
                        
                        # 総数の9割以上完了で「済」とする
                        if total_qty > 0:
                            if c_qty >= (total_qty * 0.9):
                                is_done = True
                        else:
                            if c_qty > 0:
                                is_done = True
                                
                        status_icon = "✅ 済" if is_done else "➖"
                        with cols[idx]:
                            st.markdown(f"**{proc_name}**<br><span style='font-size:1.2rem;'>{status_icon}</span>", unsafe_allow_html=True)
                    st.divider()

                    st.markdown("##### 📝 作業の一括記録")
                    if target_items:
                        st.info("このカレンダーに含まれる名入れ先が抽出されました。作業した名入れ先にチェックを入れてください。")
                        with st.form(key=f"group_form_{selected_group_product}"):
                            c1, c2, c3 = st.columns(3)
                            with c1: group_proc = st.selectbox("工程", ["断裁", "丁合", "綴じ", "梱包", "その他"])
                            with c2: group_state = st.selectbox("状態", ["完了", "進行中", "保留"])
                            with c3: group_time = st.number_input("全体の合計時間(分) ※部数で按分されます", min_value=0, value=0)
                            
                            st.write("**名入れ先リスト:**")
                            checked_items = {}
                            for name, qty in target_items.items():
                                if st.checkbox(f"{name} (部数: {qty})", key=f"chk_{name}"):
                                    checked_items[name] = qty
                                    
                            if st.form_submit_button("一括記録する", type="primary"):
                                if not worker:
                                    st.error("サイドバーで担当者を選択してください。")
                                elif not checked_items:
                                    st.error("作業した名入れ先を1つ以上選択してください。")
                                else:
                                    db_batch = db.batch()
                                    
                                    # 時間の按分計算用（選択された部数の合計）
                                    checked_total_qty = sum(checked_items.values())
                                    
                                    for name, qty in checked_items.items():
                                        doc_ref = db.collection('tasks').document()
                                        
                                        # 部数に応じて時間を按分する
                                        assigned_time = 0
                                        if group_time > 0:
                                            if checked_total_qty > 0:
                                                assigned_time = int(group_time * (qty / checked_total_qty))
                                            else:
                                                assigned_time = group_time // len(checked_items)
                                                
                                        data = {
                                            '日時': firestore.SERVER_TIMESTAMP,
                                            '作業者': worker,
                                            '得意先': selected_group_customer,
                                            '製品名': selected_group_product,
                                            '工程': group_proc,
                                            '詳細': name,
                                            '出来数': qty,
                                            '時間': assigned_time,
                                            '状態': group_state,
                                            'is_calendar': True
                                        }
                                        db_batch.set(doc_ref, data)
                                    db_batch.commit()
                                    st.session_state.success_msg = f"「{selected_group_product}」の作業を {len(checked_items)}件一括登録しました（時間按分済）。"
                                    load_from_firestore.clear()
                                    st.rerun()
                    else:
                        st.warning("明細データに名入れ（内容コード9）が見つかりませんでした。単体で記録します。")
                        with st.form(key=f"single_form_{selected_group_product}"):
                            c1, c2 = st.columns(2)
                            with c1: s_proc = st.selectbox("工程", ["断裁", "丁合", "綴じ", "梱包", "その他"])
                            with c2: s_state = st.selectbox("状態", ["完了", "進行中", "保留"])
                            c3, c4 = st.columns(2)
                            with c3: s_qty = st.number_input("出来数", min_value=0, value=0)
                            with c4: s_time = st.number_input("時間(分)", min_value=0, value=0)
                            
                            if st.form_submit_button("単体で記録する", type="primary"):
                                if not worker:
                                    st.error("担当者を選択してください。")
                                else:
                                    db.collection('tasks').add({
                                        '日時': firestore.SERVER_TIMESTAMP,
                                        '作業者': worker,
                                        '得意先': selected_group_customer,
                                        '製品名': selected_group_product,
                                        '工程': s_proc,
                                        '詳細': "名入れ不明（単体登録）",
                                        '出来数': s_qty,
                                        '時間': s_time,
                                        '状態': s_state,
                                        'is_calendar': True
                                    })
                                    st.session_state.success_msg = f"「{selected_group_product}」を記録しました。"
                                    load_from_firestore.clear()
                                    st.rerun()
                                    
                    st.divider()
                    
                    # 進行中の作業一覧（カレンダー固有）
                    # カレンダーフラグが付いている、または製品名が一致するもの
                    group_tasks = tasks_df[(tasks_df['製品名'] == selected_group_product) | (tasks_df.get('is_calendar', False) == True) & (tasks_df['製品名'] == selected_group_product)]
                    if not group_tasks.empty:
                        with st.expander(f"📅 【{selected_group_product}】の進行中・保留の作業", expanded=False):
                            for _, row in group_tasks.iterrows():
                                with st.container():
                                    c1, c2, c3 = st.columns([2, 3, 1])
                                    with c1:
                                        st.write(f"**{row.get('工程', '')}** ({row.get('作業者', '')})")
                                    with c2:
                                        st.write(f"**詳細:** {row.get('詳細', '')}")
                                        st.caption(f"状態: {row.get('状態', '')} | 出来数: {row.get('出来数', 0)} | 時間: {row.get('時間', 0)}分")
                                    with c3:
                                        if st.button("完了にする", key=f"g_done_{row['id']}"):
                                            db.collection(row['_collection']).document(row['id']).update({
                                                '状態': '完了',
                                                '更新日時': firestore.SERVER_TIMESTAMP
                                            })
                                            st.session_state.success_msg = f"作業「{row.get('詳細', '')}」を完了しました。"
                                            load_from_firestore.clear()
                                            st.rerun()

# ==========================================
# TAB 4: 管理者：未照合データ一括修正
# ==========================================
with tab4:
    st.header("⚙️ 未照合データ(手入力) の一括修正")
    st.markdown("手入力されてCSVと一致しない「製品名」を、正しい予定表の品名に一括で書き換えます。")
    
    col_d1, col_d2 = st.columns(2)
    with col_d1: start_date = st.date_input("開始日", st.session_state.date - timedelta(days=7))
    with col_d2: end_date = st.date_input("終了日", st.session_state.date)
    
    if start_date > end_date:
        st.error("開始日は終了日以前にしてください。")
    else:
        all_tasks_df = load_all_tasks_for_period(start_date, end_date)
        
        official_products = []
        if not sch.empty:
            official_products = sch['品名'].dropna().unique().tolist()
            
        unmatched_products = []
        if not all_tasks_df.empty:
            recorded_products = all_tasks_df['製品名'].dropna().unique().tolist()
            unmatched_products = [p for p in recorded_products if p not in official_products]
            
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
            
            st.write("**変更後の詳細（名入れ先など）※任意**")
            target_detail = st.text_input("詳細を一括で上書きする場合", help="現場が「名入れ先」を間違って品名として登録してしまった場合、ここで本来の名入れ先に移し替えることができます。")
            
        final_target = manual_target if manual_target else target_product
        
        if source_product and not source_product.startswith("（"):
            st.markdown(f"##### Step 2: 「{source_product}」の対象データ確認")
            target_rows = all_tasks_df[all_tasks_df['製品名'] == source_product]
            st.write(f"対象件数: {len(target_rows)} 件")
            st.dataframe(target_rows[['日時', '作業者', '得意先', '製品名', '工程', '詳細']], use_container_width=True)
            
        st.markdown("##### Step 3: 一括書き換えの実行")
        if st.button("この品名を一括で書き換える", type="primary"):
            if not source_product or source_product.startswith("（"):
                st.error("変更元の品名を正しく選択してください。")
            elif not final_target:
                st.error("変更先の品名を入力または選択してください。")
            elif source_product == final_target and not target_detail:
                st.error("変更内容（品名か詳細）を入力してください。")
            else:
                with st.spinner(f"「{source_product}」を変更中..."):
                    try:
                        target_rows = all_tasks_df[all_tasks_df['製品名'] == source_product]
                        if target_rows.empty:
                            st.warning("該当する品名のデータが見つかりませんでした。")
                        else:
                            db_batch = db.batch()
                            update_count = 0
                            
                            for _, row in target_rows.iterrows():
                                doc_id = row.get('id')
                                col_name = row.get('_collection')
                                if col_name and doc_id:
                                    doc_ref = db.collection(col_name).document(doc_id)
                                    
                                    # 品名の更新に加え、詳細(名入れ)も入力されていれば更新する
                                    update_data = {"製品名": final_target}
                                    if target_detail:
                                        update_data["詳細"] = target_detail.strip()
                                        update_data["is_calendar"] = True # カレンダーとして強制認識させる
                                        
                                    db_batch.update(doc_ref, update_data)
                                    update_count += 1
                                    
                            if update_count > 0:
                                db_batch.commit()
                                detail_msg = f"（詳細を「{target_detail}」に上書きしました）" if target_detail else ""
                                st.session_state.success_msg = f"✅ {update_count}件の作業記録を「{final_target}」に書き換えました！{detail_msg}"
                                load_from_firestore.clear()
                                load_all_tasks_for_period.clear()
                                st.rerun()
                    except Exception as e:
                        st.error(f"更新中にエラーが発生しました: {e}")
