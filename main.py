import os
import pandas as pd
import requests
from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from dotenv import load_dotenv
from urllib.parse import urlparse
from requests.auth import HTTPBasicAuth
from datetime import datetime
from bs4 import BeautifulSoup
import io
import urllib3

# Disable SSL warnings (not recommended for prod)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()
USER = os.getenv("USER", "admin")
PASS = os.getenv("PASS", "admin")
SECRET_KEY = os.getenv("SECRET_KEY", "changeme")
UPLOAD_DIR = "uploads"
STATIC_DIR = "static"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)

templates = Jinja2Templates(directory="templates")
app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ====== Helper functions for schema ======
def read_accounts_and_data(file_path):
    xls = pd.ExcelFile(file_path)
    sheet_names = [s.lower() for s in xls.sheet_names]
    if 'accounts' in sheet_names:
        accounts_df = pd.read_excel(xls, sheet_name=[n for n in xls.sheet_names if n.lower() == 'accounts'][0])
    elif 'account' in sheet_names:
        accounts_df = pd.read_excel(xls, sheet_name=[n for n in xls.sheet_names if n.lower() == 'account'][0])
    else:
        raise Exception("Không tìm thấy sheet 'accounts' hoặc 'account' trong file.")

    if 'data' in sheet_names:
        data_df = pd.read_excel(xls, sheet_name=[n for n in xls.sheet_names if n.lower() == 'data'][0])
    else:
        raise Exception("Không tìm thấy sheet 'data' trong file.")

    return accounts_df, data_df

def get_account_dict(accounts_df):
    acc_dict = {}
    for _, row in accounts_df.iterrows():
        key = str(row['site']).strip().lower()
        acc_dict[key] = {
            "WP_API_URL": str(row['WP_API_URL']).strip(),
            "WP_USER": str(row['WP_USER']).strip(),
            "WP_APP_PASS": str(row['WP_APP_PASS']).strip()
        }
    return acc_dict

def is_homepage_url(url):
    parsed = urlparse(url)
    path = parsed.path.rstrip('/')
    if not path and (not parsed.query and not parsed.fragment):
        return True
    return path == ''

def get_homepage_id(account):
    api_endpoint = f"{account['WP_API_URL']}/wp-json/wp/v2/settings"
    resp = requests.get(api_endpoint, auth=HTTPBasicAuth(account['WP_USER'], account['WP_APP_PASS']), verify=False)
    if resp.status_code == 200:
        page_id = resp.json().get('page_on_front', 0)
        try:
            page_id = int(page_id)
        except Exception:
            page_id = 0
        if page_id > 0:
            return page_id
    return None

def get_id_from_url(url, type_, account):
    if type_ in ["post", "page"]:
        if is_homepage_url(url):
            homepage_id = get_homepage_id(account)
            if homepage_id:
                return homepage_id
        slug = urlparse(url).path.rstrip('/').split('/')[-1]
        api_endpoint = f"{account['WP_API_URL']}/wp-json/wp/v2/{type_}s"
        params = {"per_page": 1, "slug": slug}
        resp = requests.get(api_endpoint, params=params, auth=HTTPBasicAuth(account['WP_USER'], account['WP_APP_PASS']), verify=False)
        if resp.status_code == 200 and resp.json():
            return resp.json()[0]['id']
    elif type_ == "category":
        slug = urlparse(url).path.rstrip('/').split('/')[-1]
        api_endpoint = f"{account['WP_API_URL']}/wp-json/wp/v2/categories"
        params = {"per_page": 1, "slug": slug}
        resp = requests.get(api_endpoint, params=params, auth=HTTPBasicAuth(account['WP_USER'], account['WP_APP_PASS']), verify=False)
        if resp.status_code == 200 and resp.json():
            return resp.json()[0]['id']
    return None

def get_current_schema(post_id, type_, account):
    if type_ in ["post", "page"]:
        api_endpoint = f"{account['WP_API_URL']}/wp-json/wp/v2/{type_}s/{post_id}"
        resp = requests.get(api_endpoint, auth=HTTPBasicAuth(account['WP_USER'], account['WP_APP_PASS']), verify=False)
        if resp.status_code == 200:
            meta = resp.json().get('meta', {})
            inpost = meta.get('_inpost_head_script', {})
            if isinstance(inpost, dict):
                return inpost.get('synth_header_script', '') or ''
    elif type_ == "category":
        api_endpoint = f"{account['WP_API_URL']}/wp-json/wp/v2/categories/{post_id}"
        resp = requests.get(api_endpoint, auth=HTTPBasicAuth(account['WP_USER'], account['WP_APP_PASS']), verify=False)
        if resp.status_code == 200:
            meta = resp.json().get('meta', {})
            return meta.get('category_schema', '') or ''
    return ''

def update_schema(item_id, script_schema, type_, account):
    script_schema = script_schema.strip() if script_schema else ""
    if type_ in ["post", "page"]:
        api_endpoint = f"{account['WP_API_URL']}/wp-json/wp/v2/{type_}s/{item_id}"

        if script_schema == "":
            payload = {
                "meta": {
                    "_inpost_head_script": {
                        "synth_header_script": ""
                    }
                }
            }
        else:
            old_schema = get_current_schema(item_id, type_, account)
            if old_schema and script_schema in old_schema:
                new_schema = old_schema
            elif old_schema:
                new_schema = (old_schema.rstrip() + "\n" + script_schema)
            else:
                new_schema = script_schema

            payload = {
                "meta": {
                    "_inpost_head_script": {
                        "synth_header_script": new_schema
                    }
                }
            }

        resp = requests.patch(api_endpoint, json=payload, auth=HTTPBasicAuth(account['WP_USER'], account['WP_APP_PASS']), verify=False)
        if resp.status_code == 200:
            return True, None
        else:
            try:
                error_detail = resp.json()
            except Exception:
                error_detail = resp.text
            return False, error_detail

    elif type_ == "category":
        api_endpoint = f"{account['WP_API_URL']}/wp-json/wp/v2/categories/{item_id}"
        get_resp = requests.get(api_endpoint, auth=HTTPBasicAuth(account['WP_USER'], account['WP_APP_PASS']), verify=False)
        html_description = ""
        if get_resp.status_code == 200:
            data = get_resp.json()
            html_description = data.get("description", "")

        payload = {
            "meta": {
                "category_schema": script_schema
            }
        }
        patch_resp = requests.patch(api_endpoint, json=payload, auth=HTTPBasicAuth(account['WP_USER'], account['WP_APP_PASS']), verify=False)

        fix_payload = {
            "description": html_description
        }
        fix_resp = requests.patch(api_endpoint, json=fix_payload, auth=HTTPBasicAuth(account['WP_USER'], account['WP_APP_PASS']), verify=False)

        if patch_resp.status_code == 200:
            return True, None
        else:
            try:
                error_detail = patch_resp.json()
            except Exception:
                error_detail = patch_resp.text
            return False, error_detail

    else:
        return False, f"Loại '{type_}' không hỗ trợ"

def process_excel_multi_account(file_path, action="chencode"):
    logs = []
    delete_mode = (action == "xoascript")
    try:
        accounts_df, data_df = read_accounts_and_data(file_path)
        accounts_dict = get_account_dict(accounts_df)
        require_cols = {'url', 'type', 'site'} if delete_mode else {'url', 'script_schema', 'type', 'site'}
        if not require_cols.issubset(data_df.columns):
            return [f"File không đúng định dạng. Sheet 'data' phải có cột: {require_cols}"], None

        results = []
        for idx, row in data_df.iterrows():
            url = row['url']
            type_ = row['type'].strip().lower()
            site = str(row['site']).strip().lower()
            schema = "" if delete_mode else row.get('script_schema', '')
            account = accounts_dict.get(site)
            if not account:
                msg = f"🚫❌ [{idx+1}] Không tìm thấy tài khoản cho site: {site}"
                logs.append(msg)
                results.append({"stt": idx+1, "url": url, "site": site, "type": type_, "result": "Không tìm thấy tài khoản"})
                continue

            item_id = get_id_from_url(url, type_, account)
            if not item_id:
                msg = f"🚫❌ [{idx+1}] Không tìm thấy ID cho URL: {url} (loại: {type_}, site: {site})"
                logs.append(msg)
                results.append({"stt": idx+1, "url": url, "site": site, "type": type_, "result": "Không tìm thấy ID"})
                continue
            ok, detail = update_schema(item_id, schema, type_, account)
            if ok:
                action_text = "Xoá" if delete_mode else "Cập nhật"
                msg = f"✨✅ [{idx+1}] {action_text} schema cho {type_} ID {item_id} thành công (site: {site})"
                result = "Thành công"
            else:
                msg = f"🚫❌ [{idx+1}] Lỗi khi {('xoá' if delete_mode else 'cập nhật')} schema cho {type_} ID {item_id} (site: {site})"
                result = f"Lỗi: {detail}"
                logs.append(f"💥⚠️ [{idx+1}] Chi tiết lỗi: {detail}")
            logs.append(msg)
            results.append({"stt": idx+1, "url": url, "site": site, "type": type_, "result": result})

        # Kết quả xuất file Excel
        df_result = pd.DataFrame(results)
        out_file = os.path.join(UPLOAD_DIR, f"result_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx")
        df_result.to_excel(out_file, index=False)
        return logs, out_file
    except Exception as e:
        return [f"🚫❌ Lỗi khi xử lý: {e}"], None

# ========== Helper for CRAWL ==========
def crawl_url(url):
    try:
        try:
            res = requests.get(url, timeout=10)
        except requests.exceptions.SSLError:
            res = requests.get(url, timeout=10, verify=False)
        soup = BeautifulSoup(res.text, "html.parser")
        title = soup.find("meta", property="og:title")
        desc = soup.find("meta", property="og:description")
        image = soup.find("meta", property="og:image") or soup.find("meta", property="og:image:secure_url")
        entry_date = soup.find("time", class_="entry-date published updated")
        updated_time = soup.find("meta", property="og:updated_time")
        date = None
        if entry_date:
            date = entry_date.get("datetime") or entry_date.text
        elif updated_time:
            date = updated_time.get("content")
        return {
            "URL": url,
            "Title": title["content"] if title and "content" in title.attrs else "",
            "Description": desc["content"] if desc and "content" in desc.attrs else "",
            "Date": date or "",
            "Image": image["content"] if image and "content" in image.attrs else ""
        }
    except Exception:
        return {
            "URL": url,
            "Title": "",
            "Description": "",
            "Date": "",
            "Image": ""
        }

# ========== ROUTES ==========

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login")
    return RedirectResponse("/dashboard")

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": ""})

@app.post("/login", response_class=HTMLResponse)
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == USER and password == PASS:
        request.session["user"] = username
        return RedirectResponse("/dashboard", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": "Sai tài khoản hoặc mật khẩu"})

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login")
    return RedirectResponse("/dashboard")

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login")
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.post("/login", response_class=HTMLResponse)
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == USER and password == PASS:
        request.session["user"] = username
        return RedirectResponse("/dashboard", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": "Sai tài khoản hoặc mật khẩu"})

@app.get("/logout", response_class=HTMLResponse)
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login")

@app.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login")
    return templates.TemplateResponse("upload.html", {"request": request, "logs": None, "file_url": None})

@app.post("/upload", response_class=HTMLResponse)
async def do_upload(request: Request, action: str = Form(...), file: UploadFile = File(...)):
    if not request.session.get("user"):
        return RedirectResponse("/login")
    temp_file = os.path.join(UPLOAD_DIR, f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{file.filename}")
    with open(temp_file, "wb") as f:
        f.write(await file.read())
    logs, out_file = process_excel_multi_account(temp_file, action=action)
    file_url = f"/static/{os.path.basename(out_file)}" if out_file else None
    # Copy file to static dir for download
    if out_file:
        import shutil
        shutil.copy(out_file, os.path.join(STATIC_DIR, os.path.basename(out_file)))
    return templates.TemplateResponse(
        "upload.html", {"request": request, "logs": logs, "file_url": file_url}
    )

@app.get("/crawl", response_class=HTMLResponse)
def crawl_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login")
    return templates.TemplateResponse("crawl.html", {"request": request, "result": None, "file_url": None, "error": None})

@app.post("/crawl", response_class=HTMLResponse)
async def do_crawl(request: Request, file: UploadFile = File(...)):
    if not request.session.get("user"):
        return RedirectResponse("/login")
    try:
        df = pd.read_excel(io.BytesIO(await file.read()))
        if "URL" not in df.columns:
            return templates.TemplateResponse(
                "crawl.html", {"request": request, "result": None, "file_url": None, "error": "File phải có cột tên 'URL'!"}
            )
        urls = df["URL"].dropna().tolist()
        result = []
        for url in urls:
            data = crawl_url(str(url).strip())
            result.append(data)
        result_df = pd.DataFrame(result)
        output = io.BytesIO()
        result_df.to_excel(output, index=False)
        output.seek(0)
        # Lưu file để có link download
        save_name = f"crawl_result_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
        out_path = os.path.join(STATIC_DIR, save_name)
        with open(out_path, "wb") as f:
            f.write(output.read())
        output.seek(0)
        return templates.TemplateResponse(
            "crawl.html", {
                "request": request,
                "result": result,
                "file_url": f"/static/{save_name}",
                "error": None
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "crawl.html", {"request": request, "result": None, "file_url": None, "error": f"Lỗi: {e}"}
        )

# Run with: uvicorn main:app --host 0.0.0.0 --port 8080
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080)
