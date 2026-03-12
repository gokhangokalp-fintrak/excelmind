"""
ExcelMind Backend Server (Flask)
=================================
Auth: Google OAuth + Magic Link
DB: PostgreSQL
Admin: /admin panel
"""

import os
import sys
import json
import uuid
import hashlib
import secrets
import traceback
import urllib.request
import ssl
from datetime import datetime, timedelta, date
from functools import wraps

from flask import Flask, request, jsonify, send_file, send_from_directory, redirect, make_response
import psycopg2
from psycopg2.extras import RealDictCursor

# Add parent dir
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from smart_excel_engine import build_smart_excel, analyze_for_dashboard, detect_data_type, read_excel

# ============================================================
# CONFIG
# ============================================================
app = Flask(__name__, static_folder=None)

PORT = int(os.environ.get("PORT", 8080))
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
SMTP_EMAIL = os.environ.get("SMTP_EMAIL", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
APP_URL = os.environ.get("APP_URL", "https://excelmind.onrender.com")
ADMIN_EMAILS = os.environ.get("ADMIN_EMAILS", "easylifegroup.tr@gmail.com").split(",")
SECRET_KEY = os.environ.get("SECRET_KEY", secrets.token_hex(32))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Free plan limits
FREE_MONTHLY_UPLOADS = 5
TRIAL_DAYS = 14

# ============================================================
# DATABASE
# ============================================================
def get_db():
    """Get database connection"""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return conn

def init_db():
    """Create tables if not exist"""
    if not DATABASE_URL:
        print("[DB] No DATABASE_URL configured, skipping init")
        return

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255),
            avatar_url TEXT,
            plan VARCHAR(20) DEFAULT 'free',
            trial_start TIMESTAMP,
            trial_used BOOLEAN DEFAULT FALSE,
            stripe_customer_id VARCHAR(255),
            created_at TIMESTAMP DEFAULT NOW(),
            last_login TIMESTAMP DEFAULT NOW(),
            is_admin BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS sessions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            token VARCHAR(255) UNIQUE NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS magic_links (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email VARCHAR(255) NOT NULL,
            token VARCHAR(255) UNIQUE NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            used BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS usage_logs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE SET NULL,
            action VARCHAR(50) NOT NULL,
            filename VARCHAR(255),
            data_type VARCHAR(50),
            file_size INTEGER,
            ip_address VARCHAR(50),
            user_agent TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS page_views (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            path VARCHAR(255),
            ip_address VARCHAR(50),
            user_agent TEXT,
            referrer TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS payments (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE SET NULL,
            email VARCHAR(255),
            amount DECIMAL(10,2) NOT NULL,
            currency VARCHAR(10) DEFAULT 'TRY',
            plan VARCHAR(20) NOT NULL,
            payment_type VARCHAR(30) NOT NULL,
            payment_provider VARCHAR(30),
            provider_payment_id VARCHAR(255),
            status VARCHAR(20) DEFAULT 'completed',
            period_start TIMESTAMP,
            period_end TIMESTAMP,
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
        CREATE INDEX IF NOT EXISTS idx_magic_links_token ON magic_links(token);
        CREATE INDEX IF NOT EXISTS idx_usage_logs_user ON usage_logs(user_id);
        CREATE INDEX IF NOT EXISTS idx_usage_logs_created ON usage_logs(created_at);
        CREATE INDEX IF NOT EXISTS idx_page_views_created ON page_views(created_at);
        CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id);
        CREATE INDEX IF NOT EXISTS idx_payments_created ON payments(created_at);
        CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[DB] Tables initialized")

# ============================================================
# AUTH HELPERS
# ============================================================
def get_current_user():
    """Get user from session token cookie"""
    token = request.cookies.get('session_token')
    if not token or not DATABASE_URL:
        return None

    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT u.* FROM users u
            JOIN sessions s ON s.user_id = u.id
            WHERE s.token = %s AND s.expires_at > NOW()
        """, (token,))
        user = cur.fetchone()
        cur.close()
        conn.close()
        return dict(user) if user else None
    except:
        return None

def create_session(user_id):
    """Create session token, return token string"""
    token = secrets.token_urlsafe(48)
    expires = datetime.utcnow() + timedelta(days=30)

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sessions (user_id, token, expires_at) VALUES (%s, %s, %s)",
        (user_id, token, expires)
    )
    conn.commit()
    cur.close()
    conn.close()
    return token, expires

def require_auth(f):
    """Decorator: require logged-in user"""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({'error': 'Giriş yapmanız gerekiyor'}), 401
        request.user = user
        return f(*args, **kwargs)
    return decorated

def require_admin(f):
    """Decorator: require admin user"""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user or not user.get('is_admin'):
            return jsonify({'error': 'Yetkisiz erişim'}), 403
        request.user = user
        return f(*args, **kwargs)
    return decorated

def log_usage(user_id, action, filename=None, data_type=None, file_size=None):
    """Log user action"""
    if not DATABASE_URL:
        return
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO usage_logs (user_id, action, filename, data_type, file_size, ip_address, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (user_id, action, filename, data_type, file_size,
              request.remote_addr, str(request.user_agent)[:200]))
        conn.commit()
        cur.close()
        conn.close()
    except:
        pass

def check_upload_limit(user):
    """Check if user can upload (free plan limit)"""
    if not user or not DATABASE_URL:
        return True  # No auth = no limit (for now)

    if user['plan'] == 'pro':
        return True

    # Check trial
    if user['plan'] == 'trial':
        if user.get('trial_start'):
            trial_end = user['trial_start'] + timedelta(days=TRIAL_DAYS)
            if datetime.utcnow() < trial_end:
                return True
        # Trial expired, switch to free
        conn = get_db()
        cur = conn.cursor()
        cur.execute("UPDATE users SET plan='free' WHERE id=%s", (user['id'],))
        conn.commit()
        cur.close()
        conn.close()

    # Free plan: check monthly uploads
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) as cnt FROM usage_logs
        WHERE user_id = %s AND action = 'upload'
        AND created_at > date_trunc('month', NOW())
    """, (user['id'],))
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result['cnt'] < FREE_MONTHLY_UPLOADS

def send_magic_link_email(email, token):
    """Send magic link via SMTP"""
    if not SMTP_EMAIL or not SMTP_PASSWORD:
        print(f"[MAIL] SMTP not configured. Magic link: {APP_URL}/auth/verify?token={token}")
        return False

    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    link = f"{APP_URL}/auth/verify?token={token}"
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'ExcelMind - Giriş Linkiniz'
    msg['From'] = f'ExcelMind <{SMTP_EMAIL}>'
    msg['To'] = email

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:500px;margin:0 auto;padding:40px 20px">
        <div style="text-align:center;margin-bottom:30px">
            <h1 style="color:#1a5276;font-size:28px;margin:0">Excel<span style="color:#27AE60">Mind</span></h1>
        </div>
        <div style="background:#f8fafc;border-radius:12px;padding:30px;text-align:center">
            <h2 style="color:#1a2332;font-size:20px;margin-bottom:12px">Giriş yapın</h2>
            <p style="color:#6b7c93;font-size:14px;margin-bottom:24px">
                Aşağıdaki butona tıklayarak ExcelMind hesabınıza giriş yapabilirsiniz.
            </p>
            <a href="{link}" style="display:inline-block;background:linear-gradient(135deg,#27AE60,#2ecc71);
               color:#fff;padding:14px 40px;border-radius:10px;text-decoration:none;font-weight:700;font-size:16px">
                Giriş Yap
            </a>
            <p style="color:#8899aa;font-size:12px;margin-top:20px">
                Bu link 15 dakika geçerlidir. Eğer bu isteği siz yapmadıysanız, bu e-postayı görmezden gelebilirsiniz.
            </p>
        </div>
    </div>
    """

    msg.attach(MIMEText(html, 'html'))

    try:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        server.starttls()
        server.login(SMTP_EMAIL, SMTP_PASSWORD)
        server.sendmail(SMTP_EMAIL, email, msg.as_string())
        server.quit()
        print(f"[MAIL] Magic link sent to {email}")
        return True
    except Exception as e:
        print(f"[MAIL] Error: {e}")
        return False

# ============================================================
# CLAUDE API
# ============================================================
def call_claude_api(prompt, max_tokens=2000):
    """Call Anthropic Claude API"""
    if not ANTHROPIC_API_KEY:
        return None

    url = "https://api.anthropic.com/v1/messages"
    payload = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}]
    }).encode('utf-8')

    headers_dict = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01"
    }

    try:
        req = urllib.request.Request(url, data=payload, headers=headers_dict, method='POST')
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            if 'content' in result and len(result['content']) > 0:
                return result['content'][0].get('text', '')
    except Exception as e:
        print(f"[AI] API error: {e}")
    return None

# ============================================================
# ROUTES: Static Files
# ============================================================
@app.route('/')
@app.route('/index.html')
def serve_index():
    return send_from_directory(BASE_DIR, 'index.html')

@app.route('/admin')
@app.route('/admin/')
def serve_admin():
    return send_from_directory(BASE_DIR, 'admin.html')

@app.route('/download/<filename>')
def download_file(filename):
    filepath = os.path.join(OUTPUT_DIR, filename)
    if os.path.exists(filepath):
        return send_file(filepath, as_attachment=True, download_name=filename)
    return jsonify({'error': 'File not found'}), 404

# ============================================================
# ROUTES: Auth
# ============================================================
@app.route('/api/auth/magic-link', methods=['POST'])
def auth_magic_link():
    """Send magic link to email"""
    if not DATABASE_URL:
        return jsonify({'error': 'Database not configured'}), 500

    data = request.get_json()
    email = (data.get('email') or '').strip().lower()

    if not email or '@' not in email:
        return jsonify({'error': 'Geçerli bir e-posta adresi girin'}), 400

    # Create or get user
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM users WHERE email = %s", (email,))
    user = cur.fetchone()

    if not user:
        is_admin = email in [e.strip().lower() for e in ADMIN_EMAILS]
        cur.execute("""
            INSERT INTO users (email, plan, trial_start, is_admin)
            VALUES (%s, 'trial', NOW(), %s)
            RETURNING *
        """, (email, is_admin))
        user = cur.fetchone()
        conn.commit()
        print(f"[AUTH] New user: {email} (admin={is_admin})")

    # Create magic link token
    token = secrets.token_urlsafe(48)
    expires = datetime.utcnow() + timedelta(minutes=15)
    cur.execute(
        "INSERT INTO magic_links (email, token, expires_at) VALUES (%s, %s, %s)",
        (email, token, expires)
    )
    conn.commit()
    cur.close()
    conn.close()

    # Send email
    sent = send_magic_link_email(email, token)

    return jsonify({
        'success': True,
        'message': 'Giriş linki e-posta adresinize gönderildi' if sent else 'E-posta gönderilemedi, konsol loglarını kontrol edin',
        'email': email
    })

@app.route('/auth/verify')
def auth_verify():
    """Verify magic link token"""
    token = request.args.get('token')
    if not token or not DATABASE_URL:
        return redirect('/?auth=error')

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT * FROM magic_links
        WHERE token = %s AND used = FALSE AND expires_at > NOW()
    """, (token,))
    link = cur.fetchone()

    if not link:
        cur.close()
        conn.close()
        return redirect('/?auth=expired')

    # Mark used
    cur.execute("UPDATE magic_links SET used = TRUE WHERE id = %s", (link['id'],))

    # Get user
    cur.execute("SELECT * FROM users WHERE email = %s", (link['email'],))
    user = cur.fetchone()

    if not user:
        cur.close()
        conn.close()
        return redirect('/?auth=error')

    # Update last login
    cur.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (user['id'],))
    conn.commit()

    # Create session
    session_token, expires = create_session(user['id'])

    cur.close()
    conn.close()

    # Set cookie and redirect
    resp = make_response(redirect('/?auth=success'))
    resp.set_cookie('session_token', session_token,
                     expires=expires, httponly=True, secure=True, samesite='Lax')
    return resp

@app.route('/api/auth/google', methods=['POST'])
def auth_google():
    """Google OAuth: verify ID token and create session"""
    if not GOOGLE_CLIENT_ID or not DATABASE_URL:
        return jsonify({'error': 'Google login not configured'}), 500

    data = request.get_json()
    credential = data.get('credential')
    if not credential:
        return jsonify({'error': 'No credential'}), 400

    # Verify Google ID token
    try:
        verify_url = f"https://oauth2.googleapis.com/tokeninfo?id_token={credential}"
        req = urllib.request.Request(verify_url)
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
            google_data = json.loads(resp.read().decode('utf-8'))

        email = google_data.get('email', '').lower()
        name = google_data.get('name', '')
        avatar = google_data.get('picture', '')

        if not email:
            return jsonify({'error': 'Google hesabından e-posta alınamadı'}), 400

    except Exception as e:
        print(f"[AUTH] Google verify error: {e}")
        return jsonify({'error': 'Google doğrulama başarısız'}), 400

    # Create or update user
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM users WHERE email = %s", (email,))
    user = cur.fetchone()

    if not user:
        is_admin = email in [e.strip().lower() for e in ADMIN_EMAILS]
        cur.execute("""
            INSERT INTO users (email, name, avatar_url, plan, trial_start, is_admin)
            VALUES (%s, %s, %s, 'trial', NOW(), %s)
            RETURNING *
        """, (email, name, avatar, is_admin))
        user = cur.fetchone()
        conn.commit()
    else:
        cur.execute("""
            UPDATE users SET name=%s, avatar_url=%s, last_login=NOW() WHERE id=%s
        """, (name, avatar, user['id']))
        conn.commit()

    # Create session
    session_token, expires = create_session(user['id'])

    cur.close()
    conn.close()

    resp = make_response(jsonify({
        'success': True,
        'user': {
            'email': email,
            'name': name,
            'avatar': avatar,
            'plan': user['plan']
        }
    }))
    resp.set_cookie('session_token', session_token,
                     expires=expires, httponly=True, secure=True, samesite='Lax')
    return resp

@app.route('/api/auth/me')
def auth_me():
    """Get current user info"""
    user = get_current_user()
    if not user:
        return jsonify({'logged_in': False})

    # Check trial status
    plan = user['plan']
    days_left = None
    if plan == 'trial' and user.get('trial_start'):
        trial_end = user['trial_start'] + timedelta(days=TRIAL_DAYS)
        if datetime.utcnow() > trial_end:
            plan = 'free'
            # Update DB
            try:
                conn = get_db()
                cur = conn.cursor()
                cur.execute("UPDATE users SET plan='free' WHERE id=%s", (user['id'],))
                conn.commit()
                cur.close()
                conn.close()
            except:
                pass
        else:
            days_left = (trial_end - datetime.utcnow()).days

    # Monthly upload count
    upload_count = 0
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) as cnt FROM usage_logs
            WHERE user_id = %s AND action = 'upload'
            AND created_at > date_trunc('month', NOW())
        """, (user['id'],))
        result = cur.fetchone()
        upload_count = result['cnt']
        cur.close()
        conn.close()
    except:
        pass

    return jsonify({
        'logged_in': True,
        'user': {
            'id': str(user['id']),
            'email': user['email'],
            'name': user.get('name') or user['email'].split('@')[0],
            'avatar': user.get('avatar_url'),
            'plan': plan,
            'is_admin': user.get('is_admin', False),
            'days_left': days_left,
            'uploads_this_month': upload_count,
            'upload_limit': None if plan in ('pro', 'trial') else FREE_MONTHLY_UPLOADS,
            'created_at': str(user['created_at'])
        }
    })

@app.route('/api/auth/logout', methods=['POST'])
def auth_logout():
    """Logout: delete session"""
    token = request.cookies.get('session_token')
    if token and DATABASE_URL:
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("DELETE FROM sessions WHERE token = %s", (token,))
            conn.commit()
            cur.close()
            conn.close()
        except:
            pass

    resp = make_response(jsonify({'success': True}))
    resp.delete_cookie('session_token')
    return resp

# ============================================================
# ROUTES: Excel API (existing functionality)
# ============================================================
@app.route('/api/analyze', methods=['POST'])
def api_analyze():
    """Upload Excel, get JSON analysis"""
    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file uploaded'}), 400

    data_type = request.form.get('type', 'auto')
    ext = os.path.splitext(file.filename)[1] or '.xlsx'
    tmp_path = os.path.join(UPLOAD_DIR, f"tmp_{uuid.uuid4().hex}{ext}")

    try:
        file.save(tmp_path)

        # Log usage
        user = get_current_user()
        if user:
            log_usage(user['id'], 'upload', file.filename, data_type, os.path.getsize(tmp_path))

        result = analyze_for_dashboard(tmp_path, data_type)
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

@app.route('/api/generate', methods=['POST'])
def api_generate():
    """Upload Excel, get Smart Excel back"""
    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file uploaded'}), 400

    data_type = request.form.get('type', 'auto')
    file_id = uuid.uuid4().hex
    ext = os.path.splitext(file.filename)[1] or '.xlsx'
    input_path = os.path.join(UPLOAD_DIR, f"input_{file_id}{ext}")
    output_filename = f"ExcelMind_Smart_{file_id[:8]}.xlsx"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    try:
        file.save(input_path)

        # Log usage
        user = get_current_user()
        if user:
            log_usage(user['id'], 'generate', file.filename, data_type, os.path.getsize(input_path))

        stats = build_smart_excel(input_path, output_path)
        return jsonify({
            'success': True,
            'download_url': f'/download/{output_filename}',
            'filename': output_filename,
            'stats': stats
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': f'Generation failed: {str(e)}'}), 500
    finally:
        if os.path.exists(input_path):
            os.remove(input_path)

@app.route('/api/ai-analyze', methods=['POST'])
def api_ai_analyze():
    """AI-powered analysis via Claude"""
    if not ANTHROPIC_API_KEY:
        return jsonify({'error': 'API key not configured'}), 400

    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file uploaded'}), 400

    ext = os.path.splitext(file.filename)[1] or '.xlsx'
    tmp_path = os.path.join(UPLOAD_DIR, f"tmp_{uuid.uuid4().hex}{ext}")

    try:
        file.save(tmp_path)

        # Log usage
        user = get_current_user()
        if user:
            log_usage(user['id'], 'ai_analyze', file.filename)

        headers_list, data_rows, sheet_name = read_excel(tmp_path)

        # Prepare sample
        sample_rows = data_rows[:15]
        csv_lines = [' | '.join(str(h) for h in headers_list), '-' * 80]
        for row in sample_rows:
            csv_lines.append(' | '.join(str(v)[:30] if v is not None else '' for v in row))

        data_sample = '\n'.join(csv_lines)

        prompt = f"""Sen bir veri analiz uzmanısın. Aşağıdaki Excel verisini analiz et.

Dosya adı: {file.filename}
Sayfa adı: {sheet_name}
Toplam satır: {len(data_rows)}
Sütun sayısı: {len(headers_list)}

İlk 15 satır:
{data_sample}

Lütfen şu JSON formatında yanıt ver (SADECE JSON, başka metin yazma):
{{
  "data_type": "sales|finance|bank|hr|inventory|ecommerce|cashflow|customers|general",
  "data_type_tr": "Türkçe veri türü adı",
  "summary": "Verinin 1-2 cümlelik Türkçe özeti",
  "column_roles": {{
    "main_value": "Ana sayısal değer sütunu adı",
    "category": "Ana kategori sütunu adı",
    "date": "Tarih sütunu adı veya null",
    "filters": ["Filtre olarak kullanılabilecek sütun adları"]
  }},
  "insights": [
    "Veri hakkında önemli bulgu 1 (Türkçe)",
    "Veri hakkında önemli bulgu 2 (Türkçe)",
    "Veri hakkında önemli bulgu 3 (Türkçe)",
    "İş önerisi veya dikkat edilecek nokta (Türkçe)",
    "Trend veya pattern tespiti (Türkçe)"
  ],
  "kpi_suggestions": [
    {{"label": "KPI adı", "description": "Ne anlama geldiği"}},
    {{"label": "KPI adı", "description": "Ne anlama geldiği"}},
    {{"label": "KPI adı", "description": "Ne anlama geldiği"}},
    {{"label": "KPI adı", "description": "Ne anlama geldiği"}}
  ],
  "risk_alerts": [
    "Varsa risk veya uyarı (Türkçe)"
  ]
}}"""

        ai_result = call_claude_api(prompt)

        if ai_result:
            try:
                json_str = ai_result
                if '```json' in json_str:
                    json_str = json_str.split('```json')[1].split('```')[0]
                elif '```' in json_str:
                    json_str = json_str.split('```')[1].split('```')[0]
                start = json_str.index('{')
                end = json_str.rindex('}') + 1
                parsed = json.loads(json_str[start:end])
                return jsonify({'success': True, 'ai_analysis': parsed})
            except (json.JSONDecodeError, ValueError):
                return jsonify({
                    'success': True,
                    'ai_analysis': {
                        'insights': [ai_result[:500]],
                        'data_type': 'general',
                        'summary': ai_result[:200]
                    }
                })
        else:
            return jsonify({'error': 'Claude API call failed'}), 500

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': f'AI analysis failed: {str(e)}'}), 500
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

# ============================================================
# ROUTES: Page View Tracking
# ============================================================
@app.route('/api/track', methods=['POST'])
def track_pageview():
    """Track page view"""
    if not DATABASE_URL:
        return jsonify({'ok': True})

    try:
        data = request.get_json() or {}
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO page_views (path, ip_address, user_agent, referrer)
            VALUES (%s, %s, %s, %s)
        """, (data.get('path', '/'), request.remote_addr,
              str(request.user_agent)[:200], request.referrer or ''))
        conn.commit()
        cur.close()
        conn.close()
    except:
        pass
    return jsonify({'ok': True})

# ============================================================
# ROUTES: Admin API
# ============================================================
@app.route('/api/admin/stats')
@require_admin
def admin_stats():
    """Admin dashboard stats"""
    conn = get_db()
    cur = conn.cursor()

    # User counts
    cur.execute("SELECT COUNT(*) as total FROM users")
    total_users = cur.fetchone()['total']

    cur.execute("SELECT COUNT(*) as cnt FROM users WHERE plan='pro'")
    pro_users = cur.fetchone()['cnt']

    cur.execute("SELECT COUNT(*) as cnt FROM users WHERE plan='trial'")
    trial_users = cur.fetchone()['cnt']

    cur.execute("SELECT COUNT(*) as cnt FROM users WHERE plan='free'")
    free_users = cur.fetchone()['cnt']

    # Today's stats
    cur.execute("SELECT COUNT(*) as cnt FROM users WHERE created_at::date = CURRENT_DATE")
    new_today = cur.fetchone()['cnt']

    cur.execute("SELECT COUNT(*) as cnt FROM usage_logs WHERE created_at::date = CURRENT_DATE")
    actions_today = cur.fetchone()['cnt']

    cur.execute("SELECT COUNT(*) as cnt FROM page_views WHERE created_at::date = CURRENT_DATE")
    views_today = cur.fetchone()['cnt']

    # This month uploads
    cur.execute("""
        SELECT COUNT(*) as cnt FROM usage_logs
        WHERE action='upload' AND created_at > date_trunc('month', NOW())
    """)
    uploads_month = cur.fetchone()['cnt']

    # AI analysis count
    cur.execute("""
        SELECT COUNT(*) as cnt FROM usage_logs
        WHERE action='ai_analyze' AND created_at > date_trunc('month', NOW())
    """)
    ai_month = cur.fetchone()['cnt']

    # Top data types
    cur.execute("""
        SELECT data_type, COUNT(*) as cnt FROM usage_logs
        WHERE data_type IS NOT NULL AND created_at > date_trunc('month', NOW())
        GROUP BY data_type ORDER BY cnt DESC LIMIT 5
    """)
    top_types = [dict(r) for r in cur.fetchall()]

    # Daily views (last 30 days)
    cur.execute("""
        SELECT created_at::date as day, COUNT(*) as cnt
        FROM page_views
        WHERE created_at > NOW() - INTERVAL '30 days'
        GROUP BY day ORDER BY day
    """)
    daily_views = [{'day': str(r['day']), 'count': r['cnt']} for r in cur.fetchall()]

    # Daily signups (last 30 days)
    cur.execute("""
        SELECT created_at::date as day, COUNT(*) as cnt
        FROM users
        WHERE created_at > NOW() - INTERVAL '30 days'
        GROUP BY day ORDER BY day
    """)
    daily_signups = [{'day': str(r['day']), 'count': r['cnt']} for r in cur.fetchall()]

    # Revenue stats
    cur.execute("""
        SELECT COALESCE(SUM(amount), 0) as total
        FROM payments WHERE status='completed'
    """)
    total_revenue = float(cur.fetchone()['total'])

    cur.execute("""
        SELECT COALESCE(SUM(amount), 0) as total
        FROM payments WHERE status='completed' AND created_at > date_trunc('month', NOW())
    """)
    month_revenue = float(cur.fetchone()['total'])

    cur.execute("""
        SELECT COALESCE(SUM(amount), 0) as total
        FROM payments WHERE status='completed' AND created_at::date = CURRENT_DATE
    """)
    today_revenue = float(cur.fetchone()['total'])

    cur.execute("""
        SELECT COUNT(*) as cnt FROM payments WHERE status='completed'
    """)
    total_payments = cur.fetchone()['cnt']

    # Monthly revenue trend (last 6 months)
    cur.execute("""
        SELECT to_char(created_at, 'YYYY-MM') as month,
               SUM(amount) as revenue, COUNT(*) as count
        FROM payments WHERE status='completed'
        AND created_at > NOW() - INTERVAL '6 months'
        GROUP BY month ORDER BY month
    """)
    monthly_revenue = [{'month': r['month'], 'total': float(r['revenue']), 'count': r['count']} for r in cur.fetchall()]

    # Revenue by plan type
    cur.execute("""
        SELECT plan, COUNT(*) as count, SUM(amount) as revenue
        FROM payments WHERE status='completed'
        AND created_at > date_trunc('month', NOW())
        GROUP BY plan ORDER BY revenue DESC
    """)
    revenue_by_plan = [{'plan': r['plan'], 'count': r['count'], 'total': float(r['revenue'])} for r in cur.fetchall()]

    # Revenue by payment type
    cur.execute("""
        SELECT payment_type, COUNT(*) as count, SUM(amount) as revenue
        FROM payments WHERE status='completed'
        AND created_at > date_trunc('month', NOW())
        GROUP BY payment_type ORDER BY revenue DESC
    """)
    revenue_by_type = [{'type': r['payment_type'], 'count': r['count'], 'total': float(r['revenue'])} for r in cur.fetchall()]

    # Trial to Pro conversion rate
    cur.execute("SELECT COUNT(*) as cnt FROM users WHERE trial_used = TRUE")
    trial_used = cur.fetchone()['cnt']
    cur.execute("SELECT COUNT(*) as cnt FROM users WHERE plan = 'pro'")
    pro_converted = cur.fetchone()['cnt']
    conversion_rate = (pro_converted / trial_used * 100) if trial_used > 0 else 0

    cur.close()
    conn.close()

    return jsonify({
        'users': {
            'total': total_users, 'pro': pro_users,
            'trial': trial_users, 'free': free_users,
            'new_today': new_today
        },
        'activity': {
            'actions_today': actions_today,
            'views_today': views_today,
            'uploads_month': uploads_month,
            'ai_analyses_month': ai_month
        },
        'revenue': {
            'total_revenue': total_revenue,
            'month_revenue': month_revenue,
            'today_revenue': today_revenue,
            'total_payments': total_payments,
            'conversion_rate': round(conversion_rate, 1),
            'monthly_trend': monthly_revenue,
            'by_plan': revenue_by_plan,
            'by_type': revenue_by_type
        },
        'top_data_types': top_types,
        'daily_views': daily_views,
        'daily_signups': daily_signups
    })

@app.route('/api/admin/users')
@require_admin
def admin_users():
    """List all users"""
    conn = get_db()
    cur = conn.cursor()

    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    offset = (page - 1) * per_page

    cur.execute("SELECT COUNT(*) as total FROM users")
    total = cur.fetchone()['total']

    cur.execute("""
        SELECT u.*,
            (SELECT COUNT(*) FROM usage_logs WHERE user_id=u.id AND action='upload') as upload_count,
            (SELECT COUNT(*) FROM usage_logs WHERE user_id=u.id AND action='ai_analyze') as ai_count
        FROM users u
        ORDER BY u.created_at DESC
        LIMIT %s OFFSET %s
    """, (per_page, offset))

    users = []
    for row in cur.fetchall():
        u = dict(row)
        u['id'] = str(u['id'])
        u['created_at'] = str(u['created_at'])
        u['last_login'] = str(u['last_login'])
        if u.get('trial_start'):
            u['trial_start'] = str(u['trial_start'])
        users.append(u)

    cur.close()
    conn.close()

    return jsonify({'users': users, 'total': total, 'page': page, 'per_page': per_page})

@app.route('/api/admin/usage')
@require_admin
def admin_usage():
    """Recent usage logs"""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT ul.*, u.email
        FROM usage_logs ul
        LEFT JOIN users u ON u.id = ul.user_id
        ORDER BY ul.created_at DESC
        LIMIT 100
    """)
    logs = []
    for row in cur.fetchall():
        r = dict(row)
        r['id'] = str(r['id'])
        r['user_id'] = str(r['user_id']) if r['user_id'] else None
        r['created_at'] = str(r['created_at'])
        logs.append(r)

    cur.close()
    conn.close()

    return jsonify({'logs': logs})

@app.route('/api/admin/user/<user_id>/update', methods=['POST'])
@require_admin
def admin_update_user(user_id):
    """Admin: update user plan or status"""
    data = request.get_json()
    plan = data.get('plan')
    is_admin = data.get('is_admin')

    conn = get_db()
    cur = conn.cursor()

    if plan:
        cur.execute("UPDATE users SET plan=%s WHERE id=%s", (plan, user_id))
        if plan == 'trial':
            cur.execute("UPDATE users SET trial_start=NOW() WHERE id=%s", (user_id,))

    if is_admin is not None:
        cur.execute("UPDATE users SET is_admin=%s WHERE id=%s", (is_admin, user_id))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({'success': True})

@app.route('/api/admin/payments')
@require_admin
def admin_payments():
    """List all payments"""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT p.*, u.email, u.name
        FROM payments p
        LEFT JOIN users u ON u.id = p.user_id
        ORDER BY p.created_at DESC
        LIMIT 200
    """)
    payments = []
    for row in cur.fetchall():
        r = dict(row)
        r['id'] = str(r['id'])
        r['user_id'] = str(r['user_id']) if r['user_id'] else None
        r['amount'] = float(r['amount'])
        r['created_at'] = str(r['created_at'])
        r['period_start'] = str(r['period_start']) if r.get('period_start') else None
        r['period_end'] = str(r['period_end']) if r.get('period_end') else None
        payments.append(r)

    cur.close()
    conn.close()
    return jsonify({'payments': payments})

@app.route('/api/admin/payments/add', methods=['POST'])
@require_admin
def admin_add_payment():
    """Manually add a payment (for bank transfers, manual upgrades, etc.)"""
    data = request.get_json()
    email = data.get('email', '').strip().lower()
    amount = float(data.get('amount', 0))
    plan = data.get('plan', 'pro')
    payment_type = data.get('payment_type', 'manual')
    notes = data.get('notes', '')
    currency = data.get('currency', 'TRY')

    if not email or amount <= 0:
        return jsonify({'error': 'Email ve tutar gerekli'}), 400

    conn = get_db()
    cur = conn.cursor()

    # Find user
    cur.execute("SELECT * FROM users WHERE email = %s", (email,))
    user = cur.fetchone()

    user_id = user['id'] if user else None

    # Record payment
    cur.execute("""
        INSERT INTO payments (user_id, email, amount, currency, plan, payment_type,
                              payment_provider, status, period_start, period_end, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'completed', NOW(), NOW() + INTERVAL '30 days', %s)
        RETURNING id
    """, (user_id, email, amount, currency, plan, payment_type, 'manual', notes))

    payment_id = cur.fetchone()['id']

    # Upgrade user plan if exists
    if user:
        cur.execute("UPDATE users SET plan=%s WHERE id=%s", (plan, user_id))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({'success': True, 'payment_id': str(payment_id)})

# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    init_db()

    print(f"""
╔══════════════════════════════════════════════════╗
║         ExcelMind Backend (Flask)                ║
║         http://0.0.0.0:{PORT}                      ║
╠══════════════════════════════════════════════════╣
║  Auth: Google OAuth + Magic Link                 ║
║  DB: PostgreSQL                                  ║
║  Admin: /admin                                   ║
╚══════════════════════════════════════════════════╝
    """)

    app.run(host='0.0.0.0', port=PORT, debug=False)
