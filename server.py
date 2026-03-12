"""
ExcelMind Backend Server
========================
Endpoints:
  POST /api/analyze   → Upload Excel, get JSON analysis for dashboard
  POST /api/generate  → Upload Excel, get Smart Excel file back
  GET  /              → Serve frontend (ExcelMind_MVP.html)
  GET  /download/<id> → Download generated smart Excel

Uses Python built-in http.server (no external dependencies needed).
For production: swap to FastAPI/Flask + gunicorn.
"""

import os
import sys
import json
import uuid
import shutil
import tempfile
import traceback
import urllib.parse
from datetime import datetime, date
from http.server import HTTPServer, SimpleHTTPRequestHandler
from io import BytesIO

# Add parent dir to path for smart_excel_engine import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import openpyxl
import urllib.request
import ssl

# ============================================================
# CONFIGURATION
# ============================================================
PORT = int(os.environ.get("PORT", 8080))
HOST = "0.0.0.0"
UPLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "outputs")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
FRONTEND_DIR = os.path.dirname(os.path.abspath(__file__))

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# SMART EXCEL ENGINE (embedded for single-file deployment)
# ============================================================
from smart_excel_engine import build_smart_excel, analyze_for_dashboard, detect_data_type

# ============================================================
# MULTIPART PARSER (no external deps)
# ============================================================
def parse_multipart(body, content_type):
    """Parse multipart/form-data without cgi module (deprecated in 3.13)"""
    # Extract boundary
    boundary = None
    for part in content_type.split(';'):
        part = part.strip()
        if part.startswith('boundary='):
            boundary = part[9:].strip('"')
            break

    if not boundary:
        return {}, {}

    boundary_bytes = boundary.encode()
    delimiter = b'--' + boundary_bytes
    end_delimiter = delimiter + b'--'

    fields = {}
    files = {}

    parts = body.split(delimiter)
    for part in parts:
        if not part or part.strip() == b'' or part.strip() == b'--':
            continue
        if part.startswith(b'--'):
            continue

        # Split headers from content
        if b'\r\n\r\n' in part:
            header_section, content = part.split(b'\r\n\r\n', 1)
        elif b'\n\n' in part:
            header_section, content = part.split(b'\n\n', 1)
        else:
            continue

        # Remove trailing \r\n
        if content.endswith(b'\r\n'):
            content = content[:-2]
        elif content.endswith(b'\n'):
            content = content[:-1]

        # Parse headers
        headers_str = header_section.decode('utf-8', errors='replace')
        name = None
        filename = None
        for line in headers_str.split('\n'):
            line = line.strip()
            if 'Content-Disposition' in line:
                for item in line.split(';'):
                    item = item.strip()
                    if item.startswith('name='):
                        name = item[5:].strip('"')
                    elif item.startswith('filename='):
                        filename = item[9:].strip('"')

        if name:
            if filename:
                files[name] = {'filename': filename, 'data': content}
            else:
                fields[name] = content.decode('utf-8', errors='replace')

    return fields, files

# ============================================================
# REQUEST HANDLER
# ============================================================
class ExcelMindHandler(SimpleHTTPRequestHandler):

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        # Serve frontend
        if path == '/' or path == '/index.html':
            self.serve_file(os.path.join(FRONTEND_DIR, 'index.html'), 'text/html')
            return

        # Download generated files
        if path.startswith('/download/'):
            file_id = path.split('/download/')[1]
            file_path = os.path.join(OUTPUT_DIR, file_id)
            if os.path.exists(file_path):
                self.send_response(200)
                self.send_header('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                self.send_header('Content-Disposition', f'attachment; filename="{file_id}"')
                self.send_header('Content-Length', str(os.path.getsize(file_path)))
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                with open(file_path, 'rb') as f:
                    self.wfile.write(f.read())
            else:
                self.send_error(404, 'File not found')
            return

        # Static files
        if path.startswith('/static/'):
            filepath = os.path.join(FRONTEND_DIR, path[1:])
            if os.path.exists(filepath):
                self.serve_file(filepath)
            else:
                self.send_error(404)
            return

        self.send_error(404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        content_length = int(self.headers.get('Content-Length', 0))
        content_type = self.headers.get('Content-Type', '')
        body = self.rfile.read(content_length)

        try:
            if path == '/api/analyze':
                self.handle_analyze(body, content_type)
            elif path == '/api/generate':
                self.handle_generate(body, content_type)
            elif path == '/api/ai-analyze':
                self.handle_ai_analyze(body, content_type)
            else:
                self.send_error(404)
        except Exception as e:
            traceback.print_exc()
            self.send_json(500, {'error': str(e)})

    def do_OPTIONS(self):
        """Handle CORS preflight"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    # --- API: Analyze uploaded Excel ---
    def handle_analyze(self, body, content_type):
        fields, files = parse_multipart(body, content_type)

        if 'file' not in files:
            self.send_json(400, {'error': 'No file uploaded'})
            return

        file_info = files['file']
        data_type = fields.get('type', 'auto')

        # Preserve original extension (.xls vs .xlsx)
        orig_name = file_info.get('filename', 'file.xlsx')
        ext = os.path.splitext(orig_name)[1] or '.xlsx'
        tmp_path = os.path.join(UPLOAD_DIR, f"tmp_{uuid.uuid4().hex}{ext}")
        try:
            with open(tmp_path, 'wb') as f:
                f.write(file_info['data'])

            # Analyze
            result = analyze_for_dashboard(tmp_path, data_type)
            self.send_json(200, result)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    # --- API: Generate Smart Excel ---
    def handle_generate(self, body, content_type):
        fields, files = parse_multipart(body, content_type)

        if 'file' not in files:
            self.send_json(400, {'error': 'No file uploaded'})
            return

        file_info = files['file']
        data_type = fields.get('type', 'auto')

        # Save uploaded file (preserve .xls extension)
        file_id = uuid.uuid4().hex
        orig_name = file_info.get('filename', 'file.xlsx')
        ext = os.path.splitext(orig_name)[1] or '.xlsx'
        input_path = os.path.join(UPLOAD_DIR, f"input_{file_id}{ext}")
        output_filename = f"ExcelMind_Smart_{file_id[:8]}.xlsx"
        output_path = os.path.join(OUTPUT_DIR, output_filename)

        try:
            with open(input_path, 'wb') as f:
                f.write(file_info['data'])

            # Generate smart Excel
            stats = build_smart_excel(input_path, output_path)

            self.send_json(200, {
                'success': True,
                'download_url': f'/download/{output_filename}',
                'filename': output_filename,
                'stats': stats
            })
        except Exception as e:
            traceback.print_exc()
            self.send_json(500, {'error': f'Generation failed: {str(e)}'})
        finally:
            if os.path.exists(input_path):
                os.remove(input_path)

    # --- API: AI-powered analysis via Claude ---
    def handle_ai_analyze(self, body, content_type):
        if not ANTHROPIC_API_KEY:
            self.send_json(400, {'error': 'API key not configured'})
            return

        fields, files = parse_multipart(body, content_type)

        if 'file' not in files:
            self.send_json(400, {'error': 'No file uploaded'})
            return

        file_info = files['file']
        orig_name = file_info.get('filename', 'file.xlsx')
        ext = os.path.splitext(orig_name)[1] or '.xlsx'
        tmp_path = os.path.join(UPLOAD_DIR, f"tmp_{uuid.uuid4().hex}{ext}")

        try:
            with open(tmp_path, 'wb') as f:
                f.write(file_info['data'])

            # Read Excel data
            from smart_excel_engine import read_excel
            headers_list, data_rows, sheet_name = read_excel(tmp_path)

            # Prepare data sample for Claude (first 15 rows)
            sample_rows = data_rows[:15]
            csv_lines = []
            csv_lines.append(' | '.join(str(h) for h in headers_list))
            csv_lines.append('-' * 80)
            for row in sample_rows:
                csv_lines.append(' | '.join(str(v)[:30] if v is not None else '' for v in row))

            data_sample = '\n'.join(csv_lines)
            total_rows = len(data_rows)

            # Build prompt
            prompt = f"""Sen bir veri analiz uzmanısın. Aşağıdaki Excel verisini analiz et.

Dosya adı: {file_info['filename']}
Sayfa adı: {sheet_name}
Toplam satır: {total_rows}
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

            # Call Claude API
            ai_result = call_claude_api(prompt)

            if ai_result:
                # Try to parse JSON from response
                try:
                    # Extract JSON from response (Claude might add extra text)
                    json_str = ai_result
                    if '```json' in json_str:
                        json_str = json_str.split('```json')[1].split('```')[0]
                    elif '```' in json_str:
                        json_str = json_str.split('```')[1].split('```')[0]

                    # Find first { and last }
                    start = json_str.index('{')
                    end = json_str.rindex('}') + 1
                    json_str = json_str[start:end]

                    parsed = json.loads(json_str)
                    self.send_json(200, {'success': True, 'ai_analysis': parsed})
                except (json.JSONDecodeError, ValueError) as e:
                    print(f"[AI] JSON parse error: {e}")
                    print(f"[AI] Raw response: {ai_result[:500]}")
                    self.send_json(200, {
                        'success': True,
                        'ai_analysis': {
                            'insights': [ai_result[:500]],
                            'data_type': 'general',
                            'summary': ai_result[:200]
                        }
                    })
            else:
                self.send_json(500, {'error': 'Claude API call failed'})

        except Exception as e:
            traceback.print_exc()
            self.send_json(500, {'error': f'AI analysis failed: {str(e)}'})
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    # --- Helpers ---
    def send_json(self, code, data):
        body = json.dumps(data, ensure_ascii=False, default=str).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def serve_file(self, filepath, content_type=None):
        if not os.path.exists(filepath):
            self.send_error(404)
            return

        if content_type is None:
            ext = os.path.splitext(filepath)[1].lower()
            types = {'.html': 'text/html', '.js': 'application/javascript',
                     '.css': 'text/css', '.json': 'application/json',
                     '.png': 'image/png', '.ico': 'image/x-icon'}
            content_type = types.get(ext, 'application/octet-stream')

        with open(filepath, 'rb') as f:
            data = f.read()

        self.send_response(200)
        self.send_header('Content-Type', content_type + '; charset=utf-8' if 'text' in content_type else content_type)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format, *args):
        """Custom log format"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {args[0]}")

# ============================================================
# MAIN
# ============================================================
# ============================================================
# CLAUDE API CALLER
# ============================================================
def call_claude_api(prompt, max_tokens=2000):
    """Call Anthropic Claude API using urllib (no external deps)"""
    if not ANTHROPIC_API_KEY:
        print("[AI] No API key configured")
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
                text = result['content'][0].get('text', '')
                print(f"[AI] Claude response: {len(text)} chars")
                return text
            else:
                print(f"[AI] Unexpected response: {result}")
                return None
    except Exception as e:
        print(f"[AI] API error: {e}")
        traceback.print_exc()
        return None


def main():
    print(f"""
╔══════════════════════════════════════════════════╗
║         ExcelMind Backend Server                 ║
║         http://{HOST}:{PORT}                       ║
╠══════════════════════════════════════════════════╣
║  POST /api/analyze   → Dashboard veri analizi    ║
║  POST /api/generate  → Akıllı Excel oluştur     ║
║  GET  /download/<id> → Dosya indir               ║
╚══════════════════════════════════════════════════╝
    """)

    server = HTTPServer((HOST, PORT), ExcelMindHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
        server.server_close()

if __name__ == '__main__':
    main()
