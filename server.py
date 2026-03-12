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

# ============================================================
# CONFIGURATION
# ============================================================
PORT = int(os.environ.get("PORT", 8080))
HOST = "0.0.0.0"
UPLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "outputs")
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

        # Save temp file
        tmp_path = os.path.join(UPLOAD_DIR, f"tmp_{uuid.uuid4().hex}.xlsx")
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

        # Save uploaded file
        file_id = uuid.uuid4().hex
        input_path = os.path.join(UPLOAD_DIR, f"input_{file_id}.xlsx")
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
