#!/usr/bin/env python3
"""
Links to Notes for Obsidian v2.0
Convierte URLs desde CSV a notas Markdown listas para Obsidian

CAMBIOS v2.0:
- Eliminado tag "bookmark" automático
- Campo tags vacío cuando no hay tags en CSV (en lugar de [])
"""

from __future__ import annotations
import argparse
import csv
import io
import json
import re
import sys
import time
import zipfile
import os
from pathlib import Path
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from readability import Document
from slugify import slugify
import dateparser
from jinja2 import Template
import html2text

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

MAX_FILENAME_LENGTH = 200
MAX_TITLE_LENGTH = 100

_session = requests.Session()
_retry = Retry(total=3, backoff_factor=0.6, status_forcelist=[429, 500, 502, 503, 504])
_session.mount("http://", HTTPAdapter(max_retries=_retry))
_session.mount("https://", HTTPAdapter(max_retries=_retry))

def fetch_url(url: str, timeout: int = 25) -> tuple[str, requests.Response]:
    """Obtiene contenido de una URL con headers realistas"""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://www.google.com/",
        "Cache-Control": "max-age=0",
    }
    resp = _session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    if resp.encoding:
        resp.encoding = resp.apparent_encoding
    return resp.text, resp

def extract_meta(html: str, url: str) -> dict:
    """Extrae metadata de la página HTML"""
    soup = BeautifulSoup(html, "lxml")
    
    # URL canónica
    canonical = None
    link_tag = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
    if link_tag and link_tag.get("href"):
        canonical = link_tag["href"].strip()
    og_url = soup.find("meta", property="og:url")
    if og_url and og_url.get("content"):
        canonical = og_url["content"].strip() or canonical

    # Título
    title = None
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        title = og_title["content"].strip()
    if not title and soup.title and soup.title.string:
        title = soup.title.string.strip()

    # Autor y fecha (JSON-LD primero)
    author = None
    published_date = None
    for ld in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(ld.string)
            candidates = data if isinstance(data, list) else [data]
            for d in candidates:
                if not isinstance(d, dict):
                    continue
                a = d.get("author")
                if a and not author:
                    if isinstance(a, dict) and a.get("name"):
                        author = str(a["name"]).strip()
                    elif isinstance(a, list) and a:
                        if isinstance(a[0], dict) and a[0].get("name"):
                            author = str(a[0]["name"]).strip()
                if not published_date and d.get("datePublished"):
                    published_date = str(d["datePublished"]).strip()
        except:
            pass

    # Fallback para autor
    if not author:
        for key in [{"name": "author"}, {"property": "article:author"}]:
            m = soup.find("meta", attrs=key)
            if m and m.get("content"):
                author = m["content"].strip()
                break

    # Fallback para fecha
    if not published_date:
        for key in [{"property": "article:published_time"}, {"name": "date"}]:
            m = soup.find("meta", attrs=key)
            if m and m.get("content"):
                published_date = m["content"].strip()
                break

    # Limpiar autor
    if author:
        author = re.sub(r'\s+', ' ', author).strip()

    # Normalizar fecha
    published_dt = dateparser.parse(published_date) if published_date else None
    published_date_norm = published_dt.date().isoformat() if published_dt else None

    # Descripción
    description = None
    for key in [{"name": "description"}, {"property": "og:description"}]:
        m = soup.find("meta", attrs=key)
        if m and m.get("content"):
            description = m["content"].strip()
            break

    # Extraer contenido legible
    doc = Document(html)
    content_html = doc.summary(html_partial=True)
    content_text = BeautifulSoup(content_html, "lxml").get_text("\n")

    # Estadísticas
    words = re.findall(r"\w+", content_text)
    word_count = len(words)
    reading_time_min = max(1, round(word_count / 225))

    # Convertir a Markdown
    h = html2text.HTML2Text()
    h.ignore_images = True
    h.body_width = 0
    content_md = h.handle(content_html)

    meta = {
        "title": title or url,
        "author": author,
        "published_date": published_date_norm,
        "summary": description,
        "source_url": canonical or url,
        "word_count": word_count,
        "reading_time_min": reading_time_min,
        "tags": [],  # v2.0: Sin tag "bookmark" por defecto
    }
    return {"meta": meta, "content_md": content_md}

def try_wayback_machine(url: str) -> Optional[tuple[str, str]]:
    """Intenta recuperar contenido de Archive.org"""
    print(f"  Intentando Archive.org...")
    try:
        api_url = f"http://archive.org/wayback/available?url={url}"
        resp = requests.get(api_url, timeout=10)
        data = resp.json()
        
        if not data.get('archived_snapshots'):
            return None
            
        closest = data['archived_snapshots'].get('closest')
        if not closest or not closest.get('available'):
            return None
        
        archived_url = closest['url']
        print(f"  Snapshot encontrado")
        
        resp = requests.get(archived_url, timeout=20)
        resp.raise_for_status()
        return resp.text, archived_url
    except:
        return None

def create_fallback_note(url: str, csv_metadata: dict) -> dict:
    """Crea nota básica cuando no se puede obtener contenido"""
    meta = {
        "title": csv_metadata.get("title", url),
        "author": None,
        "published_date": None,
        "summary": csv_metadata.get("description", ""),
        "source_url": url,
        "word_count": 0,
        "reading_time_min": 0,
        "tags": csv_metadata.get("tags", []),  # v2.0: Sin tag "bookmark" por defecto
        "status": "unavailable",
    }
    
    content_md = f"""**CONTENIDO NO DISPONIBLE**

Esta URL no pudo ser accedida automaticamente.

Visita manualmente: [{url}]({url})
"""
    return {"meta": meta, "content_md": content_md}

DEFAULT_TEMPLATE = """---
title: "{{ meta.title }}"
source: "{{ meta.source_url }}"
author: {{ meta.author_wikilinks | default("") | tojson }}
published: {{ meta.published_date | default("") | tojson }}
created: {{ meta.created_date | tojson }}
description: {{ meta.summary | default("") | tojson }}
tags: {% if meta.tags %}[{% for t in meta.tags %}{{ t | tojson }}{% if not loop.last %}, {% endif %}{% endfor %}]{% endif %}
---

# {{ meta.title }}

> TL;DR
> {{ meta.summary | default("") }}

## Notes
{{ content_md }}

## Links
- {{ meta.source_url }}
"""

def normalize_tags(tags: list[str]) -> list[str]:
    """Normaliza tags: elimina duplicados y espacios (v2.0: sin agregar 'bookmark')"""
    uniq = []
    seen = set()
    for t in tags:
        t = t.strip()
        if not t:
            continue
        if t not in seen:
            uniq.append(t)
            seen.add(t)
    return uniq

def decide_out_path(out_dir: Path, meta: dict) -> Path:
    """Genera ruta de salida organizando por fecha"""
    ref_iso = meta.get("published_date") or datetime.utcnow().isoformat()
    dt = dateparser.parse(ref_iso) or datetime.utcnow()
    folder = out_dir / dt.strftime("%Y") / dt.strftime("%m")
    folder.mkdir(parents=True, exist_ok=True)

    title = meta.get("title") or "nota"
    if len(title) > MAX_TITLE_LENGTH:
        title = title[:MAX_TITLE_LENGTH]
    
    base = slugify(title)
    if len(base) > MAX_FILENAME_LENGTH:
        base = base[:MAX_FILENAME_LENGTH]
    if not base or base == "-":
        base = "nota"
    
    out = folder / f"{base}.md"
    if not out.exists():
        return out
    
    # Evitar colisiones
    i = 2
    while True:
        candidate = folder / f"{base}-{i}.md"
        if not candidate.exists():
            return candidate
        i += 1

def render_markdown(meta: dict, content_md: str, template_path: Optional[str]) -> str:
    """Renderiza la nota Markdown usando la plantilla"""
    # Convertir autor a wikilinks
    author = meta.get("author") or ""
    author_wikilinks = ""
    if author:
        parts = re.split(r"[;,]", author)
        parts = [p.strip() for p in parts if p.strip()]
        if parts:
            author_wikilinks = ", ".join(f"[[{p}]]" for p in parts)
    meta["author_wikilinks"] = author_wikilinks
    meta["created_date"] = datetime.utcnow().date().isoformat()

    tpl_text = Path(template_path).read_text(encoding="utf-8") if template_path else DEFAULT_TEMPLATE
    tpl = Template(tpl_text)
    return tpl.render(meta=meta, content_md=content_md)

def process_url_with_fallbacks(url: str, tags: list[str], csv_row: dict, out_dir: Path, template_path: Optional[str], sleep_s: float) -> tuple[Optional[Path], str]:
    """Procesa URL con estrategia de fallbacks: directo -> Archive.org -> nota básica"""
    try:
        print(f"  Intento directo...")
        html, resp = fetch_url(url)
        data = extract_meta(html, resp.url)
        status = "success"
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"  403 Forbidden")
            wayback_result = try_wayback_machine(url)
            if wayback_result:
                html, archived_url = wayback_result
                data = extract_meta(html, url)
                data["meta"]["source_url"] = url
                status = "archived"
            else:
                print(f"  Creando nota basica...")
                data = create_fallback_note(url, csv_row)
                status = "fallback"
        else:
            raise
    except Exception as e:
        print(f"  Error: {e}")
        data = create_fallback_note(url, csv_row)
        status = "fallback"
    
    # Combinar tags: extraídos + CSV
    meta = data["meta"]
    csv_tags = csv_row.get("tags", [])
    all_tags = list(meta.get("tags", [])) + list(tags) + list(csv_tags)
    norm_tags = [t.strip().lower() for t in all_tags]
    meta["tags"] = normalize_tags(norm_tags)
    
    # Renderizar y guardar
    md_text = render_markdown(meta, data["content_md"], template_path)
    
    try:
        out_path = decide_out_path(out_dir, meta)
        out_path.write_text(md_text, encoding="utf-8")
    except OSError:
        # Fallback para nombres muy largos
        meta["title"] = meta["title"][:50]
        out_path = decide_out_path(out_dir, meta)
        out_path.write_text(md_text, encoding="utf-8")
    
    if sleep_s > 0:
        time.sleep(sleep_s)
    
    return out_path, status

def read_urls_from_csv_enhanced(path: Path) -> list:
    """Lee CSV con auto-detección de delimitador y parsing de tags"""
    raw = path.read_text(encoding="utf-8")
    try:
        dialect = csv.Sniffer().sniff(raw, delimiters=",;\t|:")
    except:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(raw), dialect=dialect)

    field_map = {name.lower(): name for name in (reader.fieldnames or [])}
    if "url" not in field_map:
        raise ValueError("El CSV debe tener una columna 'url'.")
    
    col_url = field_map["url"]
    col_tags = field_map.get("tags")
    col_title = field_map.get("title")
    col_desc = field_map.get("description")

    items = []
    for row in reader:
        url = (row.get(col_url) or "").strip()
        if not url:
            continue
        
        # Parsear tags (soporta JSON array o string separado)
        tags_str = (row.get(col_tags) or "") if col_tags else ""
        tags = []
        rawt = tags_str.strip()
        if rawt.startswith("[") and rawt.endswith("]"):
            try:
                arr = json.loads(rawt)
                if isinstance(arr, list):
                    tags = [str(t).strip() for t in arr if str(t).strip()]
            except:
                pass
        if not tags and rawt:
            tags = [t.strip() for t in re.split(r"[,;|]", rawt) if t.strip()]
        
        csv_metadata = {
            "title": row.get(col_title, "") if col_title else "",
            "description": row.get(col_desc, "") if col_desc else "",
            "tags": tags,
        }
        
        items.append((url, normalize_tags(tags), csv_metadata))
    
    return items

def main():
    ap = argparse.ArgumentParser(
        description="Links to Notes for Obsidian v2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python links_to_notes.py --csv bookmarks.csv --out ./notas
  python links_to_notes.py --csv bookmarks.csv --out ./notas --sleep 2
  
Cambios v2.0:
  - Sin tag "bookmark" automático
  - Campo tags vacío cuando no hay tags en CSV
        """
    )
    ap.add_argument("--csv", required=True, help="Archivo CSV con URLs")
    ap.add_argument("--out", required=True, help="Carpeta de salida")
    ap.add_argument("--sleep", type=float, default=1.0, help="Segundos entre requests (recomendado: 1-2)")
    ap.add_argument("--template", help="Plantilla Jinja2 personalizada (opcional)")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    items = read_urls_from_csv_enhanced(Path(args.csv))

    if not items:
        print("No hay URLs para procesar.")
        sys.exit(1)

    results = {"success": [], "archived": [], "fallback": [], "failed": []}
    
    print(f"\n{'='*70}")
    print(f"Links to Notes for Obsidian v2.0")
    print(f"{'='*70}")
    print(f"Procesando {len(items)} URLs...\n")
    
    for idx, (url, tags, csv_row) in enumerate(items, 1):
        print(f"\n[{idx}/{len(items)}] {url[:80]}...")
        try:
            out, status = process_url_with_fallbacks(url, tags, csv_row, out_dir, args.template, args.sleep)
            results[status].append(str(out))
            emoji = {"success": "OK", "archived": "ARCHIVE", "fallback": "BASIC", "failed": "ERROR"}
            print(f"  {emoji[status]}")
        except Exception as e:
            print(f"  ERROR: {e}")
            results["failed"].append(url)
    
    # Generar reportes
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    total_notes = len(results['success']) + len(results['archived']) + len(results['fallback'])
    
    summary_file = out_dir / f"_00_summary_{timestamp}.txt"
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write("="*70 + "\n")
        f.write("LINKS TO NOTES FOR OBSIDIAN v2.0 - REPORTE\n")
        f.write("="*70 + "\n\n")
        f.write(f"Total URLs: {len(items)}\n\n")
        f.write(f"Exito directo:      {len(results['success']):4d} ({len(results['success'])/len(items)*100:5.1f}%)\n")
        f.write(f"Desde Archive.org:  {len(results['archived']):4d} ({len(results['archived'])/len(items)*100:5.1f}%)\n")
        f.write(f"Nota basica:        {len(results['fallback']):4d} ({len(results['fallback'])/len(items)*100:5.1f}%)\n")
        f.write(f"Fallos:             {len(results['failed']):4d} ({len(results['failed'])/len(items)*100:5.1f}%)\n\n")
        f.write(f"Notas creadas: {total_notes}\n\n")
        f.write("Cambios v2.0:\n")
        f.write("- Sin tag 'bookmark' automatico\n")
        f.write("- Campo tags vacio cuando no hay tags en CSV\n")
    
    if results['fallback']:
        fallback_file = out_dir / f"_01_manual_review_{timestamp}.txt"
        with open(fallback_file, 'w', encoding='utf-8') as f:
            f.write("URLs para revision manual:\n\n")
            for path in results['fallback']:
                md_content = Path(path).read_text(encoding='utf-8')
                url_match = re.search(r'source:\s*"([^"]+)"', md_content)
                if url_match:
                    f.write(f"{url_match.group(1)}\n")
    
    if results['failed']:
        failed_file = out_dir / f"_02_failed_{timestamp}.txt"
        with open(failed_file, 'w', encoding='utf-8') as f:
            f.write("URLs con error:\n\n")
            for url in results['failed']:
                f.write(f"{url}\n")
    
    # Crear ZIP
    print("\n" + "="*70)
    print("Creando ZIP...")
    
    zip_name = f"obsidian_notes_{timestamp}.zip"
    with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        file_count = 0
        for root, dirs, files in os.walk(out_dir):
            for file in files:
                file_path = Path(root) / file
                arcname = file_path.relative_to(out_dir.parent)
                zipf.write(file_path, arcname)
                file_count += 1
    
    zip_size = Path(zip_name).stat().st_size / 1024 / 1024
    print(f"ZIP creado: {zip_name} ({zip_size:.2f} MB)")
    print(f"Archivos incluidos: {file_count}")
    
    # Descargar en Colab
    try:
        from google.colab import files as colab_files
        print("\nDescargando...")
        colab_files.download(zip_name)
        print("Descarga iniciada!")
    except:
        print(f"Archivo disponible: {zip_name}")
    
    # Resumen final
    print("\n" + "="*70)
    print("PROCESO COMPLETADO!")
    print("="*70)
    print(f"URLs procesadas: {len(items)}")
    print(f"Notas creadas: {total_notes}")
    print(f"Archivo: {zip_name}")
    print(f"\nReportes:")
    print(f"  - {summary_file.name}")
    if results['fallback']:
        print(f"  - {fallback_file.name}")
    if results['failed']:
        print(f"  - {failed_file.name}")
    print("\n" + "="*70)

if __name__ == "__main__":
    main()
