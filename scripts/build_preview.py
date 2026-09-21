#!/usr/bin/env python3
"""Build a local GitHub-style HTML preview of this repository's docs.

Scans the working tree for Markdown files (plus LICENSE / NOTICE), copies a
rendered HTML twin of each under preview/, rewrites internal .md links so the
preview is fully navigable from the browser via file://.

Markdown is rendered client-side with marked.js (loaded from jsDelivr) so the
script itself has no Python dependencies beyond the standard library.
"""
from __future__ import annotations

import html as htmllib
import os
import pathlib
import re
import shutil

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "preview"
REPO_NAME = "dblog-impl"

SKIP_DIR_PARTS = {"build", "node_modules", "preview", ".git", ".gradle", ".github", ".claude"}


def should_skip(rel: pathlib.Path) -> bool:
    return any(part.startswith(".") or part in SKIP_DIR_PARTS for part in rel.parts)


def iter_markdown_files() -> list[pathlib.Path]:
    out = []
    for p in ROOT.rglob("*.md"):
        rel = p.relative_to(ROOT)
        if should_skip(rel):
            continue
        out.append(rel)
    return sorted(out)


PLAIN_TEXT_FILES = [pathlib.Path("LICENSE"), pathlib.Path("NOTICE")]

# Label allows one level of balanced [...] so the outer link in
# `[![alt](src)](url)` (image inside link) is matched as a whole.
LINK_RE = re.compile(r"(!?)\[((?:[^\[\]]|\[[^\]]*\])*)\]\(([^)\s]+)(\s+\"[^\"]*\")?\)")
REF_LINK_RE = re.compile(r"^\s*\[([^\]]+)\]:\s*(\S+)(\s+.*)?$", re.MULTILINE)
HTML_IMG_RE = re.compile(r'(<img\b[^>]*?\bsrc=)(["\'])([^"\']+)\2', re.IGNORECASE)


IMAGE_MEDIA_EXTS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico",
    ".mp4", ".webm", ".mov", ".ogg", ".mp3",
}


def rewrite_target(target: str, current_rel: pathlib.Path, known_md: set[str]) -> str:
    """Rewrite a link target so it resolves to the generated HTML twin."""
    if not target:
        return target
    if target.startswith(("http://", "https://", "mailto:", "#", "data:", "javascript:")):
        return target
    # Image / media files resolve to the real file on disk, not a generated
    # HTML twin. The preview tree under `preview/` mirrors the repo layout,
    # so from any twin we have to (a) climb out of `preview/`, then (b) walk
    # back down to the original repo-relative path.
    path_for_ext = target.split("#", 1)[0].split("?", 1)[0]
    if pathlib.PurePosixPath(path_for_ext).suffix.lower() in IMAGE_MEDIA_EXTS:
        fragment = ""
        query = ""
        path = target
        if "#" in path:
            path, frag = path.split("#", 1)
            fragment = "#" + frag
        if "?" in path:
            path, q = path.split("?", 1)
            query = "?" + q
        base_dir = (ROOT / current_rel).parent
        try:
            resolved = (base_dir / path).resolve()
            rel_to_root = resolved.relative_to(ROOT)
        except (ValueError, OSError):
            return target
        current_html_rel = current_rel.with_suffix(".html")
        current_dir = current_html_rel.parent
        depth = 0 if str(current_dir) == "." else len(current_dir.parts)
        escape = "/".join([".."] * (depth + 1))
        return f"{escape}/{rel_to_root.as_posix()}{query}{fragment}"

    # Split off query / fragment
    fragment = ""
    query = ""
    path = target
    if "#" in path:
        path, frag = path.split("#", 1)
        fragment = "#" + frag
    if "?" in path:
        path, q = path.split("?", 1)
        query = "?" + q
    if path == "":
        # Pure fragment, keep as-is
        return target

    # Resolve relative to current file's directory, then normalise
    base_dir = (ROOT / current_rel).parent
    try:
        resolved = (base_dir / path).resolve()
        rel_to_root = resolved.relative_to(ROOT)
    except (ValueError, OSError):
        return target  # leave absolute / out-of-tree targets alone

    rel_str = rel_to_root.as_posix()
    new_rel: str | None = None

    if rel_str.endswith(".md"):
        new_rel = rel_str[:-3] + ".html"
    elif rel_to_root.name in {"LICENSE", "NOTICE"}:
        new_rel = rel_str + ".html"
    elif resolved.is_dir() or path.endswith("/"):
        readme_candidate = rel_to_root / "README.md"
        if readme_candidate.as_posix() in known_md:
            new_rel = (rel_to_root / "README.html").as_posix()

    if new_rel is None:
        # File outside the preview set (source code, scripts, archives,
        # config, etc.). Point at the actual repo file via ../-relative
        # paths: same approach as images. Browsers display plain-text
        # source files natively, so no wrapper page is needed and the
        # link is genuinely transitive (you can keep navigating the
        # repo through file:// without 404ing on a missing twin).
        if resolved.exists() and resolved.is_file():
            current_html_rel = current_rel.with_suffix(".html")
            current_dir = current_html_rel.parent
            depth = 0 if str(current_dir) == "." else len(current_dir.parts)
            escape = "/".join([".."] * (depth + 1))
            return f"{escape}/{rel_str}{query}{fragment}"
        return target  # leave unresolvable links alone

    # Compute the target relative to the directory that will hold the HTML
    # twin of the current file, so the preview directory is self-contained
    # and works over file://.
    current_html_rel = current_rel.with_suffix(".html")
    current_dir = current_html_rel.parent
    target_path = pathlib.PurePosixPath(new_rel)
    rel_target = pathlib.PurePosixPath(
        os.path.relpath(target_path, current_dir if str(current_dir) != "." else "")
    )
    return rel_target.as_posix() + query + fragment


def rewrite_markdown(md: str, current_rel: pathlib.Path, known_md: set[str]) -> str:
    def inline_sub(m: re.Match) -> str:
        bang, label, target, title = m.group(1), m.group(2), m.group(3), m.group(4) or ""
        new_target = rewrite_target(target, current_rel, known_md)
        # Recurse into the label so an image nested inside a link label
        # (`[![alt](src)](url)`) also gets its src rewritten.
        new_label = LINK_RE.sub(inline_sub, label)
        return f"{bang}[{new_label}]({new_target}{title})"

    md = LINK_RE.sub(inline_sub, md)

    def ref_sub(m: re.Match) -> str:
        label, target, rest = m.group(1), m.group(2), m.group(3) or ""
        new_target = rewrite_target(target, current_rel, known_md)
        return f"[{label}]: {new_target}{rest}"

    md = REF_LINK_RE.sub(ref_sub, md)

    # Markdown passes raw HTML through verbatim, so without this pass an
    # `<img src="docs/img/foo.png">` resolves relative to preview/ instead of
    # climbing back to the repo root.
    def html_img_sub(m: re.Match) -> str:
        prefix, quote, src = m.group(1), m.group(2), m.group(3)
        new_src = rewrite_target(src, current_rel, known_md)
        return f"{prefix}{quote}{new_src}{quote}"

    md = HTML_IMG_RE.sub(html_img_sub, md)

    return md


def breadcrumb_html(rel: pathlib.Path, root_prefix: str) -> str:
    parts = list(rel.parts)
    crumbs = [f'<a href="{root_prefix}index.html">{REPO_NAME}</a>']
    accum = ""
    for i, part in enumerate(parts):
        accum = (accum + "/" + part) if accum else part
        is_last = i == len(parts) - 1
        if is_last:
            crumbs.append(f"<span>{htmllib.escape(part)}</span>")
        else:
            # Link to folder README if one exists; otherwise plain text.
            folder_readme_md = pathlib.Path(accum) / "README.md"
            if (ROOT / folder_readme_md).exists() and not should_skip(folder_readme_md):
                folder_readme = accum + "/README.html"
                crumbs.append(
                    f'<a href="{root_prefix}{folder_readme}">{htmllib.escape(part)}</a>')
            else:
                crumbs.append(f"<span>{htmllib.escape(part)}</span>")
    return " / ".join(crumbs)


PAGE_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__ · __REPO__</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/github-markdown-css@5.5.1/github-markdown-light.min.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/highlight.js@11.9.0/styles/github.min.css">
<style>
  :root { color-scheme: light; }
  * { box-sizing: border-box; }
  body { margin: 0; color: #1f2328; background: #f6f8fa;
         font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif; }
  a { color: #0969da; text-decoration: none; }
  a:hover { text-decoration: underline; }
  header.gh-bar { background: #24292f; color: #fff; padding: 12px 24px;
                  display: flex; align-items: center; justify-content: space-between;
                  gap: 16px; flex-wrap: wrap; }
  header.gh-bar .brand { display: flex; align-items: baseline; gap: 12px; }
  header.gh-bar .brand a.repo { color: #fff; font-weight: 600; font-size: 16px; }
  header.gh-bar .brand .tag { color: #9da5b0; font-size: 12px; }
  header.gh-bar nav a { color: #c9d1d9; font-size: 13px; margin-left: 16px; }
  nav.breadcrumbs { padding: 10px 24px; background: #fff;
                    border-bottom: 1px solid #d0d7de; font-size: 14px; color: #656d76; }
  nav.breadcrumbs a { color: #0969da; }
  .wrap { max-width: 1012px; margin: 24px auto; padding: 0 16px; }
  .file { background: #fff; border: 1px solid #d0d7de;
          border-radius: 6px; overflow: hidden; }
  .file-header { padding: 8px 16px; background: #f6f8fa;
                 border-bottom: 1px solid #d0d7de; font-size: 13px;
                 display: flex; justify-content: space-between; align-items: center; }
  .file-header .meta { color: #656d76; font-size: 12px; }
  article.markdown-body { padding: 32px 48px; background: #fff; max-width: none; }
  article.markdown-body pre { background: #f6f8fa; }
  .plain-text { padding: 16px 24px; margin: 0;
                white-space: pre-wrap; word-break: break-word;
                font-family: ui-monospace,SFMono-Regular,"SF Mono",Menlo,monospace;
                font-size: 12.5px; line-height: 1.5; background: #fff; }
  footer.tiny { text-align: center; padding: 24px; color: #656d76; font-size: 12px; }
  @media (max-width: 767px) { article.markdown-body { padding: 16px; } }
  /* Slight tweaks so GitHub-markdown-css feels at home inside the framed card */
  article.markdown-body > :first-child { margin-top: 0; }
</style>
</head>
<body>
  <header class="gh-bar">
    <div class="brand">
      <a href="__ROOT_PREFIX__index.html" class="repo">__REPO__</a>
      <span class="tag">· local GitHub-style preview</span>
    </div>
    <nav>
      <a href="__ROOT_PREFIX__index.html">README</a>
      <a href="__ROOT_PREFIX__docs/PAPER_MAP.html">Paper map</a>
      <a href="__ROOT_PREFIX__docs/OPERATION.html">Operation</a>
      <a href="__ROOT_PREFIX__docs/CONTROL_PLANE.html">Control plane</a>
      <a href="__ROOT_PREFIX__AGENTS.html">AGENTS</a>
      <a href="__ROOT_PREFIX__all-docs.html">All docs</a>
    </nav>
  </header>
  <nav class="breadcrumbs">__BREADCRUMBS__</nav>
  <main class="wrap">
    <div class="file">
      <div class="file-header">
        <span>__FILENAME__</span>
        <span class="meta">__META__</span>
      </div>
      __CONTENT__
    </div>
  </main>
  <footer class="tiny">Rendered locally from __SRC__, derived from the repository's source tree.</footer>
  __SCRIPTS__
</body>
</html>
"""


MD_SCRIPTS = r"""
<script src="https://cdn.jsdelivr.net/npm/marked@12.0.2/marked.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/highlight.js@11.9.0/lib/common.min.js"></script>
<script>
  const src = document.getElementById('md-source').textContent;
  marked.use({
    gfm: true,
    breaks: false,
    headerIds: true,
    mangle: false
  });
  const renderer = new marked.Renderer();
  const origCode = renderer.code.bind(renderer);
  renderer.code = function(code, lang) {
    if (lang && window.hljs && hljs.getLanguage(lang)) {
      try {
        return '<pre><code class="hljs language-' + lang + '">' +
               hljs.highlight(code, { language: lang, ignoreIllegals: true }).value +
               '</code></pre>';
      } catch (e) {}
    }
    return origCode(code, lang);
  };
  document.getElementById('md-content').innerHTML = marked.parse(src, { renderer });
  // Auto-add id anchors for headings so ToC-style in-doc links keep working.
  document.querySelectorAll('#md-content h1, #md-content h2, #md-content h3, #md-content h4, #md-content h5, #md-content h6').forEach(h => {
    if (!h.id) {
      h.id = h.textContent.trim().toLowerCase()
        .replace(/[^a-z0-9\s-]/g, '')
        .replace(/\s+/g, '-');
    }
  });
</script>
"""


def root_prefix_for(rel_html: pathlib.Path) -> str:
    depth = len(rel_html.parts) - 1
    return "../" * depth


def escape_for_script(md: str) -> str:
    # The markdown is embedded in <script type="text/markdown">…</script>.
    # Only a literal closing tag can break out; neutralise it.
    return md.replace("</script>", "<\\/script>")


def render_markdown_page(rel_md: pathlib.Path, all_md: set[str]) -> None:
    src_path = ROOT / rel_md
    md = src_path.read_text(encoding="utf-8")
    md = rewrite_markdown(md, rel_md, all_md)
    out_rel = rel_md.with_suffix(".html")
    out_path = OUT / out_rel
    out_path.parent.mkdir(parents=True, exist_ok=True)
    root_prefix = root_prefix_for(out_rel)

    content_block = (
        '<article class="markdown-body" id="md-content"></article>\n'
        f'<script type="text/markdown" id="md-source">{escape_for_script(md)}</script>'
    )

    html_page = (
        PAGE_TEMPLATE
        .replace("__TITLE__", htmllib.escape(rel_md.name))
        .replace("__REPO__", htmllib.escape(REPO_NAME))
        .replace("__ROOT_PREFIX__", root_prefix)
        .replace("__BREADCRUMBS__", breadcrumb_html(rel_md, root_prefix))
        .replace("__FILENAME__", htmllib.escape(rel_md.as_posix()))
        .replace("__META__", f"{len(md):,} chars · Markdown")
        .replace("__CONTENT__", content_block)
        .replace("__SRC__", htmllib.escape(rel_md.as_posix()))
        .replace("__SCRIPTS__", MD_SCRIPTS)
    )
    out_path.write_text(html_page, encoding="utf-8")

    # README.md at any level gets an index.html copy so directory URLs work.
    if rel_md.name == "README.md":
        alt = out_path.parent / "index.html"
        alt.write_text(html_page, encoding="utf-8")


def render_text_page(rel_txt: pathlib.Path) -> None:
    src = ROOT / rel_txt
    if not src.exists():
        return
    body = src.read_text(encoding="utf-8", errors="replace")
    out_rel = pathlib.Path(rel_txt.as_posix() + ".html")
    out_path = OUT / out_rel
    out_path.parent.mkdir(parents=True, exist_ok=True)
    root_prefix = root_prefix_for(out_rel)

    content_block = f'<pre class="plain-text">{htmllib.escape(body)}</pre>'

    html_page = (
        PAGE_TEMPLATE
        .replace("__TITLE__", htmllib.escape(rel_txt.name))
        .replace("__REPO__", htmllib.escape(REPO_NAME))
        .replace("__ROOT_PREFIX__", root_prefix)
        .replace("__BREADCRUMBS__", breadcrumb_html(rel_txt, root_prefix))
        .replace("__FILENAME__", htmllib.escape(rel_txt.as_posix()))
        .replace("__META__", f"{len(body):,} chars · plain text")
        .replace("__CONTENT__", content_block)
        .replace("__SRC__", htmllib.escape(rel_txt.as_posix()))
        .replace("__SCRIPTS__", "")
    )
    out_path.write_text(html_page, encoding="utf-8")


def render_landing_index(all_md: list[pathlib.Path]) -> None:
    # The root README is already written as index.html; but we also produce a
    # docs-landing sidebar page listing every rendered document.
    items = []
    buckets: dict[str, list[pathlib.Path]] = {}
    for rel in all_md:
        top = rel.parts[0] if len(rel.parts) > 1 else "(root)"
        buckets.setdefault(top, []).append(rel)
    items.append('<article class="markdown-body"><h1>All rendered documents</h1>')
    for top, files in sorted(buckets.items()):
        items.append(f"<h2>{htmllib.escape(top)}</h2><ul>")
        for rel in sorted(files):
            href = rel.with_suffix(".html").as_posix()
            items.append(f'<li><a href="{href}">{htmllib.escape(rel.as_posix())}</a></li>')
        items.append("</ul>")
    items.append("<h2>Plain text</h2><ul>")
    for rel in PLAIN_TEXT_FILES:
        if (ROOT / rel).exists():
            items.append(f'<li><a href="{rel.as_posix()}.html">{htmllib.escape(rel.as_posix())}</a></li>')
    items.append("</ul></article>")

    html_page = (
        PAGE_TEMPLATE
        .replace("__TITLE__", "Document index")
        .replace("__REPO__", htmllib.escape(REPO_NAME))
        .replace("__ROOT_PREFIX__", "")
        .replace("__BREADCRUMBS__", f'<a href="index.html">{REPO_NAME}</a> / <span>all-docs</span>')
        .replace("__FILENAME__", "all-docs.html")
        .replace("__META__", f"{len(all_md)} markdown files · index")
        .replace("__CONTENT__", "".join(items))
        .replace("__SRC__", "generated")
        .replace("__SCRIPTS__", "")
    )
    (OUT / "all-docs.html").write_text(html_page, encoding="utf-8")


def main() -> None:
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)

    md_rels = iter_markdown_files()
    md_set = {p.as_posix() for p in md_rels}

    for rel in md_rels:
        render_markdown_page(rel, md_set)

    for rel in PLAIN_TEXT_FILES:
        render_text_page(rel)

    render_landing_index(md_rels)

    print(f"Wrote preview to: {OUT}")
    print(f"  markdown pages: {len(md_rels)}")
    print(f"  plain pages:    {sum(1 for f in PLAIN_TEXT_FILES if (ROOT / f).exists())}")
    print(f"Open: file://{OUT / 'index.html'}")


if __name__ == "__main__":
    main()
