"""Tests for _dedupe_candidates — the merge half of the SNL S52 fix.

Anchored to 2026-10-09: first-source-wins meant whichever catalogue answered
FIRST decided the whole candidate list, and "answered" only ever meant "returned
rows", never "returned rows that play". Zilean answered S52E01 with 3 dead copies
(all three probe_failed, even after a fresh TorBox load) and that was enough to
skip Torrentio, which had 18 working cached copies of the same episode. The
resolver now collects from every enabled source and dedupes.

StremThru and Zilean both index DebridMediaManager, so they genuinely return the
same hashes — dedup is what stops the merge from burning walk-budget slots
re-probing one file. The one rule that matters: this is a MERGE, not a filter.
Nothing unidentifiable may be dropped.
"""
import ast
import os
import types


def _load_helper(name):
    """Pull one module-level function out of stream.py and exec it alone.

    stream.py imports fastapi/sqlalchemy at module scope, which the test venv
    deliberately does not carry (same reason test_candidate_ordering stubs its
    imports). Parsing the file and compiling only the target function keeps the
    test honest — it runs the SHIPPING source, not a copy that can drift.
    """
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(here, "api/stream.py")
    src = open(path).read()
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            mod = types.ModuleType("stream_helper")
            exec(compile(ast.Module(body=[node], type_ignores=[]), path, "exec"),
                 mod.__dict__)
            return getattr(mod, name)
    raise AssertionError(f"{name} not found in {path} — did it get renamed?")


dedupe = _load_helper("_dedupe_candidates")


def test_same_hash_from_two_catalogues_probed_once():
    """The real case: StremThru and Zilean share DMM hashes."""
    out = dedupe([
        {"infoHash": "AABB11", "title": "from stremthru"},
        {"infoHash": "aabb11", "title": "same file, from zilean"},
    ])
    assert len(out) == 1
    assert out[0]["title"] == "from stremthru", "first source wins the tie"


def test_hash_comparison_is_case_insensitive():
    assert len(dedupe([{"infoHash": "ABCDEF"}, {"infoHash": "abcdef"}])) == 1


def test_distinct_hashes_all_survive():
    cands = [{"infoHash": f"h{i}"} for i in range(18)]
    assert len(dedupe(cands)) == 18


def test_order_is_preserved():
    out = dedupe([{"infoHash": "a"}, {"infoHash": "b"}, {"infoHash": "c"}])
    assert [c["infoHash"] for c in out] == ["a", "b", "c"]


def test_url_identity_when_there_is_no_hash():
    """Debrid-mode addon streams arrive with a url and no infoHash."""
    out = dedupe([
        {"url": "https://x/1"}, {"url": "https://x/1"}, {"url": "https://x/2"},
    ])
    assert len(out) == 2


def test_unidentifiable_candidates_are_never_dropped():
    """This is a merge, not a filter. No hash and no url == pass through."""
    out = dedupe([{"title": "mystery a"}, {"title": "mystery b"}])
    assert len(out) == 2


def test_empty_list():
    assert dedupe([]) == []


def test_the_snl_shape_zilean_dead_plus_torrentio_working():
    """Zilean's 3 dead copies and Torrentio's working ones now coexist, with
    the one genuinely shared hash collapsed."""
    zilean = [
        {"infoHash": "2eee3c21", "title": "SNL S52E01 x265 FLUX (dead)"},
        {"infoHash": "b40f27f4", "title": "SNL S52E01 x264 exe (dead)"},
        {"infoHash": "19a28856", "title": "SNL S52E01 x264 exe (dead)"},
    ]
    torrentio = [
        {"infoHash": "2eee3c21", "title": "same FLUX copy, via torrentio"},
        {"infoHash": "3b510aef", "title": "SNL S52E01 Jalen Brunson (plays)"},
        {"infoHash": "de40aa0b", "title": "SNL S52E01 Jalen Brunson GRACE"},
    ]
    out = dedupe(zilean + torrentio)
    assert len(out) == 5, "3 + 3 with one shared hash"
    assert any("plays" in c["title"] for c in out), (
        "the working copy must reach the walk — this is the whole bug"
    )
