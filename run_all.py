#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
StoqRunner - In-process task runner (Option A)
----------------------------------------------
- Runs each step by importing it and calling a callable (prefer `main(args)`).
- Falls back to executing the step file with runpy if no callable is found.
- Designed to be bundled as a single PyInstaller EXE with all deps.
- No external Python / venv activation required on target PCs.

Build tip (PyInstaller):
    pyinstaller --onefile --name StoqRunner ^
      --collect-all pandas --collect-all numpy --collect-all pyarrow ^
      --collect-all httpx --collect-all supabase ^
      run_all.py
Add hiddenimports or a tasks/ package if your steps are inside a package.
"""

from __future__ import annotations

import argparse
import importlib
import io
import os
import runpy
import sys
import time
import traceback
from dataclasses import dataclass
from multiprocessing import Process, Queue, get_start_method, set_start_method, freeze_support
from pathlib import Path
from typing import Callable, List, Optional, Sequence, Tuple

# ===== Optional: load .env placed next to the EXE/script =====
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None

# =========================
# Configuration of Steps
# =========================
# Display label + filename (must be bundled with the EXE).
STEP_FILES: List[str] = [
    "JW_Step01_MakeCsvForStoqData_offers_variants.py",
    "JW_Step02_UpsertCsvToSupabase_daily_batch.py",
    "JW_Step03_ConfirmVariantIdsOnShopify.py",
    "JW_Step04_ProcessData_SetOffersTableStatus.py",
    "JW_Step05_AddNewSellingPlan_StoqApi.py",
    "JW_Step06_UpdateSellingPlan_StoqApi.py",
    "JW_Step07_DisableRemovedSellingPlan_StoqApi.py",
    "JW_Step08_DeleteSellingPlans.py",
    "JW_Step09_AddVariantsToNewSellingPlan.py",
    "JW_Step10_ReplaceVariantsTableWithNewData.py",
    "JW_Step11_RemoveVariantsFromExistingOffers.py",
    "JW_Step12_AddVariantsToExistingOffers.py",
    "JW_Step13_BulkUpdateDeliveryProfileId.py",
]

# If you also have wrapper modules (e.g., a tasks/ package), you can map them:
# The runner will try these module names **first**; if not found it falls back to file execution.
# Example:
#   MODULE_HINTS = {
#       "JW_Step01_MakeCsvForStoqData_offers_variants.py": ["tasks.jw_step01", "JW_Step01_MakeCsvForStoqData_offers_variants"],
#   }
MODULE_HINTS: dict[str, List[str]] = {}  # leave empty if you don't have package modules

DEFAULT_PY_ARGS: List[str] = []  # default args passed to every step (optional)


# =========================
# Utilities
# =========================
def app_base_dir() -> Path:
    """Directory where the EXE/script lives (not the temp _MEIPASS)."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).parent.resolve()


def resource_path(rel: str | Path) -> Path:
    """
    Preferred lookup order:
      1) Next to app (EXE/script directory)
      2) PyInstaller onefile temp dir (_MEIPASS)
      3) Current working dir (last resort)
    """
    relp = Path(rel)
    # 1) app dir
    cand = app_base_dir() / relp
    if cand.exists():
        return cand
    # 2) _MEIPASS (onefile extraction)
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        cand = Path(meipass) / relp
        if cand.exists():
            return cand
    # 3) cwd
    return Path.cwd() / relp


def load_env_if_present() -> None:
    if load_dotenv:
        # Prefer .env next to the EXE/script
        env_file = app_base_dir() / ".env"
        if env_file.exists():
            load_dotenv(str(env_file))
        else:
            load_dotenv()  # fallback to default search


def parse_selection(selection: str, total: int) -> List[int]:
    """
    Turn '1', '1,3,5-7', or 'all' into 0-based indices.
    """
    s = selection.strip().lower()
    if s in ("all", "a", "*"):
        return list(range(total))
    if s in ("none", "n", ""):
        return []
    picked: List[int] = []
    for chunk in s.split(","):
        chunk = chunk.strip()
        if "-" in chunk:
            start, end = chunk.split("-", 1)
            i, j = int(start) - 1, int(end) - 1
            if i < 0 or j >= total or i > j:
                raise ValueError(f"Invalid range: {chunk}")
            picked.extend(range(i, j + 1))
        else:
            k = int(chunk) - 1
            if k < 0 or k >= total:
                raise ValueError(f"Invalid index: {chunk}")
            picked.append(k)
    # de-dup while preserving order
    seen = set()
    out: List[int] = []
    for x in picked:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def show_menu() -> None:
    print("\nSelect a script to run (or 'q' to quit):")
    for i, name in enumerate(STEP_FILES, 1):
        print(f"  {i:>2}. {name}")


# =========================
# Step Execution (In-Process)
# =========================
@dataclass
class Step:
    label: str
    filename: str


def _candidate_module_names(filename: str) -> List[str]:
    """
    Build potential module names from filename and MODULE_HINTS.
    Tries hints first, then stem, then 'tasks.<stem>'.
    """
    stem = Path(filename).stem
    names: List[str] = []
    if filename in MODULE_HINTS:
        names.extend(MODULE_HINTS[filename])
    # Plain stem (if file sits next to run_all or within path)
    names.append(stem)
    # tasks.<stem> (common layout)
    names.append(f"tasks.{stem}")
    # de-dup
    seen = set()
    uniq: List[str] = []
    for n in names:
        if n not in seen:
            uniq.append(n)
            seen.add(n)
    return uniq


def _call_callable(callable_obj: Callable, args: List[str]) -> None:
    """
    Call a callable with best-effort signature support:
    - Try (args) first
    - Fallback to ()
    """
    try:
        return callable_obj(args)
    except TypeError:
        # likely callable() expects no args
        return callable_obj()


def run_step_inproc(step: Step, per_script_args: List[str]) -> int:
    """
    Try importing a module and calling its entrypoint:
      - Prefer `main(args)` or `main()`
      - Else `run(args)` or `run()`
    If import fails or no callable is found, execute the file via runpy.run_path in-process.
    Return code: 0 success, 1 failure.
    """
    print(f"\n=== Running: {step.label} ===")
    # 1) Try module imports (preferred for PyInstaller to see deps)
    for modname in _candidate_module_names(step.filename):
        try:
            mod = importlib.import_module(modname)
        except ModuleNotFoundError:
            continue
        except Exception as e:
            print(f"[!] Import error for {modname}: {e}")
            traceback.print_exc()
            continue

        entry = None
        if hasattr(mod, "main"):
            entry = getattr(mod, "main")
        elif hasattr(mod, "run"):
            entry = getattr(mod, "run")

        if callable(entry):
            try:
                _call_callable(entry, per_script_args)
                return 0
            except SystemExit as se:
                return int(getattr(se, "code", 0) or 0)
            except Exception as e:
                print(f"[X] Exception in {modname}: {e}")
                traceback.print_exc()
                return 1

    # 2) Fallback: execute the file by path with runpy (still in same interpreter)
    path = resource_path(step.filename)
    if not path.exists():
        print(f"[X] Step file not found: {path}")
        return 1

    # Prepare argv for the step
    saved_argv = sys.argv[:]
    try:
        sys.argv = [str(path)] + list(per_script_args)
        runpy.run_path(str(path), run_name="__main__")
        return 0
    except SystemExit as se:
        return int(getattr(se, "code", 0) or 0)
    except Exception as e:
        print(f"[X] Exception running {path.name}: {e}")
        traceback.print_exc()
        return 1
    finally:
        sys.argv = saved_argv


# =========================
# Optional: Process Isolation
# =========================
def _child_target(step: Step, args: List[str], retq: Queue) -> None:
    code = 1
    try:
        code = run_step_inproc(step, args)
    except Exception:
        traceback.print_exc()
        code = 1
    finally:
        try:
            retq.put(code)
        except Exception:
            pass


def run_step_isolated(step: Step, per_script_args: List[str]) -> int:
    """
    Run a step in a child process (isolation). Good when a step may crash
    or leak resources. Windows-safe (freeze_support is called in main).
    """
    q: Queue = Queue()
    p = Process(target=_child_target, args=(step, per_script_args, q))
    p.start()
    p.join()
    try:
        return q.get_nowait()
    except Exception:
        # If the process crashed without putting a code, use exitcode
        return 0 if p.exitcode == 0 else 1


# =========================
# Orchestration
# =========================
def main() -> None:
    # Windows: safer start method for multiprocessing
    try:
        if get_start_method(allow_none=True) != "spawn":
            set_start_method("spawn", force=True)
    except Exception:
        pass

    load_env_if_present()

    parser = argparse.ArgumentParser(
        description="Run Stoq steps in-process. Interactive mode runs one step and returns to menu."
    )
    parser.add_argument(
        "--select",
        help="Non-interactive selection, e.g. '1,3,5-7' or 'all' (runs in sequence, then exits).",
    )
    parser.add_argument(
        "--args",
        nargs=argparse.REMAINDER,
        help="Arguments to pass to each step (everything after --args goes to the steps).",
    )
    parser.add_argument(
        "--stop-on-failure",
        action="store_true",
        help="Stop on first failure.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would run, but don't execute.",
    )
    parser.add_argument(
        "--isolate",
        action="store_true",
        help="Run each step in a separate process (isolation).",
    )
    args = parser.parse_args()

    per_script_args: List[str] = list(DEFAULT_PY_ARGS)
    if args.args:
        per_script_args = list(args.args)

    steps = [Step(label=f, filename=f) for f in STEP_FILES]

    runner = run_step_isolated if args.isolate else run_step_inproc

    # Non-interactive batch mode
    if args.select:
        total = len(steps)
        picked = parse_selection(args.select, total)
        if not picked:
            print("Nothing selected. Exiting.")
            return
        print("\nPlan:")
        for idx in picked:
            print(f"  -> {steps[idx].label}   args={per_script_args}")
        if args.dry_run:
            print("\n[dry-run] Nothing executed.")
            return

        t0 = time.time()
        for idx in picked:
            step = steps[idx]
            started = time.time()
            code = runner(step, per_script_args)
            elapsed = time.time() - started
            if code == 0:
                print(f"[OK] {step.label} completed in {elapsed:.1f}s.")
            else:
                print(f"[X] {step.label} failed with code {code} (after {elapsed:.1f}s).")
                if args.stop_on_failure:
                    sys.exit(1)
        print(f"\nFinished in {time.time() - t0:.1f}s.")
        return

    # Interactive loop
    while True:
        show_menu()
        choice = input("\nYour selection (number, or q): ").strip().lower()
        if choice in ("q", "quit", "exit"):
            print("Bye.")
            return
        try:
            idxs = parse_selection(choice, len(steps))
            if not idxs:
                print("Nothing selected.")
                continue
            idx = idxs[0]  # one-at-a-time in interactive mode
        except Exception as e:
            print(f"Selection error: {e}. Try again.")
            continue

        step = steps[idx]
        started = time.time()
        code = runner(step, per_script_args)
        elapsed = time.time() - started

        if code == 0:
            print(f"\n[OK] {step.label} completed in {elapsed:.1f}s.")
            # return to menu
        else:
            print(f"\n[X] {step.label} failed with code {code} (after {elapsed:.1f}s).")
            if args.stop_on_failure:
                sys.exit(1)
            cont = input("Continue to menu? [Y/n]: ").strip().lower()
            if cont == "n":
                sys.exit(1)


if __name__ == "__main__":
    # Required on Windows when using multiprocessing with PyInstaller
    freeze_support()
    main()
